use crate::arangodb;
use crate::input::load_request::DataLoadRequest;
use bytes::Bytes;
use lightning::client::auth::handle_auth;
use lightning::client::config::ClientConfig;
use lightning::{client, CollectionInfo, DataLoadConfiguration, DatabaseConfiguration};
use log::debug;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;
use tokio::task::JoinSet;

#[derive(Debug, Serialize, Deserialize)]
struct CursorOptions {
    stream: bool,
}

impl CursorOptions {
    pub fn new(stream: bool) -> Self {
        Self { stream }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateCursorBody {
    query: String,
    options: CursorOptions,

    #[serde(skip_serializing_if = "Option::is_none")]
    batch_size: Option<u32>,
    bind_vars: Option<HashMap<String, String>>,
}

impl CreateCursorBody {
    pub fn from_streaming_query_with_size(
        query: String,
        batch_size: Option<u32>,
        bind_vars: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            query,
            batch_size,
            options: CursorOptions::new(true),
            bind_vars,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CursorResponse {
    has_more: Option<bool>,
    id: Option<String>,
}

pub async fn get_all_data_aql(
    req: &DataLoadRequest,
    db_config: &DatabaseConfiguration,
    collections: &[CollectionInfo],
    result_channels: Vec<std::sync::mpsc::Sender<Bytes>>,
    is_edge: bool,
) -> Result<(), String> {
    let begin = SystemTime::now();

    let use_tls = db_config.endpoints[0].starts_with("https://");
    let client_config = ClientConfig::builder()
        .n_retries(5)
        .use_tls(use_tls)
        .tls_cert_opt(db_config.tls_cert.clone())
        .build();
    let client = client::build_client(&client_config)?;

    let make_url = |path: &str| -> String {
        db_config.endpoints[0].clone() + "/_db/" + &req.database + "/_api/cursor" + path
    };

    let mut cursor_ids = vec![];
    let mut error_occurred = false;
    let mut error = "".into();

    let mut task_set = JoinSet::new();
    let mut endpoints_round_robin: usize = 0;
    let mut consumers_round_robin: usize = 0;
    for col in collections.iter() {
        let query = build_aql_query(col, is_edge);
        let bind_vars = HashMap::from([("@col".to_string(), col.name.clone())]);
        let body = CreateCursorBody::from_streaming_query_with_size(query, None, Some(bind_vars));
        let body_v = serde_json::to_vec::<CreateCursorBody>(&body)
            .expect("could not serialize DumpStartBody");
        let url = make_url("");
        let cursor_create_resp = handle_auth(client.post(url), db_config)
            .body(body_v)
            .send()
            .await;

        if let Err(create_error) = cursor_create_resp {
            error_occurred = true;
            error = create_error.to_string();
            break;
        }
        let response = cursor_create_resp.unwrap();
        let bytes_res = response
            .bytes()
            .await
            .map_err(|e| format!("Error in body: {:?}", e))?;
        let response_info = serde_json::from_slice::<CursorResponse>(&bytes_res.clone());

        if let Err(create_error) = response_info {
            eprintln!(
                "An error in parsing a cursor occurred, error: {}",
                create_error
            );
        } else {
            let cursor_resp = response_info.unwrap();
            let id = cursor_resp.id;

            result_channels[consumers_round_robin]
                .clone()
                .send(bytes_res)
                .expect("Could not send to channel");
            if !cursor_resp.has_more.unwrap_or(false) {
                continue;
            }

            if let Some(id) = id {
                cursor_ids.push(id.clone());

                let client_clone = client.clone();
                let endpoint_clone = db_config.endpoints[endpoints_round_robin].clone();
                if endpoints_round_robin >= db_config.endpoints.len() {
                    endpoints_round_robin = 0;
                }
                let database_clone = req.database.clone();
                let result_channel_clone = result_channels[consumers_round_robin].clone();

                let connection_config_clone = (*db_config).clone();

                task_set.spawn(async move {
                    loop {
                        let url = format!(
                            "{}/_db/{}/_api/cursor/{}",
                            endpoint_clone, database_clone, id,
                        );
                        let start = SystemTime::now();
                        debug!(
                            "{:?} Sending post request: {} ",
                            start.duration_since(begin).unwrap(),
                            id,
                        );
                        let resp = handle_auth(client_clone.post(url), &connection_config_clone)
                            .send()
                            .await;
                        let resp = lightning::request::handle_arangodb_response(resp, |c| {
                            c == StatusCode::OK
                        })
                        .await?;
                        let end = SystemTime::now();
                        let dur = end.duration_since(start).unwrap();
                        let bytes_res = resp
                            .bytes()
                            .await
                            .map_err(|e| format!("Error in body: {:?}", e))?;
                        let response_info =
                            serde_json::from_slice::<CursorResponse>(&bytes_res.clone())
                                .map_err(|e| format!("Error in body: {:?}", e))?;
                        result_channel_clone
                            .send(bytes_res)
                            .expect("Could not send to channel!");
                        if !response_info.has_more.unwrap_or(false) {
                            debug!(
                                "{:?} Cursor exhausted, got final response... {} {:?}",
                                end.duration_since(start).unwrap(),
                                id,
                                dur
                            );
                            return Ok::<(), String>(());
                        }
                    }
                });
            }
            consumers_round_robin += 1;
            if consumers_round_robin >= result_channels.len() {
                consumers_round_robin = 0;
            }
        }
    }

    let client_for_cursor_close = client.clone();
    let cleanup_cursors = |cursor_ids: Vec<String>| async move {
        for cursor_id in cursor_ids.into_iter() {
            let delete_cursor_url = make_url(&format!("/{}", cursor_id));
            let resp = handle_auth(client_for_cursor_close.delete(delete_cursor_url), db_config)
                .send()
                .await;
            let r = lightning::request::handle_arangodb_response(resp, |c| {
                c == StatusCode::ACCEPTED || c == StatusCode::NOT_FOUND
            })
            .await;
            if let Err(error) = r {
                eprintln!(
                    "An error in cancelling a cursor occurred, cursor: {}, error: {}",
                    cursor_id, error
                );
            }
        }
    };

    if error_occurred {
        cleanup_cursors(cursor_ids).await;
        return Err(error);
    }

    while let Some(res) = task_set.join_next().await {
        let r = match res {
            Ok(_) => Ok(()),
            Err(msg) => {
                println!("Got error result: {}", msg);
                Err(msg)
            }
        };
        match r {
            Ok(_x) => {
                debug!("Got OK result!");
            }
            Err(msg) => {
                debug!("Got error result: {}", msg);
            }
        }
    }

    cleanup_cursors(cursor_ids).await;
    debug!("Done with cleanup");
    Ok(())
}

fn build_aql_query(collection_description: &CollectionInfo, is_edge: bool) -> String {
    let field_strings = collection_description
        .fields
        .iter()
        .map(|s| format!("{}: doc.{},", s, s))
        .collect::<Vec<String>>()
        .join("\n");
    let identifiers = if is_edge {
        "_to: doc._to,\n_from: doc._from,\n"
    } else {
        "_key: doc._key,\n"
    };
    let query = format!(
        "
        FOR doc in @@col
            RETURN {{
                _id: doc._id,
                {}
                {}
            }}
    ",
        identifiers, field_strings
    );
    query
}
