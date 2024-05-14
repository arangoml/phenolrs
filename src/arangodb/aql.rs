use std::collections::HashMap;
use std::time::SystemTime;
use bytes::Bytes;
use log::debug;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use crate::arangodb::dump::ShardMap;
use crate::{arangodb, client};
use crate::client::auth::handle_auth;
use crate::client::config::ClientConfig;
use crate::input::load_request::{CollectionDescription, DatabaseConfiguration, DataLoadRequest};


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
    bind_vars: Option<HashMap<String, String>>
}

impl CreateCursorBody {
    pub fn from_streaming_query_with_size(query: String, batch_size: Option<u32>, bind_vars: Option<HashMap<String, String>>) -> Self {
        Self {
            query,
            batch_size,
            options: CursorOptions::new(true),
            bind_vars,
        }
    }
}

pub async fn get_all_data_aql(
    req: &DataLoadRequest,
    connection_config: &DatabaseConfiguration,
    shard_map: &ShardMap,
    result_channels: Vec<std::sync::mpsc::Sender<Bytes>>,
) -> Result<(), String> {
    let begin = SystemTime::now();

    let use_tls = connection_config.endpoints[0].starts_with("https://");
    let client_config = ClientConfig::builder()
        .n_retries(5)
        .use_tls(use_tls)
        .tls_cert_opt(connection_config.tls_cert.clone())
        .build();
    let client = client::build_client(&client_config)?;

    let make_url = |path: &str| -> String {
        connection_config.endpoints[0].clone() + "/_db/" + &req.database + "/_api/cursor"
    };

    for col in req.vertex_collections.iter() {
        let query = build_aql_query(col);
        let bind_vars = HashMap::from([("@col".to_string(), col.name.clone())]);
        let body = CreateCursorBody::from_streaming_query_with_size(
            query, None, Some(bind_vars)
        );
        let body_v =
            serde_json::to_vec::<CreateCursorBody>(&body).expect("could not serialize DumpStartBody");
        let url = make_url("");
        let cursor_create_resp = handle_auth(client.post(url), connection_config)
            .body(body_v)
            .send()
            .await;
        todo!()
    }
    todo!()
}

fn build_aql_query(collection_description: &CollectionDescription) -> String {
    let field_strings = collection_description.fields.iter().map(|s| format!("{}: doc.{},", s, s)).collect::<Vec<&str>>().join("\n");
    let query = format!("
        FOR doc in @@col
            RETURN {{
                _id: doc._id,
                {}
            }}
    ", field_strings);
    query
}