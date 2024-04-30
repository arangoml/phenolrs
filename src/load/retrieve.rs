use super::receive;
use crate::arangodb::{
    compute_shard_map, get_all_shard_data, handle_arangodb_response_with_parsed_body,
    ShardDistribution, ShardMap,
};
use crate::client::auth::handle_auth;
use crate::client::build_client;
use crate::client::config::ClientConfig;
use crate::graphs::Graph;
use crate::input::load_request::{DataLoadRequest, DatabaseConfiguration};
use bytes::Bytes;
use log::info;
use reqwest::StatusCode;
use std::error::Error;
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::SystemTime;

pub fn get_arangodb_graph(req: DataLoadRequest) -> Result<Graph, String> {
    let graph = Graph::new(0);
    let graph_clone = graph.clone(); // for background thread
    println!("Starting computation");
    // Fetch from ArangoDB in a background thread:
    let handle = std::thread::spawn(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                println!("Loading!");
                fetch_graph_from_arangodb(req, graph_clone).await
            })
    });
    handle.join().map_err(|_s| "Computation failed")??;
    let inner_rw_lock =
        Arc::<std::sync::RwLock<Graph>>::try_unwrap(graph).map_err(|poisoned_arc| {
            if poisoned_arc.is_poisoned() {
                "Computation failed: thread failed - poisoned arc"
            } else {
                "Computation failed"
            }
        })?;
    inner_rw_lock.into_inner().map_err(|poisoned_lock| {
        format!(
            "Computation failed: thread failed - poisoned lock {}",
            poisoned_lock
                .source()
                .map_or(String::from(""), <dyn Error>::to_string)
        )
    })
}

pub async fn fetch_graph_from_arangodb(
    req: DataLoadRequest,
    graph_arc: Arc<RwLock<Graph>>,
) -> Result<Arc<RwLock<Graph>>, String> {
    let db_config = &req.configuration.database_config;
    if db_config.endpoints.is_empty() {
        return Err("no endpoints given".to_string());
    }
    let begin = std::time::SystemTime::now();

    println!(
        "{:?} Fetching graph from ArangoDB...",
        std::time::SystemTime::now().duration_since(begin).unwrap()
    );

    let use_tls = db_config.endpoints[0].starts_with("https://");
    let client_config = ClientConfig::builder()
        .n_retries(5)
        .use_tls(use_tls)
        .tls_cert_opt(db_config.tls_cert.clone())
        .build();
    let client = build_client(&client_config)?;

    let make_url =
        |path: &str| -> String { db_config.endpoints[0].clone() + "/_db/" + &req.database + path };

    // First ask for the shard distribution:
    let url = make_url("/_admin/cluster/shardDistribution");
    let resp = handle_auth(client.get(url), db_config).send().await;
    let shard_dist =
        handle_arangodb_response_with_parsed_body::<ShardDistribution>(resp, StatusCode::OK)
            .await?;

    let edge_coll_list = req
        .edge_collections
        .iter()
        .map(|ci| -> String { ci.name.clone() })
        .collect::<Vec<String>>();
    let edge_map = compute_shard_map(&shard_dist, &edge_coll_list)?;

    info!(
        "{:?} Need to fetch data from {} edge shards...",
        std::time::SystemTime::now().duration_since(begin).unwrap(),
        edge_map.values().map(|v| v.len()).sum::<usize>()
    );

    load_edges(&req, &graph_arc, &db_config, begin, &edge_map).await?;

    // And now the edges:
    {
        info!(
            "{:?} Graph loaded.",
            std::time::SystemTime::now().duration_since(begin).unwrap()
        );
    }
    info!("hi");
    Ok(graph_arc)
}

async fn load_edges(
    req: &DataLoadRequest,
    graph_arc: &Arc<RwLock<Graph>>,
    db_config: &&DatabaseConfiguration,
    begin: SystemTime,
    edge_map: &ShardMap,
) -> Result<(), String> {
    let mut senders: Vec<std::sync::mpsc::Sender<Bytes>> = vec![];
    let mut consumers: Vec<JoinHandle<Result<(), String>>> = vec![];
    for _i in 0..req
        .configuration
        .parallelism
        .expect("Why is parallelism missing")
    {
        let (sender, receiver) = std::sync::mpsc::channel::<Bytes>();
        senders.push(sender);
        let graph_clone = graph_arc.clone();
        let consumer = std::thread::spawn(move || receive::receive_edges(receiver, graph_clone));
        consumers.push(consumer);
    }
    get_all_shard_data(req, db_config, edge_map, senders).await?;
    info!(
        "{:?} Got all data, processing...",
        std::time::SystemTime::now().duration_since(begin).unwrap()
    );
    for c in consumers {
        let _guck = c.join();
    }
    Ok(())
}