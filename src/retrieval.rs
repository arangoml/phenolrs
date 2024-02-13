use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use bytes::Bytes;
use log::{debug, info};
use reqwest::StatusCode;
use serde_json::{json, Value};
use xxhash_rust::xxh3::xxh3_64_with_seed;
use crate::arangodb::{build_client, compute_shard_map, get_all_shard_data, handle_arangodb_response_with_parsed_body, ShardDistribution};
use crate::graphs::{Graph, VertexHash, VertexIndex};
use crate::load_request::{CollectionDescription, GraphAnalyticsEngineDataLoadRequest};


pub fn get_arangodb_graph() -> Arc<RwLock<Graph>> {
    let body = GraphAnalyticsEngineDataLoadRequest {
        database: "ABIDE".into(),
        vertex_collections: vec![
            CollectionDescription {
                name: "Subjects".into(),
                fields: vec!["label".into(), "brain_fmri_features".into()]
            }
        ],
        edge_collections: vec![
            CollectionDescription {
                name: "medical_affinity_graph".into(),
                fields: vec![]
            }
        ],
        batch_size: Some(400000),
        parallelism: Some(5)
    };

    let graph = Graph::new(true, 64, 0);
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
                fetch_graph_from_arangodb(body, graph_clone).await
            })
    });
    let _ = handle.join().expect("Couldn't finish computation");
    graph
}

pub async fn fetch_graph_from_arangodb(
    req: GraphAnalyticsEngineDataLoadRequest,
    graph_arc: Arc<RwLock<Graph>>,
) -> Result<Arc<RwLock<Graph>>, String> {
    // TODO: update this
    let endpoints: Vec<String> = vec!["http://localhost:8529".into()];
    let username: String = "root".into();
    let password: String = "test".into();

    if endpoints.is_empty() {
        return Err("no endpoints given".to_string());
    }
    let begin = std::time::SystemTime::now();

    info!(
        "{:?} Fetching graph from ArangoDB...",
        std::time::SystemTime::now().duration_since(begin).unwrap()
    );

    let use_tls = endpoints[0].starts_with("https://");
    let client = build_client(use_tls)?;

    let make_url = |path: &str| -> String { endpoints[0].clone() + "/_db/" + &req.database + path };

    // First ask for the shard distribution:
    let url = make_url("/_admin/cluster/shardDistribution");
    let resp = client
        .get(url)
        .basic_auth(&username, Some(&password))
        .send()
        .await;
    let shard_dist =
        handle_arangodb_response_with_parsed_body::<ShardDistribution>(resp, StatusCode::OK)
            .await?;

    // Compute which shard we must get from which dbserver, we do vertices
    // and edges right away to be able to error out early:
    let vertex_coll_list = req
        .vertex_collections
        .iter()
        .map(|ci| -> String { ci.name.clone() })
        .collect();
    let vertex_map = compute_shard_map(&shard_dist, &vertex_coll_list)?;
    let vertex_coll_field_map: Arc<RwLock<HashMap<String, Vec<String>>>> =
        Arc::new(RwLock::new(HashMap::new()));
    {
        let mut guard = vertex_coll_field_map.write().unwrap();
        for vc in req.vertex_collections.iter() {
            guard.insert(vc.name.clone(), vc.fields.clone());
        }
    }
    let edge_coll_list = req
        .edge_collections
        .iter()
        .map(|ci| -> String { ci.name.clone() })
        .collect();
    let edge_map = compute_shard_map(&shard_dist, &edge_coll_list)?;

    info!(
        "{:?} Need to fetch data from {} vertex shards and {} edge shards...",
        std::time::SystemTime::now().duration_since(begin).unwrap(),
        vertex_map.values().map(|v| v.len()).sum::<usize>(),
        edge_map.values().map(|v| v.len()).sum::<usize>()
    );

    // Let's first get the vertices:
    {
        // We use multiple threads to receive the data in batches:
        let mut senders: Vec<std::sync::mpsc::Sender<Bytes>> = vec![];
        let mut consumers: Vec<JoinHandle<Result<(), String>>> = vec![];
        let prog_reported = Arc::new(Mutex::new(0 as u64));
        for _i in 0..req.parallelism.expect("Why is parallelism missing") {
            let (sender, receiver) = std::sync::mpsc::channel::<Bytes>();
            senders.push(sender);
            let graph_clone = graph_arc.clone();
            let prog_reported_clone = prog_reported.clone();
            let vertex_coll_field_map_clone = vertex_coll_field_map.clone();
            let consumer = std::thread::spawn(move || -> Result<(), String> {
                let vcf_map = vertex_coll_field_map_clone.read().unwrap();
                let begin = std::time::SystemTime::now();
                while let Ok(resp) = receiver.recv() {
                    let body = std::str::from_utf8(resp.as_ref())
                        .map_err(|e| format!("UTF8 error when parsing body: {:?}", e))?;
                    debug!(
                        "{:?} Received post response, body size: {}",
                        std::time::SystemTime::now().duration_since(begin),
                        body.len()
                    );
                    let mut vertex_keys: Vec<Vec<u8>> = vec![];
                    let mut current_vertex_col: Option<Vec<u8>> = None;
                    vertex_keys.reserve(400000);
                    let mut vertex_json: Vec<Value> = vec![];
                    let mut json_initialized = false;
                    let mut fields: Vec<String> = vec![];
                    for line in body.lines() {
                        let v: Value = match serde_json::from_str(line) {
                            Err(err) => {
                                return Err(format!(
                                    "Error parsing document for line:\n{}\n{:?}",
                                    line, err
                                ));
                            }
                            Ok(val) => val,
                        };
                        let id = &v["_id"];
                        match id {
                            Value::String(i) => {
                                let mut buf = vec![];
                                buf.extend_from_slice((&i[..]).as_bytes());
                                vertex_keys.push(buf);
                                if current_vertex_col.is_none() {
                                    let pos = i.find("/").unwrap();
                                    current_vertex_col = Some((&i[0..pos]).into());
                                }
                                if !json_initialized {
                                    json_initialized = true;
                                    let pos = i.find("/");
                                    match pos {
                                        None => {
                                            fields = vec![];
                                        }
                                        Some(p) => {
                                            let collname = (&i[0..p]).to_string();
                                            let flds = vcf_map.get(&collname);
                                            match flds {
                                                None => {
                                                    fields = vec![];
                                                }
                                                Some(v) => {
                                                    fields = v.clone();
                                                    vertex_json.reserve(400000);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {
                                return Err(format!(
                                    "JSON is no object with a string _id attribute:\n{}",
                                    line
                                ));
                            }
                        }
                        // If we get here, we have to extract the field
                        // values in `fields` from the json and store it
                        // to vertex_json:
                        if !fields.is_empty() {
                            if fields.len() == 1 {
                                vertex_json.push(v[&fields[0]].clone());
                            } else {
                                let mut j = json!({});
                                for f in fields.iter() {
                                    j[&f] = v[&f].clone();
                                }
                                vertex_json.push(j);
                            }
                        }
                    }
                    let nr_vertices: u64;
                    {
                        let mut graph = graph_clone.write().unwrap();
                        let mut exceptional: Vec<(u32, VertexHash)> = vec![];
                        let mut exceptional_keys: Vec<Vec<u8>> = vec![];
                        for i in 0..vertex_keys.len() {
                            let k = &vertex_keys[i];
                            let hash = VertexHash::new(xxh3_64_with_seed(k, 0xdeadbeefdeadbeef));
                            graph.insert_vertex(
                                i as u32,
                                hash,
                                k.clone(),
                                vec![],
                                if vertex_json.is_empty() {
                                    None
                                } else {
                                    Some(vertex_json[i].clone())
                                },
                                &mut exceptional,
                                &mut exceptional_keys,
                                current_vertex_col.clone().unwrap()
                            );
                        }
                        nr_vertices = graph.number_of_vertices();
                    }
                    let mut prog = prog_reported_clone.lock().unwrap();
                    if nr_vertices > *prog + 1000000 as u64 {
                        *prog = nr_vertices;
                        info!(
                            "{:?} Have imported {} vertices.",
                            std::time::SystemTime::now().duration_since(begin).unwrap(),
                            *prog
                        );
                    }
                }
                Ok(())
            });
            consumers.push(consumer);
        }
        get_all_shard_data(&req, &endpoints, &username, &password, &vertex_map, senders).await?;
        info!(
            "{:?} Got all data, processing...",
            std::time::SystemTime::now().duration_since(begin).unwrap()
        );
        for c in consumers {
            let _guck = c.join();
        }
        let mut graph = graph_arc.write().unwrap();
        graph.seal_vertices();
    }

    // And now the edges:
    {
        let mut senders: Vec<std::sync::mpsc::Sender<Bytes>> = vec![];
        let mut consumers: Vec<JoinHandle<Result<(), String>>> = vec![];
        let prog_reported = Arc::new(Mutex::new(0 as u64));
        for _i in 0..req.parallelism.expect("Why is parallelism missing") {
            let (sender, receiver) = std::sync::mpsc::channel::<Bytes>();
            senders.push(sender);
            let graph_clone = graph_arc.clone();
            let prog_reported_clone = prog_reported.clone();
            let consumer = std::thread::spawn(move || -> Result<(), String> {
                while let Ok(resp) = receiver.recv() {
                    let body = std::str::from_utf8(resp.as_ref())
                        .map_err(|e| format!("UTF8 error when parsing body: {:?}", e))?;
                    let mut froms: Vec<Vec<u8>> = vec![];
                    froms.reserve(1000000);
                    let mut tos: Vec<Vec<u8>> = vec![];
                    tos.reserve(1000000);
                    let mut current_col_name: Option<Vec<u8>> = None;
                    for line in body.lines() {
                        let v: Value = match serde_json::from_str(line) {
                            Err(err) => {
                                return Err(format!(
                                    "Error parsing document for line:\n{}\n{:?}",
                                    line, err
                                ));
                            }
                            Ok(val) => val,
                        };
                        let from = &v["_from"];
                        match from {
                            Value::String(i) => {
                                let mut buf = vec![];
                                buf.extend_from_slice((&i[..]).as_bytes());
                                froms.push(buf);
                            }
                            _ => {
                                return Err(format!(
                                    "JSON is no object with a string _from attribute:\n{}",
                                    line
                                ));
                            }
                        }
                        let to = &v["_to"];
                        match to {
                            Value::String(i) => {
                                let mut buf = vec![];
                                buf.extend_from_slice((&i[..]).as_bytes());
                                tos.push(buf);
                            }
                            _ => {
                                return Err(format!(
                                    "JSON is no object with a string _from attribute:\n{}",
                                    line
                                ));
                            }
                        }
                        match current_col_name {
                            None => {
                                let id = &v["_id"];
                                match id {
                                    Value::String(i) => {
                                        let pos = i.find("/").unwrap();
                                        current_col_name = Some((&i[0..pos]).into());
                                    }
                                    _ => {
                                        return Err(format!("JSON _id is not string attribute"));
                                    }
                                }
                            }
                            _ => {}
                        };
                    }
                    let mut edges: Vec<(VertexIndex, VertexIndex, Vec<u8>, Vec<u8>, Vec<u8>)> = vec![];
                    edges.reserve(froms.len());
                    {
                        // First translate keys to indexes by reading
                        // the graph object:
                        let graph = graph_clone.read().unwrap();
                        assert!(froms.len() == tos.len());
                        for i in 0..froms.len() {
                            let from_key = &froms[i];
                            let from_opt = graph.index_from_vertex_key(from_key);
                            let to_key = &tos[i];
                            let to_opt = graph.index_from_vertex_key(to_key);
                            if from_opt.is_some() && to_opt.is_some() {
                                edges.push((from_opt.unwrap(), to_opt.unwrap(), current_col_name.clone().unwrap(), from_key.clone(), to_key.clone()));
                            } else {
                                eprintln!("Did not find _from or _to key in vertices!");
                            }
                        }
                    }
                    let nr_edges: u64;
                    {
                        // Now actually insert edges by writing the graph
                        // object:
                        let mut graph = graph_clone.write().unwrap();
                        for e in edges {
                            graph.insert_edge(e.0, e.1, e.2, e.3, e.4, vec![]);
                        }
                        nr_edges = graph.number_of_edges();
                    }
                    let mut prog = prog_reported_clone.lock().unwrap();
                    if nr_edges > *prog + 1000000 as u64 {
                        *prog = nr_edges;
                        info!(
                            "{:?} Have imported {} edges.",
                            std::time::SystemTime::now().duration_since(begin).unwrap(),
                            *prog
                        );
                    }
                }
                Ok(())
            });
            consumers.push(consumer);
        }
        get_all_shard_data(&req, &endpoints, &username, &password, &edge_map, senders).await?;
        info!(
            "{:?} Got all data, processing...",
            std::time::SystemTime::now().duration_since(begin).unwrap()
        );
        for c in consumers {
            let _guck = c.join();
        }

        let mut graph = graph_arc.write().unwrap();
        graph.seal_edges();
        info!(
            "{:?} Graph loaded.",
            std::time::SystemTime::now().duration_since(begin).unwrap()
        );
    }
    info!("hi");
    Ok(graph_arc)
}