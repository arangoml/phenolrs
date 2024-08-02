use crate::graph::Graph;
use crate::input::load_request::DataLoadRequest;
use lightning::errors::GraphLoaderError;
use lightning::{CollectionInfo, GraphLoader};
use serde_json::Value;
use std::error::Error;
use std::sync::{Arc, RwLock};

pub fn get_arangodb_graph<G: Graph + Send + Sync + 'static>(
    req: DataLoadRequest,
    graph_factory: impl Fn() -> Arc<RwLock<G>>,
) -> Result<G, String> {
    let graph = graph_factory();
    let graph_clone = graph.clone(); // for background thread

    // Fetch from ArangoDB in a background thread:
    let handle = std::thread::spawn(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async { fetch_graph_from_arangodb_local_variant(req, graph_clone).await })
    });
    handle.join().map_err(|_s| "Computation failed")??;
    let inner_rw_lock = Arc::<std::sync::RwLock<G>>::try_unwrap(graph)
        .map_err(|_| "Computation failed: thread failed - poisoned arc".to_string())?;
    inner_rw_lock.into_inner().map_err(|poisoned_lock| {
        format!(
            "Computation failed: thread failed - poisoned lock {}",
            poisoned_lock
                .source()
                .map_or(String::from(""), <dyn Error>::to_string)
        )
    })
}

pub async fn fetch_graph_from_arangodb_local_variant<G: Graph + Send + Sync + 'static>(
    req: DataLoadRequest,
    graph_arc: Arc<RwLock<G>>,
) -> Result<Arc<RwLock<G>>, String> {
    let db_config = req.db_config;
    let load_config = req.load_config;

    let mut local_vertex_collections = vec![];
    let mut local_edge_collections = vec![];

    for col in &req.vertex_collections {
        let mut v_fields = vec![];
        if !load_config.load_all_vertex_attributes {
            v_fields.push("@collection_name".to_string());
        }
        v_fields.extend(col.fields.clone());
        let v_collection_info = CollectionInfo {
            name: col.name.clone(),
            fields: v_fields,
        };
        local_vertex_collections.push(v_collection_info);
    }
    for col in &req.edge_collections {
        let mut e_fields = vec![];
        if !load_config.load_all_edge_attributes {
            e_fields.push("@collection_name".to_string());
        }
        e_fields.extend(col.fields.clone());
        let e_collection_info = CollectionInfo {
            name: col.name.clone(),
            fields: e_fields,
        };
        local_edge_collections.push(e_collection_info);
    }

    if db_config.endpoints.is_empty() {
        return Err("no endpoints given".to_string());
    }

    let graph_loader_res = GraphLoader::new_custom(
        db_config,
        load_config,
        local_vertex_collections,
        local_edge_collections,
    )
    .await;

    let graph_loader = match graph_loader_res {
        Ok(g) => g,
        Err(e) => return Err(format!("Could not create graph loader: {:?}", e)),
    };

    let graph_arc_clone = graph_arc.clone();
    let handle_vertices = move |vertex_ids: &Vec<Vec<u8>>,
                                columns: &mut Vec<Vec<Value>>,
                                vertex_field_names: &Vec<String>| {
        let mut graph = graph_arc_clone.write().unwrap();

        for i in 0..vertex_ids.len() {
            let k = &vertex_ids[i];
            let mut cols: Vec<Value> = vec![];
            std::mem::swap(&mut cols, &mut columns[i]);
            graph.insert_vertex(k.clone(), cols, vertex_field_names);
        }

        Ok(())
    };

    if !req.vertex_collections.is_empty() {
        // only load vertices if there are any
        let vertices_result = graph_loader.do_vertices(handle_vertices).await;
        if vertices_result.is_err() {
            return Err(format!(
                "Could not load vertices: {:?}",
                vertices_result.err()
            ));
        }
    }

    let graph_arc_clone = graph_arc.clone();
    let handle_edges = move |from_ids: &Vec<Vec<u8>>,
                             to_ids: &Vec<Vec<u8>>,
                             columns: &mut Vec<Vec<Value>>,
                             edge_field_names: &Vec<String>| {
        {
            // Now actually insert edges by writing the graph
            // object:
            let mut graph = graph_arc_clone.write().unwrap();
            for i in 0..from_ids.len() {
                let insertion_result = graph.insert_edge(
                    from_ids[i].clone(),
                    to_ids[i].clone(),
                    columns[i].clone(),
                    edge_field_names,
                );
                if insertion_result.is_err() {
                    return Err(GraphLoaderError::from(format!(
                        "Could not insert edge: {:?}",
                        insertion_result.err()
                    )));
                }
            }
        }
        Ok(())
    };

    if !req.edge_collections.is_empty() {
        // only load edges if there are any
        let edges_result = graph_loader.do_edges(handle_edges).await;
        if edges_result.is_err() {
            return Err(format!("Could not load edges: {:?}", edges_result.err()));
        }
    }

    Ok(graph_arc)
}
