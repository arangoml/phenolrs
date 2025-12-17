use crate::graph::Graph;
use crate::input::load_request::{AqlDataLoadRequest, DataLoadRequest};
use arangors_graph_exporter::errors::GraphLoaderError;
use arangors_graph_exporter::{load_aql_graph, CollectionInfo, GraphLoader};
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

/// Load a graph using AQL queries following the design specification.
/// Queries are organized as a list of lists:
/// - Outer list is processed sequentially
/// - Inner lists are processed in parallel
/// Each query returns {"vertices":[...], "edges":[...]}
pub fn get_arangodb_graph_via_aql<G: Graph + Send + Sync + 'static>(
    req: AqlDataLoadRequest,
    graph_factory: impl Fn() -> Arc<RwLock<G>>,
) -> Result<G, String> {
    let graph = graph_factory();
    let graph_clone = graph.clone();

    // Fetch from ArangoDB in a background thread:
    let handle = std::thread::spawn(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async { fetch_graph_from_arangodb_via_aql(req, graph_clone).await })
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

pub async fn fetch_graph_from_arangodb_via_aql<G: Graph + Send + Sync + 'static>(
    req: AqlDataLoadRequest,
    graph_arc: Arc<RwLock<G>>,
) -> Result<Arc<RwLock<G>>, String> {
    // Create the AQL graph loader
    let aql_loader = load_aql_graph(
        req.db_config,
        req.batch_size,
        req.vertex_attributes.clone(),
        req.edge_attributes.clone(),
        req.queries,
    )
    .map_err(|e| format!("Could not create AQL graph loader: {:?}", e))?;

    // Clone attribute info for the callback
    // Add @collection_name as first field for NumpyGraph compatibility
    let mut vertex_attr_names: Vec<String> = vec!["@collection_name".to_string()];
    vertex_attr_names.extend(req.vertex_attributes.iter().map(|a| a.name.clone()));
    // Add @collection_name as first field for edges too
    let mut edge_attr_names: Vec<String> = vec!["@collection_name".to_string()];
    edge_attr_names.extend(req.edge_attributes.iter().map(|a| a.name.clone()));

    let graph_arc_clone = graph_arc.clone();
    let handle_batch = move |batch: &mut arangors_graph_exporter::GraphBatch| {
        let mut graph = graph_arc_clone.write().unwrap();

        // Insert vertices
        for i in 0..batch.vertex_ids.len() {
            let id = batch.vertex_ids[i].clone();
            // Extract collection name from id (format: "collection/key")
            let id_str = String::from_utf8_lossy(&id);
            let collection_name = id_str.split('/').next().unwrap_or("unknown").to_string();

            // Build columns with @collection_name as first element
            let mut columns: Vec<Value> = vec![Value::String(collection_name)];
            if !batch.vertex_attribute_values.is_empty() && i < batch.vertex_attribute_values.len()
            {
                columns.extend(batch.vertex_attribute_values[i].clone());
            }
            graph.insert_vertex(id, columns, &vertex_attr_names);
        }

        // Insert edges - use min length to avoid index out of bounds
        let edge_count = batch.edge_from_ids.len().min(batch.edge_to_ids.len());
        if batch.edge_from_ids.len() != batch.edge_to_ids.len() {
            log::warn!(
                "Edge array length mismatch: from={}, to={}. Processing {} edges.",
                batch.edge_from_ids.len(),
                batch.edge_to_ids.len(),
                edge_count
            );
        }
        for i in 0..edge_count {
            let from_id = batch.edge_from_ids[i].clone();
            let to_id = batch.edge_to_ids[i].clone();

            // Extract collection names from from/to ids for synthetic edge collection name
            let from_str = String::from_utf8_lossy(&from_id);
            let to_str = String::from_utf8_lossy(&to_id);
            let from_col = from_str.split('/').next().unwrap_or("unknown");
            let to_col = to_str.split('/').next().unwrap_or("unknown");
            let edge_collection = format!("{}_to_{}", from_col, to_col);

            // Build columns with @collection_name as first element
            let mut columns: Vec<Value> = vec![Value::String(edge_collection)];
            if !batch.edge_attribute_values.is_empty() && i < batch.edge_attribute_values.len() {
                columns.extend(batch.edge_attribute_values[i].clone());
            }
            let insertion_result = graph.insert_edge(from_id, to_id, columns, &edge_attr_names);
            if insertion_result.is_err() {
                return Err(GraphLoaderError::from(format!(
                    "Could not insert edge: {:?}",
                    insertion_result.err()
                )));
            }
        }

        Ok(())
    };

    // Execute the loader
    aql_loader
        .do_load(handle_batch)
        .await
        .map_err(|e| format!("Could not load graph via AQL: {:?}", e))?;

    Ok(graph_arc)
}
