use std::collections::HashMap;
use lightning::graph_store::graph::Graph;
use lightning::graph_store::vertex_key_index::VertexIndex;
use serde_json::Value;

struct MLGraph {
    graph: Graph,
    cols_to_features: HashMap::new(),
    cols_to_keys_to_inds: HashMap::new(),
    cols_to_inds_to_keys: HashMap::new(),
    coo_by_from_edge_to: HashMap::new(),
}

impl MLGraph {
    pub fn new(graph: Graph) -> MLGraph {
        MLGraph {
            graph,
            cols_to_features: HashMap::new(),
            cols_to_keys_to_inds: HashMap::new(),
            cols_to_inds_to_keys: HashMap::new(),
            coo_by_from_edge_to: HashMap::new(),
        }
    }

    pub fn insert_vertex(
        &mut self,
        key: Vec<u8>, // cannot be empty
        mut columns: Vec<Value>,
    ) -> VertexIndex {
        let v_index = self.graph.insert_vertex(key, columns);

        if let Ok(feature_map) = feature_res {
            // insert the vertex
            if !self.cols_to_keys_to_inds.contains_key(&col_name) {
                self.cols_to_keys_to_inds
                    .insert(col_name.clone(), HashMap::new());
            }

            if !self.cols_to_inds_to_keys.contains_key(&col_name) {
                self.cols_to_inds_to_keys
                    .insert(col_name.clone(), HashMap::new());
            }

            let keys_to_inds: &mut HashMap<String, usize> =
                self.cols_to_keys_to_inds.get_mut(&col_name).unwrap();
            let inds_to_keys: &mut HashMap<usize, String> =
                self.cols_to_inds_to_keys.get_mut(&col_name).unwrap();

            let cur_ind = keys_to_inds.len();
            let cur_key_str = String::from_utf8(key.clone()).unwrap();

            keys_to_inds.insert(cur_key_str.clone(), cur_ind);
            inds_to_keys.insert(cur_ind, cur_key_str);

            if !self.cols_to_features.contains_key(&col_name) {
                self.cols_to_features
                    .insert(col_name.clone(), HashMap::new());
            }
            let current_col_to_feats = self
                .cols_to_features
                .get_mut(&col_name)
                .expect("Unable to get col");

            for (feature_name, feature_vec) in feature_map {
                if !current_col_to_feats.contains_key(&feature_name) {
                    current_col_to_feats.insert(feature_name.clone(), vec![]);
                }
                current_col_to_feats
                    .get_mut(&feature_name)
                    .unwrap()
                    .append(&mut vec![feature_vec]);
            }
        }

        v_index
    }
}