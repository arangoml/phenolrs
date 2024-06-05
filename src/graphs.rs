use serde_json::{Map, Value};
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Result};

#[derive(Eq, Hash, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexHash(u64);

#[derive(Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexIndex(u64);

#[derive(Debug)]
pub struct Edge {
    pub from: VertexIndex, // index of vertex
    pub to: VertexIndex,   // index of vertex
}

pub trait Graph {
    fn as_any(&self) -> &dyn Any;

    fn insert_vertex(
        &mut self,
        key: Vec<u8>,
        json: Option<Value>,
        collection_name: Vec<u8>,
        field_names: &[String],
    );
    fn insert_edge(
        &mut self,
        col_name: Vec<u8>,
        from_id: Vec<u8>,
        to_id: Vec<u8>,
        json: Option<Value>,
    ) -> anyhow::Result<()>;
}

fn identify_graph<G: Graph>(graph: G) -> String {
    if graph.as_any().is::<NumpyGraph>() {
        return "NumpyGraph".to_string();
    } else if graph.as_any().is::<NetworkXGraph>() {
        return "NetworkXGraph".to_string();
    } else {
        return "Unknown graph type".to_string();
    }
}

#[derive(Debug)]
pub struct NumpyGraph {
    pub cols_to_keys_to_inds: HashMap<String, HashMap<String, usize>>,
    pub cols_to_inds_to_keys: HashMap<String, HashMap<usize, String>>,
    pub coo_by_from_edge_to: HashMap<(String, String, String), Vec<Vec<usize>>>,
    pub cols_to_features: HashMap<String, HashMap<String, Vec<Vec<f64>>>>,
}

#[derive(Debug)]
pub struct NetworkXGraph {
    pub load_node_dict: bool,
    pub load_adj_dict: bool,
    pub load_adj_dict_as_directed: bool,
    pub load_coo: bool,

    // node_map is a dictionary of node IDs to their json data
    // e.g {'user/1': {'name': 'Alice', 'age': 25}, 'user/2': {'name': 'Bob', 'age': 30}, ...}
    pub node_map: HashMap<String, Map<String, Value>>,

    // adj_map is a dict of dict of dict that represents the adjacency list of the graph
    // e.g {'user/1': {'user/2': {'weight': 0.6}, 'user/3': {'weight': 0.2}}, 'user/2': {'user/1': {'weight': 0.6}, 'user/3': {'weight': 0.2}}, ...}
    pub adj_map: HashMap<String, HashMap<String, Map<String, Value>>>,

    // e.g ([0, 1, 2], [1, 2, 3])
    pub coo: (Vec<usize>, Vec<usize>),

    // e.g {'user/1': 0, 'user/2': 1, ...}
    pub vertex_id_to_index: HashMap<String, usize>,
}

impl NumpyGraph {
    pub fn new() -> Arc<RwLock<NumpyGraph>> {
        Arc::new(RwLock::new(NumpyGraph {
            cols_to_features: HashMap::new(),
            cols_to_keys_to_inds: HashMap::new(),
            cols_to_inds_to_keys: HashMap::new(),
            coo_by_from_edge_to: HashMap::new(),
        }))
    }
}

impl NetworkXGraph {
    pub fn new(
        load_node_dict: bool,
        load_adj_dict: bool,
        load_adj_dict_as_directed: bool,
        load_coo: bool,
    ) -> Arc<RwLock<NetworkXGraph>> {
        Arc::new(RwLock::new(NetworkXGraph {
            load_node_dict,
            load_adj_dict,
            load_adj_dict_as_directed,
            load_coo,
            node_map: HashMap::new(),
            adj_map: HashMap::new(),
            coo: (vec![], vec![]),
            vertex_id_to_index: HashMap::new(),
        }))
    }
}

impl Graph for NumpyGraph {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn insert_vertex(
        &mut self,
        key: Vec<u8>,
        json: Option<Value>,
        collection_name: Vec<u8>,
        field_names: &[String],
    ) {
        let col_name = String::from_utf8(collection_name).unwrap();

        let feature_res = match json {
            None => {
                // We only add things here lazily as soon as some non-empty
                // data has been detected to save memory:
                if field_names.is_empty() {
                    Ok(HashMap::new())
                } else {
                    Err(())
                }
            }
            Some(j) => {
                // now parse the data
                let data = j.as_object();
                match data {
                    Some(data) => {
                        let features_in_json = field_names
                            .iter()
                            .filter(|name| data.contains_key(*name))
                            .collect::<Vec<&String>>();
                        if features_in_json.len() != field_names.len() {
                            Err(())
                        } else {
                            let data_map = data
                                .iter()
                                .filter_map(|(feature_name, val)|
                                    // now try to parse the value
                                    parse_value_to_vec(val).map(|v| (feature_name.clone(), v)))
                                .collect();
                            Ok(data_map)
                        }
                    }
                    None => {
                        if field_names.len() == 1 {
                            let feature_name = &field_names[0];
                            match parse_value_to_vec(&j) {
                                Some(value_vec) => {
                                    Ok(HashMap::from([(feature_name.clone(), value_vec)]))
                                }
                                None => Err(()),
                            }
                        } else {
                            Err(())
                        }
                    }
                }
            }
        };

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
    }

    fn insert_edge(
        &mut self,
        col_name: Vec<u8>,
        from_id: Vec<u8>,
        to_id: Vec<u8>,
        _json: Option<Value>,
    ) -> Result<()> {
        let (from_col, from_key) = {
            let s = String::from_utf8(from_id.clone()).expect("_from to be a string");
            let id_split = s.find('/').expect("Invalid format for _from");
            let (col, key) = s.split_at(id_split);
            (col.to_string(), key[1..].to_string())
        };

        let (to_col, to_key) = {
            let s = String::from_utf8(to_id.clone()).expect("_to to be a string");
            let id_split = s.find('/').expect("Invalid format for _to");
            let (col, key) = s.split_at(id_split);
            (col.to_string(), key[1..].to_string())
        };

        let key_tup = (
            String::from_utf8(col_name).unwrap(),
            from_col.clone(),
            to_col.clone(),
        );
        if !self.coo_by_from_edge_to.contains_key(&key_tup) {
            self.coo_by_from_edge_to
                .insert(key_tup.clone(), vec![vec![], vec![]]);
        }
        let from_col_keys = self
            .cols_to_keys_to_inds
            .get(&from_col)
            .ok_or_else(|| anyhow!("Unable to get keys from for {:?}", &from_col))?;
        let to_col_keys = self
            .cols_to_keys_to_inds
            .get(&to_col)
            .ok_or_else(|| anyhow!("Unable to get keys to for {:?}", &to_col))?;
        let cur_coo = self
            .coo_by_from_edge_to
            .get_mut(&key_tup)
            .ok_or_else(|| anyhow!("Unable to get COO from to for {:?}", &key_tup))?;
        let from_col_id = from_col_keys.get(&from_key);
        let to_col_id = to_col_keys.get(&to_key);
        if let (Some(from_id), Some(to_id)) = (from_col_id, to_col_id) {
            cur_coo[0].push(*from_id);
            cur_coo[1].push(*to_id);
        };
        Ok(())
    }
}

impl Graph for NetworkXGraph {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn insert_vertex(
        &mut self,
        key: Vec<u8>,
        json: Option<Value>,
        _collection_name: Vec<u8>,
        _field_names: &[String],
    ) {
        if self.load_node_dict == false {
            return;
        }

        // Simply insert the vertex into the node_map
        let vertex_id = String::from_utf8(key.clone()).unwrap();

        let properties = match json {
            Some(Value::Object(map)) => map,
            _ => Map::new(),
        };

        self.node_map.insert(vertex_id, properties);
    }

    fn insert_edge(
        &mut self,
        _col_name: Vec<u8>,
        from_id: Vec<u8>,
        to_id: Vec<u8>,
        json: Option<Value>,
    ) -> Result<()> {
        if self.load_adj_dict == false && self.load_coo == false {
            return Ok(());
        }

        let from_id_str: String = String::from_utf8(from_id.clone()).unwrap();
        let to_id_str: String = String::from_utf8(to_id.clone()).unwrap();

        // Step 1:
        // Check if from_id_str exists in vertex_id_to_index
        //      If yes, get the from_id_index from vertex_id_to_index
        //      If not, add it to vertex_id_to_index with the current length of vertex_id_to_index.

        // Step 2:
        // Check if to_id_str exists in vertex_id_to_index
        //      If yes, get the to_id_index from vertex_id_to_index
        //      If not, add it to vertex_id_to_index with the current length of vertex_id_to_index.

        // Step 3:
        // Add the edge to the COO representation

        // Step 4:
        // Add the edge to the adjacency list representation

        if self.load_coo {
            // Step 1
            let from_id_index = match self.vertex_id_to_index.get(&from_id_str) {
                Some(index) => *index,
                None => {
                    let index: usize = self.vertex_id_to_index.len();
                    self.vertex_id_to_index.insert(from_id_str.clone(), index);
                    index
                }
            };

            // Step 2
            let to_id_index = match self.vertex_id_to_index.get(&to_id_str) {
                Some(index) => *index,
                None => {
                    let index = self.vertex_id_to_index.len();
                    self.vertex_id_to_index.insert(to_id_str.clone(), index);
                    index
                }
            };

            // Step 3
            self.coo.0.push(from_id_index);
            self.coo.1.push(to_id_index);
        }

        if self.load_adj_dict {
            // Step 4
            if !self.adj_map.contains_key(&from_id_str) {
                self.adj_map.insert(from_id_str.clone(), HashMap::new());
            }

            if !self.adj_map.contains_key(&to_id_str) {
                self.adj_map.insert(to_id_str.clone(), HashMap::new());
            }

            let properties = match json {
                Some(Value::Object(map)) => map,
                _ => Map::new(),
            };

            let from_map = self.adj_map.get_mut(&from_id_str).unwrap();
            from_map.insert(to_id_str.clone(), properties.clone());

            if !self.load_adj_dict_as_directed {
                let to_map = self.adj_map.get_mut(&to_id_str).unwrap();
                to_map.insert(from_id_str.clone(), properties.clone());
            }
        }

        Ok(())
    }
}

fn parse_value_to_vec(val: &Value) -> Option<Vec<f64>> {
    // first try array
    match val.as_array() {
        Some(v) => {
            let float_casted: Vec<f64> = v.iter().filter_map(|v| v.as_f64()).collect();
            if float_casted.len() != v.len() {
                None
            } else {
                Some(float_casted)
            }
        }
        None => val.as_f64().map(|only_val| vec![only_val]),
    }
}
