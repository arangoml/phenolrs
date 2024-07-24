use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Result};

#[derive(Eq, Hash, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexHash(u64);

#[derive(Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexIndex(u64);

pub trait Graph {
    fn insert_vertex(
        &mut self,
        id: Vec<u8>,               // cannot be empty
        columns: Vec<Value>, // columns is either with load_all_vertex_attributes set to True or False
        field_names: &Vec<String>, // should be empty if load_all_vertex_attributes is set to True
    );

    fn insert_edge(
        &mut self,
        from_id: Vec<u8>,
        to_id: Vec<u8>,
        columns: Vec<Value>, // columns is either with load_all_edge_attributes set to True or False (for now, False case is not supported)
        field_names: &Vec<String>, // should be empty if load_all_edge_attributes is set to True
    ) -> anyhow::Result<()>;
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
    pub load_adj_dict: bool,
    pub load_coo: bool,
    pub load_all_vertex_attributes: bool,
    pub load_all_edge_attributes: bool,
    pub is_directed: bool,
    pub is_multigraph: bool,

    // node_map is a dictionary of node IDs to their json data
    // e.g {'user/1': {'name': 'Alice', 'age': 25}, 'user/2': {'name': 'Bob', 'age': 30}, ...}
    pub node_map: HashMap<String, Map<String, Value>>,

    // adj_map is a dict of dict of dict that represents the adjacency list of the graph
    // e.g {'user/1': {'user/2': {'weight': 0.6}, 'user/3': {'weight': 0.2}}, 'user/2': {'user/1': {'weight': 0.6}, 'user/3': {'weight': 0.2}}, ...}
    // However, it is also possible to have multiple edges between the same pair of nodes.
    // e.g {'user/1': {'user/2': {0: {'weight': 0.6}, 1: {'weight': 0.8}}}}
    pub adj_map: HashMap<String, HashMap<String, Map<String, Value>>>,
    pub adj_map_multigraph: HashMap<String, HashMap<String, HashMap<usize, Map<String, Value>>>>,

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
        load_adj_dict: bool,
        load_coo: bool,
        load_all_vertex_attributes: bool,
        load_all_edge_attributes: bool,
        is_directed: bool,
        is_multigraph: bool,
    ) -> Arc<RwLock<NetworkXGraph>> {
        Arc::new(RwLock::new(NetworkXGraph {
            load_adj_dict,
            load_coo,
            load_all_vertex_attributes,
            load_all_edge_attributes,
            is_directed,
            is_multigraph,
            node_map: HashMap::new(),
            adj_map: HashMap::new(),
            adj_map_multigraph: HashMap::new(),
            coo: (vec![], vec![]),
            vertex_id_to_index: HashMap::new(),
        }))
    }
}

impl Graph for NumpyGraph {
    fn insert_vertex(
        &mut self,
        id: Vec<u8>, // cannot be empty
        columns: Vec<Value>,
        field_names: &Vec<String>,
    ) {
        debug_assert!(!columns.is_empty());
        debug_assert_eq!(columns.len(), field_names.len());

        let col_name_position = field_names
            .iter()
            .position(|x| x == "@collection_name")
            .expect("No @collection_name in field names");
        let col_name = match &columns[col_name_position] {
            Value::String(s) => s,
            _ => panic!("Expected Value::String for @collection_name"),
        };

        let mut feature_res: HashMap<String, Vec<f64>> = HashMap::new();
        for (i, feature_name) in field_names.iter().enumerate() {
            if feature_name == "_id" {
                continue;
            }
            let feature_vec = parse_value_to_vec(&columns[i]);
            if feature_vec.is_none() {
                println!("Feature {} is not a vector. Skipping.", feature_name);
                continue;
            }
            feature_res.insert(feature_name.clone(), feature_vec.unwrap());
        }

        if !feature_res.is_empty() {
            // insert the vertex
            if !self.cols_to_keys_to_inds.contains_key(col_name.as_str()) {
                self.cols_to_keys_to_inds
                    .insert(col_name.clone(), HashMap::new());
            }

            if !self.cols_to_inds_to_keys.contains_key(col_name.as_str()) {
                self.cols_to_inds_to_keys
                    .insert(col_name.clone(), HashMap::new());
            }

            let keys_to_inds: &mut HashMap<String, usize> = self
                .cols_to_keys_to_inds
                .get_mut(col_name.as_str())
                .unwrap();
            let inds_to_keys: &mut HashMap<usize, String> = self
                .cols_to_inds_to_keys
                .get_mut(col_name.as_str())
                .unwrap();

            let cur_ind = keys_to_inds.len();
            let cur_id_str = String::from_utf8(id.clone()).unwrap();
            // let cur_key_str = cur_id_str.splitn(2, '/').nth(1).unwrap().to_string();
            // This is a bit stupid right now. Before the library merge of lightning, this route here
            // always ad the id here in key.clone(). Now it is not the case anymore. So we need to
            // check if the key is already in the format of the id or not. This should be done better soon.
            // This only occurs in case we're using the AQL Load variant.
            let cur_key_str = cur_id_str.split_once('/').map_or_else(
                || cur_id_str.clone(),      // If no '/', use the whole string
                |(_, key)| key.to_string(), // If '/' is present, use the part after '/'
            );

            keys_to_inds.insert(cur_key_str.clone(), cur_ind);
            inds_to_keys.insert(cur_ind, cur_key_str);

            if !self.cols_to_features.contains_key(col_name.as_str()) {
                self.cols_to_features
                    .insert(col_name.clone(), HashMap::new());
            }
            let current_col_to_feats = self
                .cols_to_features
                .get_mut(col_name.as_str())
                .expect("Unable to get col");

            for (feature_name, feature_vec) in feature_res {
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
        from_id: Vec<u8>,
        to_id: Vec<u8>,
        columns: Vec<Value>,
        field_names: &Vec<String>,
    ) -> Result<()> {
        assert!(!columns.is_empty());
        assert_eq!(columns.len(), field_names.len());

        let mut col_name = String::new();
        for (i, feature_name) in field_names.iter().enumerate() {
            if feature_name == "@collection_name" {
                // Set the col_name to the collection name
                col_name = columns[i].as_str().unwrap().to_string();
                break;
            }
        }

        if col_name.is_empty() {
            return Err(anyhow!("col_name not set"));
        }

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

        debug_assert!(field_names.contains(&String::from("@collection_name")));
        let col_name_position = field_names
            .iter()
            .position(|x| x == "@collection_name")
            .expect("No @collection_name in edge field names");
        let col_name = match &columns[col_name_position] {
            Value::String(s) => s.as_str(),
            _ => panic!("Expected Value::String for @collection_name"),
        };

        let key_tup = (col_name.to_string(), from_col.clone(), to_col.clone());
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

impl Graph for NetworkXGraph {
    fn insert_vertex(
        &mut self,
        id: Vec<u8>, // cannot be empty
        columns: Vec<Value>,
        field_names: &Vec<String>,
    ) {
        let mut properties = Map::new();
        let vertex_id = String::from_utf8(id.clone()).unwrap();

        if self.load_all_vertex_attributes {
            assert_eq!(columns.len(), 1);
            assert_eq!(field_names.len(), 0); // TODO: Add support for field_names

            let json = columns.first();
            properties = match json {
                Some(Value::Object(map)) => map.clone(),
                _ => panic!("Vertex data must be a json object"),
            };

            properties.insert("_id".to_string(), Value::String(vertex_id.clone()));
        } else {
            for (i, field_name) in field_names.iter().enumerate() {
                if field_name == "@collection_name" || field_name == "_id" {
                    continue;
                }
                properties.insert(field_name.clone(), columns[i].clone());
            }
        }

        self.node_map.insert(vertex_id, properties.clone());
    }

    fn insert_edge(
        &mut self,
        from_id: Vec<u8>,
        to_id: Vec<u8>,
        columns: Vec<Value>,
        field_names: &Vec<String>,
    ) -> Result<()> {
        let from_id_str: String = String::from_utf8(from_id.clone()).unwrap();
        let to_id_str: String = String::from_utf8(to_id.clone()).unwrap();

        if self.load_coo {
            let from_id_index = match self.vertex_id_to_index.get(&from_id_str) {
                Some(index) => *index,
                None => {
                    let index: usize = self.vertex_id_to_index.len();
                    self.vertex_id_to_index.insert(from_id_str.clone(), index);
                    index
                }
            };

            let to_id_index = match self.vertex_id_to_index.get(&to_id_str) {
                Some(index) => *index,
                None => {
                    let index = self.vertex_id_to_index.len();
                    self.vertex_id_to_index.insert(to_id_str.clone(), index);
                    index
                }
            };

            self.coo.0.push(from_id_index);
            self.coo.1.push(to_id_index);
        }

        if self.load_adj_dict {
            let mut properties = Map::new();

            if self.load_all_edge_attributes {
                assert_eq!(columns.len(), 1);
                assert_eq!(field_names.len(), 0);

                let json = columns.first();
                properties = match json {
                    Some(Value::Object(map)) => map.clone(),
                    _ => panic!("Edge data must be a json object"),
                };

                properties.insert("_from".to_string(), Value::String(from_id_str.clone()));
                properties.insert("_to".to_string(), Value::String(to_id_str.clone()));
            } else {
                for (i, field_name) in field_names.iter().enumerate() {
                    if field_name == "@collection_name" {
                        continue;
                    }
                    properties.insert(field_name.clone(), columns[i].clone());
                }
            }

            // MultiDiGraph
            if self.is_multigraph {
                if !self.adj_map_multigraph.contains_key(&from_id_str) {
                    self.adj_map_multigraph
                        .insert(from_id_str.clone(), HashMap::new());
                }

                if !self.adj_map_multigraph.contains_key(&to_id_str) {
                    self.adj_map_multigraph
                        .insert(to_id_str.clone(), HashMap::new());
                }

                let from_map = self.adj_map_multigraph.get_mut(&from_id_str).unwrap();
                let from_to_map = from_map.entry(to_id_str.clone()).or_default();
                let index = from_to_map.len();
                from_to_map.insert(index, properties.clone());

                // MutliGraph
                if !self.is_directed {
                    let to_map = self.adj_map_multigraph.get_mut(&to_id_str).unwrap();
                    let to_from_map = to_map.entry(from_id_str.clone()).or_default();
                    to_from_map.insert(index, properties.clone());
                }

            // DiGraph
            } else {
                if !self.adj_map.contains_key(&from_id_str) {
                    self.adj_map.insert(from_id_str.clone(), HashMap::new());
                }

                if !self.adj_map.contains_key(&to_id_str) {
                    self.adj_map.insert(to_id_str.clone(), HashMap::new());
                }

                let from_map = self.adj_map.get_mut(&from_id_str).unwrap();
                from_map.insert(to_id_str.clone(), properties.clone());

                // Graph
                if !self.is_directed {
                    let to_map = self.adj_map.get_mut(&to_id_str).unwrap();
                    to_map.insert(from_id_str.clone(), properties.clone());
                }
            }
        }

        Ok(())
    }
}
