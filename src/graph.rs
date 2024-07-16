use core::panic;
use serde_json::{Map, Value};
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Result};

#[derive(Eq, Hash, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexHash(u64);

#[derive(Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexIndex(u64);

pub trait Graph {
    fn as_any(&self) -> &dyn Any;

    fn insert_vertex(
        &mut self,
        key: Vec<u8>, // cannot be empty
        columns: Vec<Value>,
        field_names: &Vec<String>,
    );
    fn insert_edge(
        &mut self,
        col_name: Vec<u8>,
        from_id: Vec<u8>,
        to_id: Vec<u8>,
        _data: Vec<u8>,
    ) -> anyhow::Result<()>;
}

pub fn identify_graph<G: Graph>(graph_arc: &Arc<RwLock<G>>) -> &'static str {
    let graph = graph_arc.read().unwrap();
    if graph.as_any().is::<NumpyGraph>() {
        "NumpyGraph"
    } else if graph.as_any().is::<NetworkXGraph>() {
        "NetworkXGraph"
    } else {
        panic!("Unknown graph type"); // This should never happen
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
    pub load_adj_dict_as_multigraph: bool,
    pub load_coo: bool,

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
        load_node_dict: bool,
        load_adj_dict: bool,
        load_adj_dict_as_directed: bool,
        load_adj_dict_as_multigraph: bool,
        load_coo: bool,
    ) -> Arc<RwLock<NetworkXGraph>> {
        Arc::new(RwLock::new(NetworkXGraph {
            load_node_dict,
            load_adj_dict,
            load_adj_dict_as_directed,
            load_adj_dict_as_multigraph,
            load_coo,
            node_map: HashMap::new(),
            adj_map: HashMap::new(),
            adj_map_multigraph: HashMap::new(),
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
        key: Vec<u8>, // cannot be empty
        columns: Vec<Value>,
        field_names: &Vec<String>,
    ) {
        // Important note: Actually the API returns IDs instead of keys .................
        assert!(!columns.is_empty());
        assert_eq!(columns.len(), field_names.len());
        // columns[0] is type Value::String and must be the collection name
        let col_name = match &columns[0] {
            Value::String(s) => s,
            _ => panic!("Expected Value::String"),
        };

        let mut feature_res: HashMap<String, Vec<f64>> = HashMap::new();
        for (i, feature_name) in field_names.iter().enumerate() {
            if feature_name == "@collection_name" {
                continue;
            }
            let feature_vec = parse_value_to_vec(&columns[i]);
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
            let cur_id_str = String::from_utf8(key.clone()).unwrap();
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
        col_name: Vec<u8>,
        from_id: Vec<u8>,
        to_id: Vec<u8>,
        _data: Vec<u8>,
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    // Signature from MLP-674:
    // fn insert_vertex(
    //     &mut self,
    //     key: Vec<u8>,
    //     json: Option<Value>, // TODO: this needs to be passed...
    //     _columns: Vec<Value>,
    //     _field_names: &Vec<String>,
    // ) {...}

    fn insert_vertex(
        &mut self,
        key: Vec<u8>, // cannot be empty
        columns: Vec<Value>,
        field_names: &Vec<String>,
    ) {
        print!("inserting NetworkX vertex!");
    }

    // Signature from MLP-674:
    // fn insert_edge(
    //     &mut self,
    //     _col_name: Vec<u8>,
    //     from_id: Vec<u8>,
    //     to_id: Vec<u8>,
    //     // _data: Vec<u8>,
    //     json: Option<Value>, // TODO: this needs to be passed...
    // ) -> Result<()> {}

    fn insert_edge(
        &mut self,
        col_name: Vec<u8>,
        from_id: Vec<u8>,
        to_id: Vec<u8>,
        _data: Vec<u8>,
    ) -> Result<()> {
        print!("inserting NetworkX edge!");
        Ok(())
    }
}
