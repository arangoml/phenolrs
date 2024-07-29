use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Result};

#[derive(Eq, Hash, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexHash(u64);

#[derive(Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexIndex(u64);

fn panic_if_edge_exists<X>(map: &HashMap<String, X>, from_id_str: String, to_id_str: String) {
    if map.contains_key(&to_id_str) {
        panic!("ERROR: Edge '{}' to '{}' already exists in Adjacency Dictionary. Consider switching to Multi(Di)Graph instead.", from_id_str, to_id_str);
    }
}

fn parse_value_to_vec(val: &Value) -> Option<Vec<f64>> {
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
    pub symmterize_edges_if_directed: bool,

    // node_map is a dictionary of node IDs to their json data
    // e.g {'user/1': {'name': 'Alice', 'age': 25}, 'user/2': {'name': 'Bob', 'age': 30}, ...}
    pub node_map: HashMap<String, Map<String, Value>>,

    // adj_map represents the adjacency list of the graph
    // it can be a graph, digraph, multigraph, or multidigraph
    pub adj_map_graph: HashMap<String, HashMap<String, Map<String, Value>>>,
    pub adj_map_digraph: HashMap<String, HashMap<String, HashMap<String, Map<String, Value>>>>,
    pub adj_map_multigraph: HashMap<String, HashMap<String, HashMap<usize, Map<String, Value>>>>,
    pub adj_map_multidigraph:
        HashMap<String, HashMap<String, HashMap<String, HashMap<usize, Map<String, Value>>>>>,

    // e.g ([0, 1, 2], [1, 2, 3])
    pub coo: (Vec<usize>, Vec<usize>),

    // e.g {'user/1': 0, 'user/2': 1, ...}
    pub vertex_id_to_index: HashMap<String, usize>,

    // pre-defined functions
    get_vertex_properties_fn:
        fn(&mut NetworkXGraph, String, Vec<Value>, &Vec<String>) -> Map<String, Value>,

    get_edge_properties_fn:
        fn(&mut NetworkXGraph, String, String, Vec<Value>, &Vec<String>) -> Map<String, Value>,

    insert_coo_fn: fn(&mut NetworkXGraph, String, String),
    insert_adj_fn: fn(&mut NetworkXGraph, String, String, Map<String, Value>),
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
        symmterize_edges_if_directed: bool,
    ) -> Arc<RwLock<NetworkXGraph>> {
        let mut adj_map_digraph = HashMap::new();
        adj_map_digraph.insert("succ".to_string(), HashMap::new());
        adj_map_digraph.insert("pred".to_string(), HashMap::new());

        let mut adj_map_multidigraph = HashMap::new();
        adj_map_multidigraph.insert("succ".to_string(), HashMap::new());
        adj_map_multidigraph.insert("pred".to_string(), HashMap::new());

        let get_vertex_properties_fn = if load_all_vertex_attributes {
            NetworkXGraph::get_vertex_properties_all
        } else {
            NetworkXGraph::get_vertex_properties_selected
        };

        let get_edge_properties_fn = if load_adj_dict {
            if load_all_edge_attributes {
                NetworkXGraph::get_edge_properties_all
            } else {
                NetworkXGraph::get_edge_properties_selected
            }
        } else {
            NetworkXGraph::get_edge_properties_dummy
        };

        let insert_coo_fn = if load_coo {
            if is_directed {
                if symmterize_edges_if_directed {
                    NetworkXGraph::insert_coo_undirected
                } else {
                    NetworkXGraph::insert_coo_directed
                }
            } else {
                NetworkXGraph::insert_coo_undirected
            }
        } else {
            NetworkXGraph::insert_coo_dummy
        };

        let insert_adj_fn = if load_adj_dict {
            if is_multigraph {
                if is_directed {
                    NetworkXGraph::insert_adj_multidigraph
                } else {
                    NetworkXGraph::insert_adj_multigraph
                }
            } else {
                if is_directed {
                    NetworkXGraph::insert_adj_digraph
                } else {
                    NetworkXGraph::insert_adj_graph
                }
            }
        } else {
            NetworkXGraph::insert_adj_dummy
        };

        Arc::new(RwLock::new(NetworkXGraph {
            load_adj_dict,
            load_coo,
            load_all_vertex_attributes,
            load_all_edge_attributes,
            is_directed,
            is_multigraph,
            symmterize_edges_if_directed,
            node_map: HashMap::new(),
            adj_map_graph: HashMap::new(),
            adj_map_digraph: adj_map_digraph,
            adj_map_multigraph: HashMap::new(),
            adj_map_multidigraph: adj_map_multidigraph,
            coo: (vec![], vec![]),
            vertex_id_to_index: HashMap::new(),
            get_vertex_properties_fn,
            get_edge_properties_fn,
            insert_coo_fn,
            insert_adj_fn,
        }))
    }

    fn get_edge_properties_dummy(
        &mut self,
        _from_id: String,
        _to_id: String,
        _columns: Vec<Value>,
        _field_names: &Vec<String>,
    ) -> Map<String, Value> {
        Map::new()
    }

    fn insert_coo_dummy(&mut self, _from_id_str: String, _to_id_str: String) {}

    fn insert_adj_dummy(
        &mut self,
        _from_id_str: String,
        _to_id_str: String,
        _properties: Map<String, Value>,
    ) {
    }

    fn get_vertex_properties_all(
        &mut self,
        vertex_id: String,
        columns: Vec<Value>,
        field_names: &Vec<String>,
    ) -> Map<String, Value> {
        debug_assert_eq!(columns.len(), 1);
        debug_assert_eq!(field_names.len(), 0);

        let json = columns.first();
        let mut properties = match json {
            Some(Value::Object(map)) => map.clone(),
            _ => panic!("Vertex data must be a json object"),
        };

        properties.insert("_id".to_string(), Value::String(vertex_id.clone()));

        properties
    }

    fn get_vertex_properties_selected(
        &mut self,
        _vertex_id: String,
        columns: Vec<Value>,
        field_names: &Vec<String>,
    ) -> Map<String, Value> {
        let mut properties = Map::new();

        for (i, field_name) in field_names.iter().enumerate() {
            if field_name == "@collection_name" || field_name == "_id" {
                continue;
            }
            properties.insert(field_name.clone(), columns[i].clone());
        }

        properties
    }

    fn get_edge_properties_all(
        &mut self,
        from_id: String,
        to_id: String,
        columns: Vec<Value>,
        field_names: &Vec<String>,
    ) -> Map<String, Value> {
        debug_assert_eq!(columns.len(), 1);
        debug_assert_eq!(field_names.len(), 0);

        let json = columns.first();
        let mut properties = match json {
            Some(Value::Object(map)) => map.clone(),
            _ => panic!("Edge data must be a json object"),
        };

        properties.insert("_from".to_string(), Value::String(from_id.clone()));
        properties.insert("_to".to_string(), Value::String(to_id.clone()));

        properties
    }

    fn get_edge_properties_selected(
        &mut self,
        _from_id: String,
        _to_id: String,
        columns: Vec<Value>,
        field_names: &Vec<String>,
    ) -> Map<String, Value> {
        let mut properties = Map::new();

        for (i, field_name) in field_names.iter().enumerate() {
            if field_name == "@collection_name" {
                continue;
            }
            properties.insert(field_name.clone(), columns[i].clone());
        }

        properties
    }

    fn get_from_and_to_id_index(
        &mut self,
        from_id_str: String,
        to_id_str: String,
    ) -> (usize, usize) {
        let from_id_index = match self.vertex_id_to_index.get(&from_id_str) {
            Some(index) => *index,
            None => {
                let index = self.vertex_id_to_index.len();
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

        (from_id_index, to_id_index)
    }

    fn insert_coo_undirected(&mut self, from_id_str: String, to_id_str: String) {
        let (from_id_index, to_id_index) = self.get_from_and_to_id_index(from_id_str, to_id_str);

        self.coo.0.push(from_id_index);
        self.coo.1.push(to_id_index);

        self.coo.0.push(to_id_index);
        self.coo.1.push(from_id_index);
    }

    fn insert_coo_directed(&mut self, from_id_str: String, to_id_str: String) {
        let (from_id_index, to_id_index) = self.get_from_and_to_id_index(from_id_str, to_id_str);

        self.coo.0.push(from_id_index);
        self.coo.1.push(to_id_index);
    }

    fn insert_adj_graph(
        &mut self,
        from_id_str: String,
        to_id_str: String,
        properties: Map<String, Value>,
    ) {
        if !self.adj_map_graph.contains_key(&from_id_str) {
            self.adj_map_graph
                .insert(from_id_str.clone(), HashMap::new());
        }

        if !self.adj_map_graph.contains_key(&to_id_str) {
            self.adj_map_graph.insert(to_id_str.clone(), HashMap::new());
        }

        let from_map = self.adj_map_graph.get_mut(&from_id_str).unwrap();
        panic_if_edge_exists(from_map, from_id_str.clone(), to_id_str.clone());
        from_map.insert(to_id_str.clone(), properties.clone());

        let to_map = self.adj_map_graph.get_mut(&to_id_str).unwrap();
        panic_if_edge_exists(to_map, to_id_str.clone(), from_id_str.clone());
        to_map.insert(from_id_str.clone(), properties.clone());
    }

    fn insert_adj_digraph(
        &mut self,
        from_id_str: String,
        to_id_str: String,
        properties: Map<String, Value>,
    ) {
        // 1) Add [from, to] in _succ adjacency list
        let _succ = self.adj_map_digraph.get_mut("succ").unwrap();

        if !_succ.contains_key(&from_id_str) {
            _succ.insert(from_id_str.clone(), HashMap::new());
        }

        if !_succ.contains_key(&to_id_str) {
            _succ.insert(to_id_str.clone(), HashMap::new());
        }

        let succ_from_map = _succ.get_mut(&from_id_str).unwrap();
        panic_if_edge_exists(succ_from_map, from_id_str.clone(), to_id_str.clone());
        succ_from_map.insert(to_id_str.clone(), properties.clone());

        if self.symmterize_edges_if_directed {
            let succ_to_map = _succ.get_mut(&to_id_str).unwrap();
            panic_if_edge_exists(succ_to_map, to_id_str.clone(), from_id_str.clone());
            succ_to_map.insert(from_id_str.clone(), properties.clone());
        }

        // 2) Add [to, from] in _pred adjacency list
        let _pred = self.adj_map_digraph.get_mut("pred").unwrap();

        if !_pred.contains_key(&to_id_str) {
            _pred.insert(to_id_str.clone(), HashMap::new());
        }

        if !_pred.contains_key(&from_id_str) {
            _pred.insert(from_id_str.clone(), HashMap::new());
        }

        let pred_to_map = _pred.get_mut(&to_id_str).unwrap();
        panic_if_edge_exists(pred_to_map, to_id_str.clone(), from_id_str.clone());
        pred_to_map.insert(from_id_str.clone(), properties.clone());

        if self.symmterize_edges_if_directed {
            let pred_from_map = _pred.get_mut(&from_id_str).unwrap();
            panic_if_edge_exists(pred_from_map, from_id_str.clone(), to_id_str.clone());
            pred_from_map.insert(to_id_str.clone(), properties.clone());
        }
    }

    fn insert_adj_multigraph(
        &mut self,
        from_id_str: String,
        to_id_str: String,
        properties: Map<String, Value>,
    ) {
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

        let to_map = self.adj_map_multigraph.get_mut(&to_id_str).unwrap();
        let to_from_map = to_map.entry(from_id_str.clone()).or_default();
        to_from_map.insert(index, properties.clone());
    }

    fn insert_adj_multidigraph(
        &mut self,
        from_id_str: String,
        to_id_str: String,
        properties: Map<String, Value>,
    ) {
        // 1) Add [from, to] in _succ adjacency list
        let _succ = self.adj_map_multidigraph.get_mut("succ").unwrap();

        if !_succ.contains_key(&from_id_str) {
            _succ.insert(from_id_str.clone(), HashMap::new());
        }

        if !_succ.contains_key(&to_id_str) {
            _succ.insert(to_id_str.clone(), HashMap::new());
        }

        let succ_from_map = _succ.get_mut(&from_id_str).unwrap();
        let succ_from_to_map = succ_from_map.entry(to_id_str.clone()).or_default();
        let index = succ_from_to_map.len();
        succ_from_to_map.insert(index, properties.clone());

        if self.symmterize_edges_if_directed {
            let succ_to_map = _succ.get_mut(&to_id_str).unwrap();
            let succ_to_from_map = succ_to_map.entry(from_id_str.clone()).or_default();
            succ_to_from_map.insert(index, properties.clone());
        }

        // 2) Add [to, from] in _pred adjacency list
        let _pred = self.adj_map_multidigraph.get_mut("pred").unwrap();

        if !_pred.contains_key(&to_id_str) {
            _pred.insert(to_id_str.clone(), HashMap::new());
        }

        if !_pred.contains_key(&from_id_str) {
            _pred.insert(from_id_str.clone(), HashMap::new());
        }

        let pred_to_map = _pred.get_mut(&to_id_str).unwrap();
        let pred_to_from_map = pred_to_map.entry(from_id_str.clone()).or_default();
        let index = pred_to_from_map.len();
        pred_to_from_map.insert(index, properties.clone());

        if self.symmterize_edges_if_directed {
            let pred_from_map = _pred.get_mut(&from_id_str).unwrap();
            let pred_from_to_map = pred_from_map.entry(to_id_str.clone()).or_default();
            pred_from_to_map.insert(index, properties.clone());
        }
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
        debug_assert!(!columns.is_empty());
        debug_assert_eq!(columns.len(), field_names.len());

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

impl Graph for NetworkXGraph {
    fn insert_vertex(
        &mut self,
        id: Vec<u8>, // cannot be empty
        columns: Vec<Value>,
        field_names: &Vec<String>,
    ) {
        let vertex_id = String::from_utf8(id.clone()).unwrap();

        let properties =
            (self.get_vertex_properties_fn)(self, vertex_id.clone(), columns, field_names);

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

        (self.insert_coo_fn)(self, from_id_str.clone(), to_id_str.clone());

        let properties = (self.get_edge_properties_fn)(
            self,
            from_id_str.clone(),
            to_id_str.clone(),
            columns,
            field_names,
        );

        (self.insert_adj_fn)(self, from_id_str.clone(), to_id_str.clone(), properties);

        Ok(())
    }
}
