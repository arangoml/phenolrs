use anyhow::{anyhow, Result};
use serde_json::{Map, Value};
use std::any::Any;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

#[derive(Eq, Hash, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexHash(u64);

#[derive(Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexIndex(u64);

#[derive(Debug)]
pub struct Graph {
    // Index in list of graphs:
    pub graph_id: u64,

    // // key is the hash of the vertex, value is the index, high bit
    // // indicates a collision
    // pub hash_to_index: HashMap<VertexHash, VertexIndex>,

    // // key is the key of the vertex, value is the exceptional hash
    // pub exceptions: HashMap<Vec<u8>, VertexHash>,

    // // Maps indices of vertices to their names, not necessarily used:
    // pub index_to_key: Vec<Vec<u8>>,

    // // JSON data for vertices. If all data was empty, it is allowed that
    // // the following vector is empty:
    // pub vertex_json: Vec<Value>,

    // // Additional data for edges. If all data was empty, it is allowed that
    // // both of these are empty! After sealing, the offsets get one more
    // // entry to mark the end of the last one:
    // pub edge_data: Vec<u8>,
    // pub edge_data_offsets: Vec<u64>,

    // // Maps indices of vertices to offsets in edges by from:
    // pub edge_index_by_from: Vec<u64>,

    // // Edge index by from:
    // pub edges_by_from: Vec<VertexIndex>,

    // // Maps indices of vertices to offsets in edge index by to:
    // pub edge_index_by_to: Vec<u64>,

    // // Edge index by to:
    // pub edges_by_to: Vec<VertexIndex>,

    // // store keys?
    // pub store_keys: bool,

    // // sealed?
    // pub vertices_sealed: bool,
    // pub edges_sealed: bool,

    // // Flag, if edges are already indexed:
    // pub edges_indexed_from: bool,
    // pub edges_indexed_to: bool,

    // pub cols_to_keys_to_inds: HashMap<String, HashMap<String, usize>>,
    // pub coo_by_from_edge_to: HashMap<(String, String, String), Vec<Vec<usize>>>,
    // pub cols_to_features: HashMap<String, HashMap<String, Vec<Vec<f64>>>>,
    pub load_node_dict: bool,
    pub load_adj_dict: bool,
    pub load_adj_dict_as_undirected: bool,
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

impl Graph {
    pub fn new(
        /*store_keys: bool, _bits_for_hash: u8,*/ id: u64,
        load_node_dict: bool,
        load_adj_dict: bool,
        load_adj_dict_as_undirected: bool,
        load_coo: bool,
    ) -> Arc<RwLock<Graph>> {
        Arc::new(RwLock::new(Graph {
            graph_id: id,
            load_node_dict: load_node_dict,
            load_adj_dict: load_adj_dict,
            load_adj_dict_as_undirected: load_adj_dict_as_undirected,
            load_coo: load_coo,
            // hash_to_index: HashMap::new(),
            // exceptions: HashMap::new(),
            // index_to_key: vec![],
            // vertex_json: vec![],
            // edge_data: vec![],
            // edge_data_offsets: vec![],
            // edges_by_from: vec![],
            // edge_index_by_from: vec![],
            // edges_by_to: vec![],
            // edge_index_by_to: vec![],
            // store_keys,
            // vertices_sealed: false,
            // edges_sealed: false,
            // edges_indexed_from: false,
            // edges_indexed_to: false,
            // cols_to_features: HashMap::new(),
            // cols_to_keys_to_inds: HashMap::new(),
            // coo_by_from_edge_to: HashMap::new(),
            node_map: HashMap::new(),
            adj_map: HashMap::new(),
            coo: (vec![], vec![]),
            vertex_id_to_index: HashMap::new(),
        }))
    }

    pub fn insert_vertex(
        &mut self,
        key: Vec<u8>, // cannot be empty
        json: Option<Value>,
        // collection_name: Vec<u8>,
        // field_names: &[String],
    ) -> Result<()> {
        if self.load_node_dict == false {
            return Err(anyhow!(
                "Cannot insert vertex into graph that does not have load_node_dict set to true"
            ));
        }

        // Simply insert the vertex into the node_map
        let vertex_id = String::from_utf8(key.clone()).unwrap();

        let properties = match json {
            Some(Value::Object(map)) => map,
            _ => Map::new(),
        };

        self.node_map.insert(vertex_id, properties);

        Ok(())
    }

    pub fn insert_edge(
        &mut self,
        // col_name: Vec<u8>,
        from_id: Vec<u8>,
        to_id: Vec<u8>,
        json: Option<Value>,
    ) -> Result<()> {
        if self.load_adj_dict == false && self.load_coo == false {
            return Err(anyhow!("Cannot insert edge into graph that does not have load_adj_dict or load_coo set to true"));
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

            if self.load_adj_dict_as_undirected {
                let to_map = self.adj_map.get_mut(&to_id_str).unwrap();
                to_map.insert(from_id_str.clone(), properties.clone());
            }
        }

        Ok(())
    }
}

// fn parse_value_to_vec(val: &Value) -> Option<Vec<f64>> {
//     // first try array
//     match val.as_array() {
//         Some(v) => {
//             let float_casted: Vec<f64> = v.iter().filter_map(|v| v.as_f64()).collect();
//             if float_casted.len() != v.len() {
//                 None
//             } else {
//                 Some(float_casted)
//             }
//         }
//         None => val.as_f64().map(|only_val| vec![only_val]),
//     }
// }
