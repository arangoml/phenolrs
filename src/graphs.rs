use base64::{engine::general_purpose, Engine as _};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::info;
use rand::Rng;
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::Infallible;
use std::io::Cursor;
use std::sync::{Arc, Mutex, RwLock};
use xxhash_rust::xxh3::xxh3_64_with_seed;

#[derive(Eq, Hash, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexHash(u64);
impl VertexHash {
    pub fn new(x: u64) -> VertexHash {
        VertexHash(x)
    }
    pub fn to_u64(&self) -> u64 {
        self.0
    }
}

#[derive(Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexIndex(u64);
impl VertexIndex {
    pub fn new(x: u64) -> VertexIndex {
        VertexIndex(x)
    }
    pub fn to_u64(&self) -> u64 {
        self.0
    }
}

#[derive(Debug)]
pub struct Edge {
    pub from: VertexIndex, // index of vertex
    pub to: VertexIndex,   // index of vertex
}

#[derive(Debug)]
pub struct Graph {
    // Index in list of graphs:
    pub graph_id: u64,

    // key is the hash of the vertex, value is the index, high bit
    // indicates a collision
    pub hash_to_index: HashMap<VertexHash, VertexIndex>,

    // key is the key of the vertex, value is the exceptional hash
    pub exceptions: HashMap<Vec<u8>, VertexHash>,

    // Maps indices of vertices to their names, not necessarily used:
    pub index_to_key: Vec<Vec<u8>>,

    // JSON data for vertices. If all data was empty, it is allowed that
    // the following vector is empty:
    pub vertex_json: Vec<Value>,

    // Additional data for edges. If all data was empty, it is allowed that
    // both of these are empty! After sealing, the offsets get one more
    // entry to mark the end of the last one:
    pub edge_data: Vec<u8>,
    pub edge_data_offsets: Vec<u64>,

    // Maps indices of vertices to offsets in edges by from:
    pub edge_index_by_from: Vec<u64>,

    // Edge index by from:
    pub edges_by_from: Vec<VertexIndex>,

    // Maps indices of vertices to offsets in edge index by to:
    pub edge_index_by_to: Vec<u64>,

    // Edge index by to:
    pub edges_by_to: Vec<VertexIndex>,

    // store keys?
    pub store_keys: bool,

    // sealed?
    pub vertices_sealed: bool,
    pub edges_sealed: bool,

    // Flag, if edges are already indexed:
    pub edges_indexed_from: bool,
    pub edges_indexed_to: bool,

    pub cols_to_keys_to_inds: HashMap<String, HashMap<String, usize>>,
    pub coo_by_from_edge_to: HashMap<(String, String, String), Vec<Vec<usize>>>,
    pub cols_to_features: HashMap<String, HashMap<String, Vec<Vec<f64>>>>
}

struct EdgeTemp {
    pub from: VertexIndex,
    pub to: VertexIndex,
}

pub enum KeyOrHash {
    Key(Vec<u8>),
    Hash(VertexHash),
}

impl Graph {
    pub fn new(store_keys: bool, _bits_for_hash: u8, id: u64) -> Arc<RwLock<Graph>> {
        Arc::new(RwLock::new(Graph {
            graph_id: id,
            hash_to_index: HashMap::new(),
            exceptions: HashMap::new(),
            index_to_key: vec![],
            vertex_json: vec![],
            edge_data: vec![],
            edge_data_offsets: vec![],
            edges_by_from: vec![],
            edge_index_by_from: vec![],
            edges_by_to: vec![],
            edge_index_by_to: vec![],
            store_keys,
            vertices_sealed: false,
            edges_sealed: false,
            edges_indexed_from: false,
            edges_indexed_to: false,
            cols_to_features: HashMap::new(),
            cols_to_keys_to_inds: HashMap::new(),
            coo_by_from_edge_to: HashMap::new(),
        }))
    }

    pub fn insert_vertex(
        &mut self,
        key: Vec<u8>,  // cannot be empty
        json: Option<Value>,
        collection_name: Vec<u8>,
        field_names: &Vec<String>,
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
                // Now we have to pay for our laziness:
                // init the feature hashmap

                // now parse the data
                let data = j.as_object();
                match data {
                    Some(data) => {
                        let features_in_json = field_names.iter()
                            .filter(|name| { data.contains_key(*name)} ).collect::<Vec<&String>>();;
                        if features_in_json.len() != field_names.len() {
                            Err(())
                        } else {
                            let data_map = data.iter().filter_map(|(feature_name, val)| {
                                // now try to parse the value
                                match parse_value_to_array(val) {
                                    Some(v) => {
                                        Some((feature_name.clone(), v))
                                    }
                                    None => {
                                        None
                                    }
                                }
                            }).collect();
                            Ok(data_map)
                        }
                    },
                    None => {
                        if field_names.len() == 1 {
                            let feature_name = &field_names[0];
                            // if !current_col_to_feats.contains_key(feature_name) {
                            //     current_col_to_feats.insert(feature_name.clone(), vec![]);
                            // }
                            match parse_value_to_array(&j) {
                                Some(value_vec) => {
                                    Ok(HashMap::from([(feature_name.clone(), value_vec)]))
                                },
                                None => {
                                    Err(())
                                }
                            }
                            // match j.as_array() {
                            //     Some(v) => {
                            //         current_col_to_feats.get_mut(feature_name).unwrap().append(
                            //             &mut vec![v.iter().map(|v| { v.as_f64().unwrap() }).collect()]
                            //         );
                            //         Ok(())
                            //     }
                            //     None => {
                            //         match j.as_f64() {
                            //             Some(new_v) => {
                            //                 current_col_to_feats.get_mut(feature_name).unwrap().append(&mut vec![vec![new_v]]);
                            //                 Ok(())
                            //             },
                            //             None => {
                            //                 Err(())
                            //             }
                            //         }
                            //     }
                            // }
                        } else {
                            Err(())
                        }
                    }
                }
            }
        };

        match feature_res {
            Ok(feature_map) => {
                // insert the vertex
                if !self.cols_to_keys_to_inds.contains_key(&col_name) {
                    self.cols_to_keys_to_inds.insert(col_name.clone(), HashMap::new());
                }
                let col_inds = self.cols_to_keys_to_inds.get_mut(&col_name).unwrap();
                let cur_ind = col_inds.len();
                col_inds.insert(String::from_utf8(key.clone()).unwrap(), cur_ind);

                if !self.cols_to_features.contains_key(&col_name) {
                    self.cols_to_features.insert(col_name.clone(), HashMap::new());
                }
                let mut current_col_to_feats = self.cols_to_features.get_mut(&col_name).expect("Unable to get col");

                for (feature_name, feature_vec) in feature_map {
                    if !current_col_to_feats.contains_key(&feature_name) {
                        current_col_to_feats.insert(feature_name.clone(), vec![]);
                    }
                    current_col_to_feats.get_mut(&feature_name).unwrap().append(&mut vec![feature_vec]);
                }
            },
            Err(_) => {} // don't insert the edge
        }
    }

    pub fn hash_from_vertex_key(&self, k: &[u8]) -> Option<VertexHash> {
        let hash = VertexHash(xxh3_64_with_seed(k, 0xdeadbeefdeadbeef));
        let index = self.hash_to_index.get(&hash);
        match index {
            None => None,
            Some(index) => {
                if index.0 & 0x80000000_00000000 != 0 {
                    // collision!
                    let except = self.exceptions.get(k);
                    match except {
                        Some(h) => Some(*h),
                        None => Some(hash),
                    }
                } else {
                    Some(hash)
                }
            }
        }
    }

    pub fn index_from_vertex_key(&self, k: &[u8]) -> Option<VertexIndex> {
        let hash: Option<VertexHash> = self.hash_from_vertex_key(k);
        match hash {
            None => None,
            Some(vh) => {
                let index = self.hash_to_index.get(&vh);
                match index {
                    None => None,
                    Some(index) => Some(*index),
                }
            }
        }
    }

    pub fn index_from_hash(&self, h: &VertexHash) -> Option<VertexIndex> {
        let index = self.hash_to_index.get(h);
        match index {
            None => None,
            Some(i) => Some(*i),
        }
    }

    pub fn index_from_key_or_hash(&self, key_or_hash: &KeyOrHash) -> Option<VertexIndex> {
        match key_or_hash {
            KeyOrHash::Hash(h) => {
                // Lookup if hash exists, if so, this is the index
                self.index_from_hash(h)
            }
            KeyOrHash::Key(k) => {
                // Hash key, look up hash, check for exception:
                self.index_from_vertex_key(k)
            }
        }
    }

    pub fn insert_edge(&mut self, col_name: Vec<u8>, from_id: Vec<u8>, to_id: Vec<u8>, data: Vec<u8>) {
        // build up the coo representation
        let from_col: String = String::from_utf8({
            let s = String::from_utf8(from_id.clone()).expect("_from to be a string");
            let id_split = s.find("/").unwrap();
            (&s[0..id_split]).into()
        }).unwrap();
        let to_col: String = String::from_utf8({
            let s = String::from_utf8(to_id.clone()).expect("_to to be a string");
            let id_split = s.find("/").unwrap();
            (&s[0..id_split]).into()
        }).unwrap();
        let key_tup =(String::from_utf8(col_name).unwrap(), from_col.clone(), to_col.clone());
        if !self.coo_by_from_edge_to.contains_key(&key_tup) {
            self.coo_by_from_edge_to.insert(key_tup.clone(), vec![vec![], vec![]]);
        }
        let from_col_keys = self.cols_to_keys_to_inds.get(&from_col).unwrap();
        let to_col_keys = self.cols_to_keys_to_inds.get(&to_col).unwrap();
        let cur_coo = self.coo_by_from_edge_to.get_mut(&key_tup).unwrap();
        let from_col_id =from_col_keys.get(&String::from_utf8(from_id).unwrap());
        let to_col_id = to_col_keys.get(&String::from_utf8(to_id).unwrap());
        match (from_col_id, to_col_id) {
            (Some(from_id), Some(to_id)) => {
                cur_coo[0].push(from_id.clone());
                cur_coo[1].push(to_id.clone());
            },
            _ => {} // just skip the edge
        };
    }
}

fn parse_value_to_array(val: &Value) -> Option<Vec<f64>> {
    // first try array
    match val.as_array() {
        Some(v) => {
            let float_casted: Vec<f64> = v.iter().filter_map(|v| { v.as_f64() }).collect();
            if float_casted.len() != v.len() {
                None
            } else {
                Some(float_casted)
            }
        }
        None => {
            match val.as_f64() {
                Some(only_val) => { Some(vec![only_val]) },
                None => None
            }
        }
    }
}