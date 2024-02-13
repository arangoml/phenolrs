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

    // List of hashes by index:
    pub index_to_hash: Vec<VertexHash>,

    // key is the hash of the vertex, value is the index, high bit
    // indicates a collision
    pub hash_to_index: HashMap<VertexHash, VertexIndex>,

    // key is the key of the vertex, value is the exceptional hash
    pub exceptions: HashMap<Vec<u8>, VertexHash>,

    // Maps indices of vertices to their names, not necessarily used:
    pub index_to_key: Vec<Vec<u8>>,

    // Additional data for vertices. If all data was empty, it is allowed
    // that both of these are empty! After sealing, the offsets get one
    // more entry to mark the end of the last one:
    pub vertex_data: Vec<u8>,
    pub vertex_data_offsets: Vec<u64>,

    // JSON data for vertices. If all data was empty, it is allowed that
    // the following vector is empty:
    pub vertex_json: Vec<Value>,

    // Edges as from/to tuples:
    pub edges: Vec<Edge>,

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
            index_to_hash: vec![],
            hash_to_index: HashMap::new(),
            exceptions: HashMap::new(),
            index_to_key: vec![],
            vertex_data: vec![],
            vertex_data_offsets: vec![],
            vertex_json: vec![],
            edges: vec![],
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
        i: u32,
        hash: VertexHash,
        key: Vec<u8>,  // cannot be empty
        data: Vec<u8>, // can be empty
        json: Option<Value>,
        exceptional: &mut Vec<(u32, VertexHash)>,
        exceptional_keys: &mut Vec<Vec<u8>>,
        collection_name: Vec<u8>,
    ) {
        let col_name = String::from_utf8(collection_name).unwrap();
        if !self.cols_to_keys_to_inds.contains_key(&col_name) {
            self.cols_to_keys_to_inds.insert(col_name.clone(), HashMap::new());
        }
        let col_inds = self.cols_to_keys_to_inds.get_mut(&col_name).unwrap();
        let cur_ind = col_inds.len();
        col_inds.insert(String::from_utf8(key.clone()).unwrap(), cur_ind);
        // First detect a collision:
        let index = VertexIndex(self.index_to_hash.len() as u64);
        let mut actual = hash;
        if self.hash_to_index.contains_key(&hash) {
            // This is a collision, we create a random alternative
            // hash and report the collision:
            let mut rng = rand::thread_rng();
            loop {
                actual = VertexHash(rng.gen::<u64>());
                if let Some(_) = self.hash_to_index.get_mut(&actual) {
                    break;
                }
            }
            let oi = self.hash_to_index.get_mut(&hash).unwrap();
            *oi = VertexIndex((*oi).0 | 0x800000000000000);
            exceptional.push((i, actual));
            exceptional_keys.push(key.clone());
            if self.store_keys {
                self.exceptions.insert(key.clone(), actual);
            }
        }
        // Will succeed:
        self.index_to_hash.push(actual);
        self.hash_to_index.insert(actual, index);
        if self.store_keys {
            self.index_to_key.push(key.clone());
        }
        let pos = self.vertex_data.len() as u64;
        if data.is_empty() {
            // We only add things here lazily as soon as some non-empty
            // data has been detected to save memory:
            if !self.vertex_data_offsets.is_empty() {
                self.vertex_data_offsets.push(pos);
            }
        } else {
            // Now we have to pay for our laziness:
            if self.vertex_data_offsets.is_empty() {
                for _i in 0..index.to_u64() {
                    self.vertex_data_offsets.push(0);
                }
            }
            // Insert data:
            self.vertex_data_offsets.push(pos);
            self.vertex_data.extend_from_slice(&data);
        }
        match json {
            None => {
                // We only add things here lazily as soon as some non-empty
                // data has been detected to save memory:
                if !self.vertex_json.is_empty() {
                    self.vertex_json.push(json!(null));
                }
            }
            Some(j) => {
                // Now we have to pay for our laziness:
                // init the feature hashmap
                if !self.cols_to_features.contains_key(&col_name) {
                    self.cols_to_features.insert(col_name.clone(), HashMap::new());
                }
                let mut current_col_to_feats = self.cols_to_features.get_mut(&col_name).expect("Unable to get col");
                // now parse the data
                let data = j.as_object().expect("Unable to parse value as an object");
                data.iter().for_each(|(feature_name, val)| {
                    if !current_col_to_feats.contains_key(feature_name) {
                        current_col_to_feats.insert(feature_name.clone(), vec![]);
                    }
                    // first try array
                    match val.as_array() {
                        Some(v ) => {
                            current_col_to_feats.get_mut(feature_name).unwrap().append(
                                &mut vec![v.iter().map(|v| { v.as_f64().unwrap() }).collect()]
                            );
                        }
                        None => {
                            let new_v = val.as_f64().unwrap();
                            current_col_to_feats.get_mut(feature_name).unwrap().append(&mut vec![vec![new_v]]);
                        }
                    }
                    ()
                });
                // then insert it
            }
        }
    }

    pub fn number_of_vertices(&self) -> u64 {
        self.index_to_hash.len() as u64
    }

    pub fn number_of_edges(&self) -> u64 {
        self.edges.len() as u64
    }

    pub fn seal_vertices(&mut self) {
        if !self.vertex_data_offsets.is_empty() {
            self.vertex_data_offsets.push(self.vertex_data.len() as u64);
        }
        self.vertices_sealed = true;
        info!(
            "Vertices sealed in graph, number of vertices: {}",
            self.index_to_hash.len()
        );
    }

    pub fn index_edges(&mut self, by_from: bool, by_to: bool) {
        let mut tmp: Vec<EdgeTemp> = vec![];
        let number_v = self.number_of_vertices() as usize;
        let number_e = self.number_of_edges() as usize;
        tmp.reserve(number_e);
        for e in self.edges.iter() {
            tmp.push(EdgeTemp {
                from: e.from,
                to: e.to,
            });
        }

        if by_from {
            // Create lookup by from:
            tmp.sort_by(|a: &EdgeTemp, b: &EdgeTemp| -> Ordering {
                a.from.to_u64().cmp(&b.from.to_u64())
            });
            self.edge_index_by_from.clear();
            self.edge_index_by_from.reserve(number_v + 1);
            self.edges_by_from.clear();
            self.edges_by_from.reserve(number_e);
            let mut cur_from = VertexIndex::new(0);
            let mut pos: u64 = 0; // position in self.edges_by_from where
            // we currently write
            self.edge_index_by_from.push(0);
            // loop invariant: the start offset for cur_from has already been
            // written into edge_index_by_from.
            // loop invariant: pos == edges_by_from.len()
            for e in tmp.iter() {
                if e.from != cur_from {
                    loop {
                        cur_from = VertexIndex::new(cur_from.to_u64() + 1);
                        self.edge_index_by_from.push(pos);
                        if cur_from == e.from {
                            break;
                        }
                    }
                }
                self.edges_by_from.push(e.to);
                pos = pos + 1;
            }
            while cur_from.to_u64() < number_v as u64 {
                cur_from = VertexIndex::new(cur_from.to_u64() + 1);
                self.edge_index_by_from.push(pos);
            }
            self.edges_indexed_from = true;
        }

        if by_to {
            // Create lookup by to:
            tmp.sort_by(|a: &EdgeTemp, b: &EdgeTemp| -> Ordering {
                a.to.to_u64().cmp(&b.to.to_u64())
            });
            self.edge_index_by_to.clear();
            self.edge_index_by_to.reserve(number_v + 1);
            self.edges_by_to.clear();
            self.edges_by_to.reserve(number_e);
            let mut cur_to = VertexIndex::new(0);
            let mut pos = 0; // position in self.edges_by_to where we currently write
            self.edge_index_by_to.push(0);
            // loop invariant: the start offset for cur_to has already been
            // written into edge_index_by_to.
            // loop invariant: pos == edges_by_to.len()
            for e in tmp.iter() {
                if e.to != cur_to {
                    loop {
                        cur_to = VertexIndex::new(cur_to.to_u64() + 1);
                        self.edge_index_by_to.push(pos);
                        if cur_to == e.to {
                            break;
                        }
                    }
                }
                self.edges_by_to.push(e.from);
                pos = pos + 1;
            }
            while cur_to.to_u64() < number_v as u64 {
                cur_to = VertexIndex::new(cur_to.to_u64() + 1);
                self.edge_index_by_to.push(pos);
            }
            self.edges_indexed_to = true;
        }
    }

    pub fn seal_edges(&mut self) {
        self.edges_sealed = true;
        if !self.edge_data_offsets.is_empty() {
            self.edge_data_offsets.push(self.edge_data.len() as u64);
        }

        info!(
            "Sealed graph with {} vertices and {} edges.",
            self.index_to_hash.len(),
            self.edges.len()
        );
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

    pub fn insert_edge(&mut self, from: VertexIndex, to: VertexIndex, col_name: Vec<u8>, from_id: Vec<u8>, to_id: Vec<u8>, data: Vec<u8>) {
        self.edges.push(Edge { from, to });
        let offset = self.edge_data.len() as u64;
        if data.is_empty() {
            // We use edge_data_offsets lazily, only if there is some
            // non-empty data!
            if !self.edge_data_offsets.is_empty() {
                self.edge_data_offsets.push(offset);
            }
        } else {
            if self.edge_data_offsets.is_empty() {
                // We have to pay for our laziness now:
                for _i in 0..(self.edges.len() - 1) {
                    self.edge_data_offsets.push(0);
                }
            }
            self.edge_data_offsets.push(offset);
            self.edge_data.extend_from_slice(&data);
        }
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
        cur_coo[0].push(from_col_keys.get(&String::from_utf8(from_id).unwrap()).unwrap().clone());
        cur_coo[1].push(to_col_keys.get(&String::from_utf8(to_id).unwrap()).unwrap().clone());
    }
}

pub fn encode_id(id: u64) -> String {
    let mut v: Vec<u8> = vec![];
    v.write_u64::<BigEndian>(id).unwrap();
    general_purpose::URL_SAFE_NO_PAD.encode(&v)
}

pub fn decode_id(graph_id: &String) -> Result<u64, String> {
    let graph_id_decoded = general_purpose::URL_SAFE_NO_PAD.decode(graph_id.as_bytes());
    if let Err(e) = graph_id_decoded {
        return Err(format!("Cannot base64 decode graph_id {}: {}", graph_id, e,));
    }
    let graph_id_decoded = graph_id_decoded.unwrap();
    if graph_id_decoded.len() < 8 {
        return Err(format!("Too short base64 graph_id {}", graph_id));
    }
    let mut reader = Cursor::new(graph_id_decoded);
    Ok(reader.read_u64::<BigEndian>().unwrap())
}
