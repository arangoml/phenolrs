use serde_json::Value;
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

#[derive(Debug)]
pub struct Graph {
    // Index in list of graphs:
    pub graph_id: u64,

    // key is the hash of the vertex, value is the index, high bit
    // indicates a collision
    pub hash_to_index: HashMap<VertexHash, VertexIndex>,

    // key is the key of the vertex, value is the exceptional hash
    pub exceptions: HashMap<Vec<u8>, VertexHash>,

    // I need 3 new public variables:
    // 1. I need a tuple that contains 2 vectors of integers. This represents the COO format of the graph.
    // e.g ([0, 1, 2], [1, 2, 3])
    pub coo: (Vec<usize>, Vec<usize>),

    // 2. I need a map of ArangoDB Vertex IDs to indices (0 to N), where N is the number of vertices in the graph.
    // e.g {'user/1': 0, 'user/2': 1, ...}
    pub vertex_id_to_index: HashMap<String, usize>,

    // 3. I need a list of ArangoDB Vertex IDs (0 to N), where N is the number of vertices in the graph, and where
    // the index of the ID in the list is the index of the vertex in the graph!
    // e.g ['user/1', 'user/2', ...]
    pub vertex_ids: Vec<String>,
}

impl Graph {
    pub fn new(id: u64) -> Arc<RwLock<Graph>> {
        Arc::new(RwLock::new(Graph {
            graph_id: id,
            hash_to_index: HashMap::new(),
            exceptions: HashMap::new(),
            coo: (vec![], vec![]),
            vertex_id_to_index: HashMap::new(),
            vertex_ids: vec![],
        }))
    }

    pub fn insert_edge(
        &mut self,
        from_id: Vec<u8>,
        to_id: Vec<u8>,
    ) -> Result<()> {
        let from_id_str: String = String::from_utf8(from_id.clone()).unwrap();
        let to_id_str: String = String::from_utf8(to_id.clone()).unwrap();

        // Step 1:
        // Check if from_id_str exists in vertex_id_to_index
        //      If yes, get the from_id_index from vertex_id_to_index
        //      If not, add it to vertex_id_to_index with the current length of vertex_ids. Also add it to vertex_ids.

        // Step 2:
        // Check if to_id_str exists in vertex_id_to_index
        //      If yes, get the to_id_index from vertex_id_to_index
        //      If not, add it to vertex_id_to_index with the current length of vertex_ids. Also add it to vertex_ids.

        // Step 3:
        // Add the edge to the COO representation

        // Step 1
        let from_id_index = match self.vertex_id_to_index.get(&from_id_str) {
            Some(index) => *index,
            None => {
                let index = self.vertex_ids.len();
                self.vertex_id_to_index.insert(from_id_str.clone(), index);
                self.vertex_ids.push(from_id_str.clone());
                index
            }
        };

        // Step 2
        let to_id_index = match self.vertex_id_to_index.get(&to_id_str) {
            Some(index) => *index,
            None => {
                let index = self.vertex_ids.len();
                self.vertex_id_to_index.insert(to_id_str.clone(), index);
                self.vertex_ids.push(to_id_str.clone());
                index
            }
        };

        // Step 3
        self.coo.0.push(from_id_index);
        self.coo.1.push(to_id_index);

        Ok(())
    }
}

