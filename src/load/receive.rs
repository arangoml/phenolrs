use crate::graphs::Graph;
use bytes::Bytes;
use serde_json::{Value};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, RwLock};

pub fn receive_edges(
    receiver: Receiver<Bytes>,
    graph_clone: Arc<RwLock<Graph>>,
) -> Result<(), String> {
    while let Ok(resp) = receiver.recv() {
        let body = std::str::from_utf8(resp.as_ref())
            .map_err(|e| format!("UTF8 error when parsing body: {:?}", e))?;
        let mut froms: Vec<Vec<u8>> = Vec::with_capacity(1000000);
        let mut tos: Vec<Vec<u8>> = Vec::with_capacity(1000000);
        let mut graph = graph_clone.write().unwrap();
        for line in body.lines() {
            let v: Value = match serde_json::from_str(line) {
                Err(err) => {
                    return Err(format!(
                        "Error parsing document for line:\n{}\n{:?}",
                        line, err
                    ));
                }
                Ok(val) => val,
            };
            let from = &v["_from"];
            match from {
                Value::String(i) => {
                    let mut buf = vec![];
                    buf.extend_from_slice(i[..].as_bytes());
                    froms.push(buf);
                }
                _ => {
                    return Err(format!(
                        "JSON is no object with a string _from attribute:\n{}",
                        line
                    ));
                }
            }
            let to = &v["_to"];
            match to {
                Value::String(i) => {
                    let mut buf = vec![];
                    buf.extend_from_slice(i[..].as_bytes());
                    tos.push(buf);
                }
                _ => {
                    return Err(format!(
                        "JSON is no object with a string _from attribute:\n{}",
                        line
                    ));
                }
            }

            let _ = graph.insert_edge(froms.pop().unwrap(), tos.pop().unwrap()).unwrap();

        }
        // let mut edges: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(froms.len());
        // First translate keys to indexes by reading
        // the graph object:
        assert!(froms.len() == tos.len());
        // let mut graph = graph_clone.write().unwrap();
        // for i in 0..froms.len() {
        //     let from_key = &froms[i];
        //     let to_key = &tos[i];
        //     edges.push((
        //         from_key.clone(),
        //         to_key.clone(),
        //     ));
        // }
        // {
        //     // Now actually insert edges by writing the graph
        //     // object:
        //     let mut graph = graph_clone.write().unwrap();
        //     for e in edges {
        //         // don't need to worry about this error for now
        //         let _ = graph.insert_edge( e.0, e.1);
        //     }
        // }
    }
    Ok(())
}
