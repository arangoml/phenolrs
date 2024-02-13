mod graphs;
mod retrieval;
mod arangodb;
mod load_request;

use std::collections::HashMap;
use numpy::{Ix2, ToPyArray, PyArray, IntoPyArray, ndarray};
use ndarray::array;
use ndarray::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;
use crate::graphs::Graph;

type PygCompatible<'a> = (&'a PyDict, &'a PyDict, &'a PyDict);

fn construct_col_to_features<'pl>(input: HashMap<String, HashMap<String, Array<f64, Ix2>>>,  py: Python<'pl>) -> PyResult<&'pl PyDict> {
    let dict = PyDict::new(py);
    (&input).into_iter()
        .for_each(|(col_name, feature_map)| {
            let col_dict = PyDict::new(py);
            feature_map.iter()
                .for_each(|(feat_name, arr)| {
                    col_dict.set_item(feat_name, arr.to_pyarray(py)).unwrap();
                });
            dict.set_item(col_name, col_dict).unwrap();
        });
    Ok(dict)
}

fn construct_coo_by_from_edge_to<'pl>(input: HashMap<(String, String, String), Array<usize, Ix2>>, py: Python<'pl>) -> PyResult<&'pl PyDict> {
    let dict = PyDict::new(py);
    (&input).into_iter()
        .for_each(|item| {
            dict.set_item(item.0, item.1.to_pyarray(py)).unwrap()
        });
    Ok(dict)
}

fn construct_cols_to_keys_to_inds<'pl>(input: HashMap<String, HashMap<String, usize>>, py: Python<'pl>) -> PyResult<&'pl PyDict> {
    let dict = PyDict::new(py);
    (&input).into_iter()
        .for_each(|item| {
            dict.set_item(item.0, item.1).unwrap()
        });
    Ok(dict)
}



/// Loads a graph (from the name and description, into a PyG friendly format
/// Requires numpy as a runtime dependency
#[pyfunction]
fn graph_to_pyg_format<'a>(py: Python<'a>) -> PyResult<PygCompatible<'a>> {
    let graph_arc = retrieval::get_arangodb_graph();
    let inner_rw = Arc::<std::sync::RwLock<Graph>>::try_unwrap(graph_arc).unwrap();
    let graph = inner_rw.into_inner().unwrap();
    let col_to_features = construct_col_to_features(graph.cols_to_features.iter().map(|(col_name, features)| {
        let mut col_map: HashMap<String, Array2<f64>> = features.iter().map(|(feature_name, nested_feature_vec)| {
            let num_verts: usize = graph.number_of_vertices() as usize;
            let dim =
                if nested_feature_vec.len() > 0 {
                    nested_feature_vec[0].len()
                } else {
                    0
                };
            let mut arr = Array2::<f64>::default((num_verts, dim));
            for (i, mut row) in arr.axis_iter_mut(Axis(0)).enumerate() {
                for (j, col) in row.iter_mut().enumerate() {
                    *col = nested_feature_vec[i][j];
                }
            }
            (feature_name.clone(), arr)
        }).collect();
        (col_name.clone(), col_map)
    }).collect(), py)?;
    let coo_by_from_edge_to = construct_coo_by_from_edge_to(graph.coo_by_from_edge_to.iter().map(|item| {
        let num_edges: usize = graph.number_of_edges() as usize;
        let dim: usize = 2;
        let mut arr = Array2::<usize>::default((dim, num_edges));
        for (i, mut row) in arr.axis_iter_mut(Axis(0)).enumerate() {
            for (j, col) in row.iter_mut().enumerate() {
                *col = item.1[i][j];
            }
        }
        (item.0.clone(), arr)
    }).collect(), py)?;
    let cols_to_keys_to_inds = construct_cols_to_keys_to_inds(graph.cols_to_keys_to_inds, py)?;
    println!("Finished retrieval!");
    let res = (col_to_features, coo_by_from_edge_to, cols_to_keys_to_inds);
    Ok(res)
}

/// A Python module implemented in Rust.
#[pymodule]
fn phenolrs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(graph_to_pyg_format, m)?)?;
    Ok(())
}
