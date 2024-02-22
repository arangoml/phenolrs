mod graphs;
mod retrieval;
mod arangodb;
mod load_request;
mod convert;
mod output;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use crate::convert::{convert_coo_edge_map, convert_nested_features_map};

type PygCompatible<'a> = (&'a PyDict, &'a PyDict, &'a PyDict);



/// Loads a graph (from the name and description, into a PyG friendly format
/// Requires numpy as a runtime dependency
#[cfg(not(test))]
#[pyfunction]
fn graph_to_pyg_format<'a>(py: Python<'a>) -> PyResult<PygCompatible<'a>> {
    let graph = retrieval::get_arangodb_graph();
    let col_to_features = output::construct_col_to_features(convert_nested_features_map(graph.cols_to_features), py)?;
    let coo_by_from_edge_to = output::construct_coo_by_from_edge_to(convert_coo_edge_map(graph.coo_by_from_edge_to), py)?;
    let cols_to_keys_to_inds = output::construct_cols_to_keys_to_inds(graph.cols_to_keys_to_inds, py)?;
    println!("Finished retrieval!");
    let res = (col_to_features, coo_by_from_edge_to, cols_to_keys_to_inds);
    Ok(res)
}

/// A Python module implemented in Rust.
#[cfg(not(test))]
#[pymodule]
fn phenolrs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(graph_to_pyg_format, m)?)?;
    Ok(())
}
