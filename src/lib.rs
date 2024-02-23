mod arangodb;
mod client;
mod graphs;
mod input;
mod load;
mod output;

use input::load_request::DataLoadRequest;
use output::construct;
use output::convert::{convert_coo_edge_map, convert_nested_features_map};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyDict;

type PygCompatible<'a> = (&'a PyDict, &'a PyDict, &'a PyDict);

create_exception!(phenolrs, PhenolError, PyException);

/// Loads a graph (from the name and description, into a PyG friendly format
/// Requires numpy as a runtime dependency
#[cfg(not(test))]
#[pyfunction]
fn graph_to_pyg_format<'a>(
    py: Python<'a>,
    request: DataLoadRequest,
) -> PyResult<PygCompatible<'a>> {
    let graph = load::retrieve::get_arangodb_graph(request).map_err(|e| PhenolError::new_err(e))?;
    let col_to_features = construct::construct_col_to_features(
        convert_nested_features_map(graph.cols_to_features),
        py,
    )?;
    let coo_by_from_edge_to = construct::construct_coo_by_from_edge_to(
        convert_coo_edge_map(graph.coo_by_from_edge_to),
        py,
    )?;
    let cols_to_keys_to_inds =
        construct::construct_cols_to_keys_to_inds(graph.cols_to_keys_to_inds, py)?;
    println!("Finished retrieval!");
    let res = (col_to_features, coo_by_from_edge_to, cols_to_keys_to_inds);
    Ok(res)
}

/// A Python module implemented in Rust.
#[cfg(not(test))]
#[pymodule]
fn phenolrs(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(graph_to_pyg_format, m)?)?;
    m.add("PhenolError", py.get_type::<PhenolError>())?;
    Ok(())
}
