mod arangodb;
mod client;
mod graphs;
mod input;
mod load;
mod output;

use input::load_request::DataLoadRequest;
#[cfg(not(test))]
use output::construct;
use output::convert::{convert_coo_edge_map, convert_nested_features_map};
#[cfg(not(test))]
use pyo3::create_exception;
#[cfg(not(test))]
use pyo3::exceptions::PyException;
#[cfg(not(test))]
use pyo3::prelude::*;
#[cfg(not(test))]
use pyo3::types::PyDict;

#[cfg(not(test))]
type PygCompatible<'a> = (&'a PyDict, &'a PyDict, &'a PyDict);

#[cfg(not(test))]
create_exception!(phenolrs, PhenolError, PyException);

/// Loads a graph (from the name and description, into a PyG friendly format
/// Requires pytorch as a runtime dependency
#[cfg(not(test))]
#[pyfunction]
#[cfg(not(test))]
fn graph_to_pyg_format(py: Python, request: DataLoadRequest) -> PyResult<PygCompatible> {
    let graph = load::retrieve::get_arangodb_graph(request).map_err(PhenolError::new_err)?;
    py.import("torch")?;
    let col_to_features = construct::construct_col_to_features(
        convert_nested_features_map(graph.cols_to_features).map_err(|err| PhenolError::new_err(err.to_string()))?,
        py,
    )?;
    let coo_by_from_edge_to = construct::construct_coo_by_from_edge_to(
        convert_coo_edge_map(graph.coo_by_from_edge_to).map_err(|err| PhenolError::new_err(err.to_string()))?,
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
#[cfg(not(test))]
fn phenolrs(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(graph_to_pyg_format, m)?)?;
    m.add("PhenolError", py.get_type::<PhenolError>())?;
    Ok(())
}
