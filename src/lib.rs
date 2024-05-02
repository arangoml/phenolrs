mod arangodb;
mod client;
mod graphs;
mod input;
mod load;
mod output;

use input::load_request::DataLoadRequest;
use numpy::PyArray1;
#[cfg(not(test))]
use output::construct::{construct_dict, construct_dict_of_dict, construct_dict_of_dict_of_dict};
#[cfg(not(test))]
use pyo3::create_exception;
#[cfg(not(test))]
use pyo3::exceptions::PyException;
#[cfg(not(test))]
use pyo3::prelude::*;
#[cfg(not(test))]
use pyo3::types::PyDict;

#[cfg(not(test))]
create_exception!(phenolrs, PhenolError, PyException);

/// Loads a graph (from the name and description, into a COO friendly format
/// Requires numpy as a runtime dependency
#[pyfunction]
#[cfg(not(test))]
fn graph_to_json_format(
    py: Python,
    request: DataLoadRequest,
) -> PyResult<(
    &PyDict,
    &PyDict,
    &PyArray1<usize>,
    &PyArray1<usize>,
    &PyDict,
)> {
    let graph = load::retrieve::get_arangodb_graph(request).unwrap();

    let node_dict = construct_dict_of_dict(graph.node_map, py)?;
    let adj_dict = construct_dict_of_dict_of_dict(graph.adj_map, py)?;
    let vertex_id_to_index = construct_dict(graph.vertex_id_to_index, py)?;

    let coo = graph.coo;
    let src_indices = PyArray1::from_vec(py, coo.0);
    let dst_indices = PyArray1::from_vec(py, coo.1);

    let res = (
        node_dict,
        adj_dict,
        src_indices,
        dst_indices,
        vertex_id_to_index,
    );

    Ok(res)
}

/// A Python module implemented in Rust.
#[cfg(not(test))]
#[pymodule]
#[cfg(not(test))]
fn phenolrs(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(graph_to_json_format, m)?)?;
    m.add("PhenolError", py.get_type::<PhenolError>())?;
    Ok(())
}
