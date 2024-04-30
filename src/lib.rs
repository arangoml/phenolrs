mod arangodb;
mod client;
mod graphs;
mod input;
mod load;
mod output;

use input::load_request::DataLoadRequest;
#[cfg(not(test))]
use output::construct;
#[cfg(not(test))]
use pyo3::create_exception;
#[cfg(not(test))]
use pyo3::exceptions::PyException;
#[cfg(not(test))]
use pyo3::prelude::*;
#[cfg(not(test))]
use pyo3::types::PyDict;
use pyo3::types::PyList;
use pyo3::types::PyTuple;


#[cfg(not(test))]
type CooCompatible<'a> = (&'a PyList, &'a PyList, &'a PyList);

#[cfg(not(test))]
create_exception!(phenolrs, PhenolError, PyException);

/// Loads a graph (from the name and description, into a COO friendly format
/// Requires numpy as a runtime dependency
#[cfg(not(test))]
#[pyfunction]
#[cfg(not(test))]
fn graph_to_coo_format(py: Python, request: DataLoadRequest) -> PyResult<(CooCompatible)> {
    let graph = load::retrieve::get_arangodb_graph(request).unwrap();

    let coo = graph.coo;
    let src_indices = PyList::new(py, &coo.0);
    let dst_indices = PyList::new(py, &coo.1);
    let vertex_ids = PyList::new(py, &graph.vertex_ids);

    let res = (src_indices, dst_indices, vertex_ids);

    Ok(res)
}

/// A Python module implemented in Rust.
#[cfg(not(test))]
#[pymodule]
#[cfg(not(test))]
fn phenolrs(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(graph_to_coo_format, m)?)?;
    m.add("PhenolError", py.get_type::<PhenolError>())?;
    Ok(())
}
