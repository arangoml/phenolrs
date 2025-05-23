mod graph;
mod input;
mod load;
mod output;
use log::info;

use input::load_request::{DataLoadRequest, NetworkXGraphConfig};
use numpy::PyArray1;
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
use pyo3::IntoPy;
use pyo3::Py;
use pyo3::ToPyObject;

use graph::{NetworkXGraph, NumpyGraph};

#[cfg(not(test))]
type PygCompatible<'a> = (&'a PyDict, &'a PyDict, &'a PyDict, &'a PyDict);

#[cfg(not(test))]
create_exception!(phenolrs, PhenolError, PyException);

/// Loads a graph (from the name and description, into a PyG friendly format
/// Requires numpy as a runtime dependency
#[cfg(not(test))]
#[pyfunction]
fn graph_to_numpy_format(
    py: Python,
    request: DataLoadRequest,
) -> PyResult<(PyObject, PyObject, PyObject, PyObject)> {
    let _ = env_logger::try_init();

    let graph_factory = NumpyGraph::new;

    info!("Retrieving Numpy Graph...");
    let start_time = std::time::Instant::now();
    let graph =
        load::retrieve::get_arangodb_graph(request, graph_factory).map_err(PhenolError::new_err)?;
    info!("Retrieved. Took: {:?}", start_time.elapsed());

    info!("Building python objects...");
    let start_time = std::time::Instant::now();
    let col_to_features = construct::construct_col_to_features(
        convert_nested_features_map(graph.cols_to_features),
        py,
    )?;

    let coo_by_from_edge_to = construct::construct_coo_by_from_edge_to(
        convert_coo_edge_map(graph.coo_by_from_edge_to),
        py,
    )?;

    let cols_to_keys_to_inds =
        construct::construct_cols_to_keys_to_inds(graph.cols_to_keys_to_inds.clone(), py)?;

    let cols_to_inds_to_keys =
        construct::construct_cols_to_inds_to_keys(graph.cols_to_inds_to_keys, py)?;
    info!("Built. Took: {:?}", start_time.elapsed());

    let res = (
        col_to_features.into_py(py),
        coo_by_from_edge_to.into_py(py),
        cols_to_keys_to_inds.into_py(py),
        cols_to_inds_to_keys.into_py(py),
    );

    Ok(res)
}

#[pyfunction]
#[cfg(not(test))]
fn graph_to_networkx_format(
    py: Python,
    request: DataLoadRequest,
    graph_config: NetworkXGraphConfig,
) -> PyResult<(
    PyObject,
    PyObject,
    PyObject,
    PyObject,
    PyObject,
    PyObject,
    PyObject,
)> {
    let _ = env_logger::try_init();

    let load_all_vertex_attributes = request.load_config.load_all_vertex_attributes;
    let load_all_edge_attributes = request.load_config.load_all_edge_attributes;

    let graph_factory = || {
        NetworkXGraph::new(
            graph_config.load_adj_dict,
            graph_config.load_coo,
            load_all_vertex_attributes,
            load_all_edge_attributes,
            graph_config.is_directed,
            graph_config.is_multigraph,
            graph_config.symmetrize_edges_if_directed,
        )
    };

    info!("Retrieving NetworkX Graph...");
    let start_time = std::time::Instant::now();
    let graph =
        load::retrieve::get_arangodb_graph(request, graph_factory).map_err(PhenolError::new_err)?;
    info!("Retrieved. Took: {:?}", start_time.elapsed());

    info!("Building python objects...");
    let start_time = std::time::Instant::now();
    let node_dict = construct::construct_node_dict(graph.node_map, py)?;
    let adj_dict = if graph_config.is_multigraph {
        if graph_config.is_directed {
            construct::construct_multidigraph_adj_dict(graph.adj_map_multidigraph, py)?
        } else {
            construct::construct_multigraph_adj_dict(graph.adj_map_multigraph, py)?
        }
    } else {
        if graph_config.is_directed {
            construct::construct_digraph_adj_dict(graph.adj_map_digraph, py)?
        } else {
            construct::construct_graph_adj_dict(graph.adj_map_graph, py)?
        }
    };
    info!("Built. Took: {:?}", start_time.elapsed());

    let coo = graph.coo;
    let src_indices = PyArray1::from_vec(py, coo.0);
    let dst_indices = PyArray1::from_vec(py, coo.1);
    let edge_indices = PyArray1::from_vec(py, graph.edge_indices);
    let vertex_id_to_index = construct::construct_vertex_id_to_index(graph.vertex_id_to_index, py)?;
    let edge_values = construct::construct_edge_value_dict(graph.edge_values, py)?;

    let res = (
        node_dict.into_py(py),
        adj_dict.into_py(py),
        src_indices.into_py(py),
        dst_indices.into_py(py),
        edge_indices.into_py(py),
        vertex_id_to_index.into_py(py),
        edge_values.into_py(py),
    );

    Ok(res)
}

/// A Python module implemented in Rust.
#[cfg(not(test))]
#[pymodule]
fn phenolrs(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(graph_to_numpy_format, m)?)?;
    m.add_function(wrap_pyfunction!(graph_to_networkx_format, m)?)?;
    m.add("PhenolError", py.get_type::<PhenolError>())?;
    Ok(())
}
