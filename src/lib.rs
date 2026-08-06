mod graph;
mod input;
mod load;
mod output;
use log::info;

use input::load_request::{AqlDataLoadRequest, DataLoadRequest, NetworkXGraphConfig};
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
use pyo3::IntoPyObjectExt;
use pyo3::Py;

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
) -> PyResult<(Py<PyAny>, Py<PyAny>, Py<PyAny>, Py<PyAny>)> {
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
        col_to_features.into_py_any(py)?,
        coo_by_from_edge_to.into_py_any(py)?,
        cols_to_keys_to_inds.into_py_any(py)?,
        cols_to_inds_to_keys.into_py_any(py)?,
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
    Py<PyAny>,
    Py<PyAny>,
    Py<PyAny>,
    Py<PyAny>,
    Py<PyAny>,
    Py<PyAny>,
    Py<PyAny>,
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
        node_dict.into_py_any(py)?,
        adj_dict.into_py_any(py)?,
        src_indices.into_py_any(py)?,
        dst_indices.into_py_any(py)?,
        edge_indices.into_py_any(py)?,
        vertex_id_to_index.into_py_any(py)?,
        edge_values.into_py_any(py)?,
    );

    Ok(res)
}

/// Loads a graph using AQL queries into a PyG/numpy friendly format.
/// The request contains:
/// - database_config: Database connection settings
/// - batch_size: Number of items per batch (default 10000)
/// - vertex_attributes: Schema for vertex attributes {"name": "type"} or [{"name": "n", "type": "t"}]
/// - edge_attributes: Schema for edge attributes (same format)
/// - queries: List of lists of AQL queries [[{query, bindVars}, ...], ...]
///   - Outer list is processed sequentially
///   - Inner lists are processed in parallel
///   - Each query returns {"vertices":[...], "edges":[...]}
#[cfg(not(test))]
#[pyfunction]
fn graph_aql_to_numpy_format(
    py: Python,
    request: AqlDataLoadRequest,
) -> PyResult<(Py<PyAny>, Py<PyAny>, Py<PyAny>, Py<PyAny>)> {
    let _ = env_logger::try_init();

    let graph_factory = NumpyGraph::new;

    info!("Retrieving Numpy Graph via AQL...");
    let start_time = std::time::Instant::now();
    let graph = load::retrieve::get_arangodb_graph_via_aql(request, graph_factory)
        .map_err(PhenolError::new_err)?;
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
        col_to_features.into_py_any(py)?,
        coo_by_from_edge_to.into_py_any(py)?,
        cols_to_keys_to_inds.into_py_any(py)?,
        cols_to_inds_to_keys.into_py_any(py)?,
    );

    Ok(res)
}

/// Loads a graph using AQL queries into a NetworkX friendly format.
#[pyfunction]
#[cfg(not(test))]
fn graph_aql_to_networkx_format(
    py: Python,
    request: AqlDataLoadRequest,
    graph_config: NetworkXGraphConfig,
) -> PyResult<(
    Py<PyAny>,
    Py<PyAny>,
    Py<PyAny>,
    Py<PyAny>,
    Py<PyAny>,
    Py<PyAny>,
    Py<PyAny>,
)> {
    let _ = env_logger::try_init();

    // For AQL loading: if no attributes specified, load all; otherwise load selected
    // load_all_*_attributes = true means load ALL properties (no schema)
    // load_all_*_attributes = false means load SELECTED properties (user specified schema)
    let load_all_vertex_attrs = request.vertex_attributes.is_empty();
    let load_all_edge_attrs = request.edge_attributes.is_empty();

    let graph_factory = || {
        NetworkXGraph::new(
            graph_config.load_adj_dict,
            graph_config.load_coo,
            load_all_vertex_attrs,
            load_all_edge_attrs,
            graph_config.is_directed,
            graph_config.is_multigraph,
            graph_config.symmetrize_edges_if_directed,
        )
    };

    info!("Retrieving NetworkX Graph via AQL...");
    let start_time = std::time::Instant::now();
    let graph = load::retrieve::get_arangodb_graph_via_aql(request, graph_factory)
        .map_err(PhenolError::new_err)?;
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
        node_dict.into_py_any(py)?,
        adj_dict.into_py_any(py)?,
        src_indices.into_py_any(py)?,
        dst_indices.into_py_any(py)?,
        edge_indices.into_py_any(py)?,
        vertex_id_to_index.into_py_any(py)?,
        edge_values.into_py_any(py)?,
    );

    Ok(res)
}

/// A Python module implemented in Rust.
#[cfg(not(test))]
#[pymodule]
fn phenolrs(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(graph_to_numpy_format, m)?)?;
    m.add_function(wrap_pyfunction!(graph_to_networkx_format, m)?)?;
    m.add_function(wrap_pyfunction!(graph_aql_to_numpy_format, m)?)?;
    m.add_function(wrap_pyfunction!(graph_aql_to_networkx_format, m)?)?;
    m.add("PhenolError", py.get_type::<PhenolError>())?;
    Ok(())
}
