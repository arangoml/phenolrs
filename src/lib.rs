mod graph;
mod input;
mod load;
mod output;

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

use graph::{NetworkXGraph, NumpyGraph};

#[cfg(not(test))]
type PygCompatible<'a> = (&'a PyDict, &'a PyDict, &'a PyDict, &'a PyDict);

#[cfg(not(test))]
create_exception!(phenolrs, PhenolError, PyException);

/// Loads a graph (from the name and description, into a PyG friendly format
/// Requires numpy as a runtime dependency
#[cfg(not(test))]
#[pyfunction]
#[cfg(not(test))]
fn graph_to_numpy_format(py: Python, request: DataLoadRequest) -> PyResult<PygCompatible> {
    let graph_factory = NumpyGraph::new;

    println!("Retrieving Numpy Graph...");
    let graph =
        load::retrieve::get_arangodb_graph(request, graph_factory).map_err(PhenolError::new_err)?;
    println!("Retrieved. Building python objects...");

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
    println!("Built python objects.");

    let res = (
        col_to_features,
        coo_by_from_edge_to,
        cols_to_keys_to_inds,
        cols_to_inds_to_keys,
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
    &PyDict,
    &PyDict,
    &PyArray1<usize>,
    &PyArray1<usize>,
    &PyDict,
)> {
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
            graph_config.symmterize_edges_if_directed,
        )
    };

    println!("Retrieving NetworkX Graph...");
    let graph = load::retrieve::get_arangodb_graph(request, graph_factory).unwrap();
    println!("Retrieved. Building python objects...");

    let node_dict = construct::construct_dict_of_dict(graph.node_map, py)?;
    let adj_dict = if graph_config.is_multigraph {
        if graph_config.is_directed {
            construct::construct_multidigraph(graph.adj_map_multidigraph, py)?
        } else {
            construct::construct_multigraph(graph.adj_map_multigraph, py)?
        }
    } else {
        if graph_config.is_directed {
            construct::construct_digraph(graph.adj_map_digraph, py)?
        } else {
            construct::construct_graph(graph.adj_map_graph, py)?
        }
    };
    println!("Built python objects.");

    let coo = graph.coo;
    let src_indices = PyArray1::from_vec(py, coo.0);
    let dst_indices = PyArray1::from_vec(py, coo.1);
    let vertex_id_to_index = construct::construct_dict(graph.vertex_id_to_index, py)?;

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
    m.add_function(wrap_pyfunction!(graph_to_numpy_format, m)?)?;
    m.add_function(wrap_pyfunction!(graph_to_networkx_format, m)?)?;
    m.add("PhenolError", py.get_type::<PhenolError>())?;
    Ok(())
}
