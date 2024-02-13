use std::collections::HashMap;
use numpy::{Ix2, ToPyArray, PyArray, IntoPyArray, ndarray};
use ndarray::array;
use ndarray::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyDict;


#[pyclass]
struct PygCompatible {
    pub col_to_features: HashMap<String, Array<f64, Ix2>>,
    pub coo_by_from_edge_to: HashMap<(String, String, String), Array<usize, Ix2>>,
    pub cols_to_keys_to_inds: HashMap<String, HashMap<String, usize>>
}

#[pymethods]
impl PygCompatible {
    #[getter]
    fn col_to_features<'pl>(&self, py: Python<'pl>) -> PyResult<&'pl PyDict> {
        let dict = PyDict::new(py);
        (&self.col_to_features).into_iter()
            .for_each(|item| {
                dict.set_item(item.0, item.1.to_pyarray(py)).unwrap()
            });
        Ok(dict)
    }

    #[getter]
    fn coo_by_from_edge_to<'pl>(&self, py: Python<'pl>) -> PyResult<&'pl PyDict> {
        let dict = PyDict::new(py);
        (&self.coo_by_from_edge_to).into_iter()
            .for_each(|item| {
                dict.set_item(item.0, item.1.to_pyarray(py)).unwrap()
            });
        Ok(dict)
    }

    #[getter]
    fn cols_to_keys_to_inds<'pl>(&self, py: Python<'pl>) -> PyResult<&'pl PyDict> {
        let dict = PyDict::new(py);
        (&self.cols_to_keys_to_inds).into_iter()
            .for_each(|item| {
                dict.set_item(item.0, item.1).unwrap()
            });
        Ok(dict)
    }
}



/// Loads a graph (from the name and description, into a PyG friendly format
/// Requires numpy as a runtime dependency
#[pyfunction]
fn graph_to_pyg_format(py: Python<'_>) -> PyResult<PygCompatible> {
    let mut res = PygCompatible {
        col_to_features: HashMap::new(),
        coo_by_from_edge_to: HashMap::new(),
        cols_to_keys_to_inds: HashMap::new(),
    };
    let mut foo = HashMap::new();
    foo.insert("test".to_string(), 1);
    res.cols_to_keys_to_inds.insert("foo".to_string(), foo);
    Ok(res)
}

/// A Python module implemented in Rust.
#[pymodule]
fn phenolrs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(graph_to_pyg_format, m)?)?;
    Ok(())
}
