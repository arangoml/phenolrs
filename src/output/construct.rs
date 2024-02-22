use ndarray::{Array, Ix2};
use numpy::ToPyArray;
use pyo3::types::PyDict;
use pyo3::{PyResult, Python};
use std::collections::HashMap;

#[cfg(not(test))] // not(test) is needed to let us use `cargo test`
pub fn construct_col_to_features<'pl>(
    input: HashMap<String, HashMap<String, Array<f64, Ix2>>>,
    py: Python<'pl>,
) -> PyResult<&'pl PyDict> {
    let dict = PyDict::new(py);
    (&input).into_iter().for_each(|(col_name, feature_map)| {
        let col_dict = PyDict::new(py);
        feature_map.iter().for_each(|(feat_name, arr)| {
            col_dict.set_item(feat_name, arr.to_pyarray(py)).unwrap();
        });
        dict.set_item(col_name, col_dict).unwrap();
    });
    Ok(dict)
}

#[cfg(not(test))]
pub fn construct_coo_by_from_edge_to<'pl>(
    input: HashMap<(String, String, String), Array<usize, Ix2>>,
    py: Python<'pl>,
) -> PyResult<&'pl PyDict> {
    let dict = PyDict::new(py);
    (&input)
        .into_iter()
        .for_each(|item| dict.set_item(item.0, item.1.to_pyarray(py)).unwrap());
    Ok(dict)
}

#[cfg(not(test))]
pub fn construct_cols_to_keys_to_inds<'pl>(
    input: HashMap<String, HashMap<String, usize>>,
    py: Python<'pl>,
) -> PyResult<&'pl PyDict> {
    let dict = PyDict::new(py);
    (&input)
        .into_iter()
        .for_each(|item| dict.set_item(item.0, item.1).unwrap());
    Ok(dict)
}
