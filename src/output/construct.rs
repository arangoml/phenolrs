// use numpy::ToPyArray;
use pyo3::types::PyDict;
use pyo3::{PyResult, Python};
use serde_json::{Map, Value};
use std::collections::HashMap;

#[cfg(not(test))]
use pyo3::prelude::*;
// use pyo3::types::IntoPyDict;

// #[cfg(not(test))] // not(test) is needed to let us use `cargo test`
// pub fn construct_col_to_features(
//     input: HashMap<String, HashMap<String, Array<f64, Ix2>>>,
//     py: Python,
// ) -> PyResult<&PyDict> {
//     let dict = PyDict::new(py);
//     input.iter().for_each(|(col_name, feature_map)| {
//         let col_dict = PyDict::new(py);
//         feature_map.iter().for_each(|(feat_name, arr)| {
//             col_dict.set_item(feat_name, arr.to_pyarray(py)).unwrap();
//         });
//         dict.set_item(col_name, col_dict).unwrap();
//     });
//     Ok(dict)
// }

// #[cfg(not(test))]
// pub fn construct_coo_by_from_edge_to(
//     input: HashMap<(String, String, String), Array<usize, Ix2>>,
//     py: Python,
// ) -> PyResult<&PyDict> {
//     let dict = PyDict::new(py);
//     input
//         .iter()
//         .for_each(|item| dict.set_item(item.0, item.1.to_pyarray(py)).unwrap());
//     Ok(dict)
// }

// #[cfg(not(test))]
// pub fn construct_cols_to_keys_to_inds(
//     input: HashMap<String, HashMap<String, usize>>,
//     py: Python,
// ) -> PyResult<&PyDict> {
//     let dict = PyDict::new(py);
//     input
//         .iter()
//         .for_each(|item| dict.set_item(item.0, item.1).unwrap());
//     Ok(dict)
// }

#[cfg(not(test))]
pub fn construct_dict(input: HashMap<String, usize>, py: Python) -> PyResult<&PyDict> {
    let pydict = PyDict::new(py);

    for (key, value) in input {
        pydict.set_item(key, value)?;
    }

    Ok(pydict)
}

#[cfg(not(test))]
pub fn construct_dict_of_dict(
    input: HashMap<String, Map<String, Value>>,
    py: Python,
) -> PyResult<&PyDict> {
    let pydict = PyDict::new(py);

    for (key, properties) in input.iter() {
        let inner_dict = PyDict::new(py);
        for (property_key, property_value) in properties {
            let py_value = convert_to_py_object(property_value, py)?;
            inner_dict.set_item(property_key, py_value)?;
        }
        pydict.set_item(key, inner_dict)?;
    }

    Ok(pydict)
}

#[cfg(not(test))]
pub fn construct_dict_of_dict_of_dict(
    input: HashMap<String, HashMap<String, Map<String, Value>>>,
    py: Python,
) -> PyResult<&PyDict> {
    let pydict = PyDict::new(py);

    for (key, properties) in input.iter() {
        let inner_dict = PyDict::new(py);
        for (property_key, property_value) in properties.iter() {
            let inner_inner_dict = PyDict::new(py);
            for (inner_property_key, inner_property_value) in property_value {
                let py_value = convert_to_py_object(inner_property_value, py)?;
                inner_inner_dict.set_item(inner_property_key, py_value)?;
            }
            inner_dict.set_item(property_key, inner_inner_dict)?;
        }
        pydict.set_item(key, inner_dict)?;
    }

    Ok(pydict)
}

#[cfg(not(test))]
fn convert_to_py_object(value: &Value, py: Python) -> PyResult<PyObject> {
    match value {
        Value::String(s) => Ok(s.to_object(py)),
        Value::Number(num) => Ok(num.to_string().to_object(py)), // Convert to string to avoid precision issues
        Value::Bool(b) => Ok(b.to_object(py)),
        Value::Null => Ok(py.None()),
        _ => Ok(py.None()), // Simplify handling for arrays and objects
    }
}
