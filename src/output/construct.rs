use ndarray::{Array, Ix2};
use numpy::ToPyArray;
use pyo3::types::{PyDict, PyList};
use pyo3::{PyResult, Python};
use std::collections::HashMap;

use serde_json::{Map, Value};

#[cfg(not(test))]
use pyo3::prelude::*;

#[cfg(not(test))] // not(test) is needed to let us use `cargo test`
pub fn construct_col_to_features(
    input: HashMap<String, HashMap<String, Array<f64, Ix2>>>,
    py: Python,
) -> PyResult<&PyDict> {
    let dict = PyDict::new(py);
    input.iter().for_each(|(col_name, feature_map)| {
        let col_dict = PyDict::new(py);
        feature_map.iter().for_each(|(feat_name, arr)| {
            col_dict.set_item(feat_name, arr.to_pyarray(py)).unwrap();
        });
        dict.set_item(col_name, col_dict).unwrap();
    });
    Ok(dict)
}

#[cfg(not(test))]
pub fn construct_coo_by_from_edge_to(
    input: HashMap<(String, String, String), Array<usize, Ix2>>,
    py: Python,
) -> PyResult<&PyDict> {
    let dict = PyDict::new(py);
    input
        .iter()
        .for_each(|item| dict.set_item(item.0, item.1.to_pyarray(py)).unwrap());
    Ok(dict)
}

#[cfg(not(test))]
pub fn construct_cols_to_keys_to_inds(
    input: HashMap<String, HashMap<String, usize>>,
    py: Python,
) -> PyResult<&PyDict> {
    let dict = PyDict::new(py);
    input
        .iter()
        .for_each(|item| dict.set_item(item.0, item.1).unwrap());
    Ok(dict)
}

#[cfg(not(test))]
pub fn construct_cols_to_inds_to_keys(
    input: HashMap<String, HashMap<usize, String>>,
    py: Python,
) -> PyResult<&PyDict> {
    let dict = PyDict::new(py);
    input
        .iter()
        .for_each(|item| dict.set_item(item.0, item.1).unwrap());
    Ok(dict)
}

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
            let py_value = construct_py_object(property_value, py)?;
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
                let py_value = construct_py_object(inner_property_value, py)?;
                inner_inner_dict.set_item(inner_property_key, py_value)?;
            }
            inner_dict.set_item(property_key, inner_inner_dict)?;
        }
        pydict.set_item(key, inner_dict)?;
    }

    Ok(pydict)
}

#[cfg(not(test))]
pub fn construct_dict_of_dict_of_dict_of_dict(
    input: HashMap<String, HashMap<String, HashMap<usize, Map<String, Value>>>>,
    py: Python,
) -> PyResult<&PyDict> {
    let pydict = PyDict::new(py);

    for (key, properties) in input.iter() {
        let inner_dict = PyDict::new(py);
        for (property_key, property_value) in properties.iter() {
            let inner_inner_dict = PyDict::new(py);
            for (inner_property_key, inner_property_value) in property_value.iter() {
                let inner_inner_inner_dict = PyDict::new(py);
                for (inner_inner_property_key, inner_inner_property_value) in inner_property_value {
                    let py_value = construct_py_object(inner_inner_property_value, py)?;
                    inner_inner_inner_dict.set_item(inner_inner_property_key, py_value)?;
                }
                inner_inner_dict.set_item(inner_property_key, inner_inner_inner_dict)?;
            }
            inner_dict.set_item(property_key, inner_inner_dict)?;
        }
        pydict.set_item(key, inner_dict)?;
    }

    Ok(pydict)
}

#[cfg(not(test))]
fn construct_py_object(value: &Value, py: Python) -> PyResult<PyObject> {
    match value {
        Value::Null => Ok(py.None()),
        Value::String(s) => Ok(s.to_object(py)),
        Value::Bool(b) => Ok(b.to_object(py)),
        Value::Number(num) => {
            if let Some(i) = num.as_i64() {
                Ok(i.to_object(py))
            } else if let Some(u) = num.as_u64() {
                Ok(u.to_object(py))
            } else {
                Ok(num.as_f64().unwrap().to_object(py))
            }
        }
        Value::Array(arr) => {
            let py_list = PyList::empty(py);
            for item in arr {
                py_list.append(construct_py_object(item, py)?)?;
            }
            Ok(py_list.to_object(py))
        }
        Value::Object(obj) => {
            let py_dict = PyDict::new(py);
            for (key, value) in obj {
                py_dict.set_item(key, construct_py_object(value, py)?)?;
            }
            Ok(py_dict.to_object(py))
        }
    }
}
