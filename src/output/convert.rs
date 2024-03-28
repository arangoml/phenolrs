use ndarray::{Array2, Axis};
use std::collections::HashMap;
use std::hash::Hash;
use pyo3_tch::PyTensor;
use pyo3_tch::tch::{TchError, Tensor};

pub fn convert_coo_edge_map(
    coo_edge_map: HashMap<(String, String, String), Vec<Vec<usize>>>,
) -> Result<HashMap<(String, String, String), PyTensor>, TchError> {
    let mut coo_out_edge_map = HashMap::new();
    coo_edge_map
        .iter()
        .try_for_each(|(edge_tup, edge_mat)| {
            let tens = two_dim_usize_to_pytensor(edge_mat)?;
            coo_out_edge_map.insert(edge_tup.clone(), tens);
            Ok::<(), TchError>(())
        })?;
    Ok(coo_out_edge_map)
}

pub fn convert_nested_features_map(
    nested_features_map: HashMap<String, HashMap<String, Vec<Vec<f64>>>>
) -> Result<HashMap<String, HashMap<String, PyTensor>>, TchError> {
    let mut nested_torch_features = HashMap::new();
    nested_features_map.iter().try_for_each(|(col_name, features)| {
        let mut col_map = HashMap::new();
        features.iter()
            .try_for_each(|(feature_name, nested_feature_vec)| {
                let py_tensor = two_dim_vec64_to_pytensor(nested_feature_vec)?;
                col_map.insert(feature_name.clone(), py_tensor);
                Ok::<(), TchError>(())
            })?;
        nested_torch_features.insert(col_name.clone(), col_map);
        Ok::<(), TchError>(())
    })?;
    Ok(nested_torch_features)
}

fn two_dim_vec_to_array<T: Default + Copy>(twod: &[Vec<T>]) -> Option<Array2<T>> {
    if twod.is_empty() {
        return None;
    }
    let m: usize = twod.len();
    let n = if !twod.is_empty() { twod[0].len() } else { 0 };
    let mut arr = Array2::<T>::default((m, n));
    for (i, mut row) in arr.axis_iter_mut(Axis(0)).enumerate() {
        for (j, col) in row.iter_mut().enumerate() {
            *col = twod[i][j];
        }
    }
    Some(arr)
}


fn two_dim_vec64_to_pytensor(twod: &[Vec<f64>]) -> Result<PyTensor, TchError> {
    if twod.is_empty() {
        return Ok(PyTensor(Tensor::new()));
    }

    // let m: usize = twod.len();
    // let n = if !twod.is_empty() { twod[0].len() } else { 0 };
    let mut tensors: Vec<Tensor> = vec![];
    twod.iter().try_for_each(|v| {
        let t = Tensor::f_from_slice(v.as_ref())?;
        tensors.push(t);
        Ok::<(), TchError>(())
    })?;
    Ok(PyTensor(Tensor::stack(&tensors, 0)))
}

fn two_dim_usize_to_pytensor(twod: &[Vec<usize>]) -> Result<PyTensor, TchError> {
    if twod.is_empty() {
        return Ok(PyTensor(Tensor::new()));
    }

    // let m: usize = twod.len();
    // let n = if !twod.is_empty() { twod[0].len() } else { 0 };
    let mut tensors: Vec<Tensor> = vec![];
    twod.iter().try_for_each(|v| {
        let t = Tensor::f_from_slice(v.iter().map(|val| *val as i32).collect::<Vec<i32>>().as_slice())?;
        tensors.push(t);
        Ok::<(), TchError>(())
    })?;
    Ok(PyTensor(Tensor::stack(&tensors, 0)))
}