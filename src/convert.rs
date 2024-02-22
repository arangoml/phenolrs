use std::collections::HashMap;
use ndarray::{Array2, Axis};

pub fn convert_coo_edge_map(coo_edge_map: HashMap<(String, String, String), Vec<Vec<usize>>>) -> HashMap<(String, String, String), Array2<usize>> {
    coo_edge_map.iter().filter_map(|(edge_tup, edge_mat)| {
        match twod_vec_to_array(edge_mat) {
            None => None,
            Some(arr) => Some((edge_tup.clone(), arr))
        }
    }).collect()
}

pub fn convert_nested_features_map(nested_features_map: HashMap<String, HashMap<String, Vec<Vec<f64>>>>) -> HashMap<String, HashMap<String, Array2<f64>>> {
    nested_features_map.iter().map(|(col_name, features)| {
        let col_map: HashMap<String, Array2<f64>> = features.iter().filter_map(|(feature_name, nested_feature_vec)| {
            match twod_vec_to_array(nested_feature_vec) {
                None => None,
                Some(arr) => Some((feature_name.clone(), arr))
            }
        }).collect();
        (col_name.clone(), col_map)
    }).collect()
}

fn twod_vec_to_array<T: std::default::Default + Copy>(twod: &Vec<Vec<T>>) -> Option<Array2<T>> {
    if twod.is_empty() {
        return None;
    }
    let m: usize = twod.len();
    let n =
        if twod.len() > 0 {
            twod[0].len()
        } else {
            0
        };
    let mut arr = Array2::<T>::default((m, n));
    for (i, mut row) in arr.axis_iter_mut(Axis(0)).enumerate() {
        for (j, col) in row.iter_mut().enumerate() {
            *col = twod[i][j];
        }
    }
    Some(arr)
}

