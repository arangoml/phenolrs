use crate::input::load_request::{
    create_aql_query, create_data_item, AqlDataLoadRequest, DataLoadRequest, NetworkXGraphConfig,
};
use arangors_graph_exporter::graph_loader::CollectionInfo;
use arangors_graph_exporter::{AqlQuery, DataItem, DataLoadConfiguration, DatabaseConfiguration};
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyDict, PyList};
use pyo3::{prelude::*, Bound, FromPyObject, PyAny, PyResult};
use pythonize::depythonize;
use std::collections::HashMap;

#[derive(Default)]
pub struct LocalDataLoadConfiguration(pub DataLoadConfiguration);

impl From<LocalDataLoadConfiguration> for DataLoadConfiguration {
    fn from(local: LocalDataLoadConfiguration) -> Self {
        local.0
    }
}
#[derive(Default)]
pub struct LocalDatabaseConfiguration(pub DatabaseConfiguration);

impl From<LocalDatabaseConfiguration> for DatabaseConfiguration {
    fn from(local: LocalDatabaseConfiguration) -> Self {
        local.0
    }
}
pub struct LocalCollectionInfo(pub CollectionInfo);

pub fn create_collection_info_vec(
    collection_info: Vec<LocalCollectionInfo>,
) -> Vec<CollectionInfo> {
    collection_info.iter().map(|c| c.0.clone()).collect()
}

impl FromPyObject<'_> for DataLoadRequest {
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        let input_dict = ob.downcast::<PyDict>()?;
        let db_config: LocalDatabaseConfiguration = input_dict
            .get_item("database_config")?
            .map_or(Ok(LocalDatabaseConfiguration::default()), |c| c.extract())?;
        let load_config: LocalDataLoadConfiguration = input_dict
            .get_item("load_config")?
            .map_or(Ok(LocalDataLoadConfiguration::default()), |c| c.extract())?;
        let vertex_collections: Vec<LocalCollectionInfo> =
            input_dict.get_item("vertex_collections")?.map_or_else(
                || Err(PyValueError::new_err("vertex_collections not provided")),
                |s| s.extract(),
            )?;
        let edge_collections: Vec<LocalCollectionInfo> =
            input_dict.get_item("edge_collections")?.map_or_else(
                || Err(PyValueError::new_err("edge_collections not provided")),
                |s| s.extract(),
            )?;
        Ok(DataLoadRequest {
            vertex_collections: create_collection_info_vec(vertex_collections),
            edge_collections: create_collection_info_vec(edge_collections),
            load_config: load_config.into(),
            db_config: db_config.into(),
        })
    }
}

impl FromPyObject<'_> for LocalDataLoadConfiguration {
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        let input_dict = ob.downcast::<PyDict>()?;
        let parallelism: u32 = input_dict
            .get_item("parallelism")?
            .map_or(Ok(8), |v| v.extract())?;
        let batch_size: u64 = input_dict
            .get_item("batch_size")?
            .map_or(Ok(400000), |v| v.extract())?;
        let prefetch_count: u32 = input_dict
            .get_item("prefetch_count")?
            .map_or(Ok(5), |v| v.extract())?;
        let load_all_vertex_attributes: bool = input_dict
            .get_item("load_all_vertex_attributes")?
            .map_or(Ok(false), |v| v.extract())?;
        let load_all_edge_attributes: bool = input_dict
            .get_item("load_all_edge_attributes")?
            .map_or(Ok(false), |v| v.extract())?;
        Ok(LocalDataLoadConfiguration(DataLoadConfiguration {
            parallelism,
            batch_size,
            prefetch_count,
            load_all_vertex_attributes,
            load_all_edge_attributes,
        }))
    }
}

impl FromPyObject<'_> for LocalDatabaseConfiguration {
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        let input_dict = ob.downcast::<PyDict>()?;
        let database: String = input_dict
            .get_item("database")?
            .map_or_else(|| Ok("_system".into()), |c| c.extract())?;
        let endpoints: Vec<String> = input_dict
            .get_item("endpoints")?
            .map_or_else(|| Ok(vec!["http://localhost:8529".into()]), |c| c.extract())?;
        let username: String = input_dict
            .get_item("username")?
            .map_or_else(|| Ok("root".into()), |c| c.extract())?;
        let password: String = input_dict
            .get_item("password")?
            .map_or_else(|| Ok("".into()), |c| c.extract())?;
        let jwt_token: String = input_dict
            .get_item("jwt_token")?
            .map_or_else(|| Ok("".into()), |c| c.extract())?;
        let tls_cert: Option<String> = input_dict
            .get_item("tls_cert")?
            .map_or_else(|| Ok(None), |c| c.extract())?;
        Ok(LocalDatabaseConfiguration(DatabaseConfiguration {
            database,
            endpoints,
            username,
            password,
            jwt_token,
            tls_cert,
        }))
    }
}

impl FromPyObject<'_> for LocalCollectionInfo {
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        let input_dict = ob.downcast::<PyDict>()?;
        let name: String = input_dict.get_item("name")?.map_or_else(
            || Err(PyValueError::new_err("Collection name not set")),
            |s| s.extract::<String>(),
        )?;
        let fields: Vec<String> = input_dict
            .get_item("fields")?
            .map_or_else(|| Ok(vec![]), |s| s.extract())?;
        Ok(LocalCollectionInfo(CollectionInfo {
            name: name.into(),
            fields,
        }))
    }
}

impl FromPyObject<'_> for NetworkXGraphConfig {
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        let input_dict = ob.downcast::<PyDict>()?;
        let load_adj_dict: bool = input_dict
            .get_item("load_adj_dict")?
            .map_or_else(|| Ok(true), |c| c.extract())?;
        let load_coo: bool = input_dict
            .get_item("load_coo")?
            .map_or_else(|| Ok(true), |c| c.extract())?;
        let is_directed: bool = input_dict
            .get_item("is_directed")?
            .map_or_else(|| Ok(true), |c| c.extract())?;
        let is_multigraph: bool = input_dict
            .get_item("is_multigraph")?
            .map_or_else(|| Ok(true), |c| c.extract())?;
        let symmetrize_edges_if_directed: bool = input_dict
            .get_item("symmetrize_edges_if_directed")?
            .map_or_else(|| Ok(false), |c| c.extract())?;
        Ok(NetworkXGraphConfig {
            load_adj_dict,
            load_coo,
            is_directed,
            is_multigraph,
            symmetrize_edges_if_directed,
        })
    }
}

/// Helper struct for parsing a single AQL query from Python
pub struct LocalAqlQuery(pub AqlQuery);

impl FromPyObject<'_> for LocalAqlQuery {
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        let input_dict = ob.downcast::<PyDict>()?;

        let query: String = input_dict.get_item("query")?.map_or_else(
            || Err(PyValueError::new_err("AQL query string is required")),
            |s| s.extract::<String>(),
        )?;

        // Parse bind_vars - can be "bindVars" or "bind_vars" for flexibility
        let bind_vars: HashMap<String, serde_json::Value> = input_dict
            .get_item("bindVars")?
            .or(input_dict.get_item("bind_vars")?)
            .map_or_else(
                || Ok::<HashMap<String, serde_json::Value>, PyErr>(HashMap::new()),
                |v| {
                    depythonize(&v).map_err(|e| {
                        PyValueError::new_err(format!("Failed to parse bind_vars: {}", e))
                    })
                },
            )?;

        Ok(LocalAqlQuery(create_aql_query(query, bind_vars)))
    }
}

/// Helper struct for parsing a DataItem (attribute definition) from Python
pub struct LocalDataItem(pub DataItem);

impl FromPyObject<'_> for LocalDataItem {
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        let input_dict = ob.downcast::<PyDict>()?;

        let name: String = input_dict.get_item("name")?.map_or_else(
            || Err(PyValueError::new_err("Attribute name is required")),
            |s| s.extract::<String>(),
        )?;

        let type_str: String = input_dict
            .get_item("type")?
            .or(input_dict.get_item("data_type")?)
            .map_or_else(
                || Err(PyValueError::new_err("Attribute type is required")),
                |s| s.extract::<String>(),
            )?;

        create_data_item(name, &type_str)
            .map(|di| LocalDataItem(di))
            .map_err(|e| PyValueError::new_err(e))
    }
}

impl FromPyObject<'_> for AqlDataLoadRequest {
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        let input_dict = ob.downcast::<PyDict>()?;

        // Parse database configuration
        let db_config: LocalDatabaseConfiguration = input_dict
            .get_item("database_config")?
            .map_or(Ok(LocalDatabaseConfiguration::default()), |c| c.extract())?;

        // Parse batch size (default 10000)
        let batch_size: u64 = input_dict
            .get_item("batch_size")?
            .map_or(Ok(10000), |v| v.extract())?;

        // Parse vertex attributes - expected format: [{"name": "attr1", "type": "string"}, ...]
        // Or simplified: {"attr1": "string", "attr2": "number"}
        let vertex_attributes: Vec<DataItem> = input_dict
            .get_item("vertex_attributes")?
            .map_or_else(|| Ok(vec![]), |v| parse_attributes(v))?;

        // Parse edge attributes
        let edge_attributes: Vec<DataItem> = input_dict
            .get_item("edge_attributes")?
            .map_or_else(|| Ok(vec![]), |v| parse_attributes(v))?;

        // Parse queries - expected format: [[{query, bindVars}, ...], [...], ...]
        let queries: Vec<Vec<AqlQuery>> = input_dict.get_item("queries")?.map_or_else(
            || Err(PyValueError::new_err("queries is required")),
            |v| {
                let outer_list = v.downcast::<PyList>()?;
                let mut result = vec![];
                for group in outer_list.iter() {
                    let inner_list = group.downcast::<PyList>()?;
                    let mut group_queries = vec![];
                    for query_obj in inner_list.iter() {
                        let local_query: LocalAqlQuery = query_obj.extract()?;
                        group_queries.push(local_query.0);
                    }
                    result.push(group_queries);
                }
                Ok(result)
            },
        )?;

        Ok(AqlDataLoadRequest {
            db_config: db_config.into(),
            batch_size,
            vertex_attributes,
            edge_attributes,
            queries,
        })
    }
}

/// Parse attributes from either dict format {"attr": "type"} or list format [{"name": "attr", "type": "type"}]
fn parse_attributes(ob: Bound<'_, PyAny>) -> PyResult<Vec<DataItem>> {
    // Try dict format first: {"attr1": "string", "attr2": "number"}
    if let Ok(dict) = ob.downcast::<PyDict>() {
        let mut items = vec![];
        for (key, value) in dict.iter() {
            let name: String = key.extract()?;
            let type_str: String = value.extract()?;
            let item = create_data_item(name, &type_str).map_err(|e| PyValueError::new_err(e))?;
            items.push(item);
        }
        return Ok(items);
    }

    // Try list format: [{"name": "attr1", "type": "string"}, ...]
    if let Ok(list) = ob.downcast::<PyList>() {
        let mut items = vec![];
        for item in list.iter() {
            let local_item: LocalDataItem = item.extract()?;
            items.push(local_item.0);
        }
        return Ok(items);
    }

    Err(PyValueError::new_err(
        "Attributes must be a dict {'name': 'type'} or list [{'name': 'n', 'type': 't'}]",
    ))
}
