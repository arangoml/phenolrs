use crate::input::load_request::{DataLoadRequest, NetworkXGraphConfig};
use lightning::graph_loader::CollectionInfo;
use lightning::{DataLoadConfiguration, DatabaseConfiguration};
use pyo3::exceptions::PyValueError;
use pyo3::types::PyDict;
use pyo3::{FromPyObject, PyAny, PyResult};

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
    fn extract(ob: &PyAny) -> PyResult<Self> {
        let input_dict: &PyDict = ob.downcast()?;
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
    fn extract(ob: &PyAny) -> PyResult<Self> {
        let input_dict: &PyDict = ob.downcast()?;
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
    fn extract(ob: &'_ PyAny) -> PyResult<Self> {
        let input_dict: &PyDict = ob.downcast()?;
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
    fn extract(ob: &'_ PyAny) -> PyResult<Self> {
        let input_dict: &PyDict = ob.downcast()?;
        let name: &str = input_dict.get_item("name")?.map_or_else(
            || Err(PyValueError::new_err("Collection name not set")),
            |s| s.extract(),
        )?;
        let fields: Vec<&str> = input_dict
            .get_item("fields")?
            .map_or_else(|| Ok(vec![]), |s| s.extract())?;
        Ok(LocalCollectionInfo(CollectionInfo {
            name: name.into(),
            fields: fields.iter().map(|s| String::from(*s)).collect(),
        }))
    }
}

impl FromPyObject<'_> for NetworkXGraphConfig {
    fn extract(ob: &'_ PyAny) -> PyResult<Self> {
        let input_dict: &PyDict = ob.downcast()?;
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
