use lightning::graph_loader::{CollectionInfo};
use lightning::{DataLoadConfiguration, DatabaseConfiguration};
use pyo3::exceptions::PyValueError;
use pyo3::types::PyDict;
use pyo3::{FromPyObject, PyAny, PyResult};
use crate::input::load_request::DataLoadRequest;

pub struct LocalDataLoadConfiguration(pub DataLoadConfiguration);

impl From<LocalDataLoadConfiguration> for DataLoadConfiguration {
    fn from(local: LocalDataLoadConfiguration) -> Self {
        local.0
    }
}
pub struct LocalDatabaseConfiguration(pub DatabaseConfiguration);

impl From<LocalDatabaseConfiguration> for DatabaseConfiguration {
    fn from(local: LocalDatabaseConfiguration) -> Self {
        local.0
    }
}
pub struct LocalCollectionInfo(pub CollectionInfo);

pub fn create_collection_info_vec(collection_info: Vec<LocalCollectionInfo>) -> Vec<CollectionInfo> {
    collection_info.iter().map(|c| c.0.clone()).collect()
}

impl Default for LocalDatabaseConfiguration {
    fn default() -> Self {
        LocalDatabaseConfiguration(DatabaseConfiguration::default())
    }
}

impl Default for LocalDataLoadConfiguration {
    fn default() -> Self {
        LocalDataLoadConfiguration(DataLoadConfiguration::default())
    }
}

impl FromPyObject<'_> for DataLoadRequest {
    fn extract(ob: &PyAny) -> PyResult<Self> {
        let input_dict: &PyDict = ob.downcast()?;
        let database_value: &str = input_dict.get_item("database")?.map_or_else(
            || Err(PyValueError::new_err("Database not set")),
            |s| s.extract(),
        )?;
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
            database: database_value.to_string(),
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
        Ok(LocalDataLoadConfiguration(DataLoadConfiguration {
            parallelism,
            batch_size,
            prefetch_count,
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
