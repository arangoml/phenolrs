use lightning::{CollectionInfo, DataLoadConfiguration, DatabaseConfiguration};

// TODO: remove database from toplevel. already used in db_config
pub struct DataLoadRequest {
    pub database: String,
    pub vertex_collections: Vec<CollectionInfo>,
    pub edge_collections: Vec<CollectionInfo>,
    pub db_config: DatabaseConfiguration,
    pub load_config: DataLoadConfiguration,
}
