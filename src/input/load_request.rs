use lightning::{CollectionInfo, DataLoadConfiguration, DatabaseConfiguration};

pub struct DataLoadRequest {
    pub database: String,
    pub vertex_collections: Vec<CollectionInfo>,
    pub edge_collections: Vec<CollectionInfo>,
    pub db_config: DatabaseConfiguration,
    pub load_config: DataLoadConfiguration,
}
