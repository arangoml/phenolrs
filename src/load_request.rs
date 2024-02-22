pub struct CollectionDescription {
    pub name: String,
    pub fields: Vec<String>,
}

pub struct DataLoadRequest {
    pub database: String,
    pub vertex_collections: Vec<CollectionDescription>,
    pub edge_collections: Vec<CollectionDescription>,
    pub configuration: DataLoadConfiguration,
}

pub struct DataLoadConfiguration {
    pub database_config: DatabaseConfiguration,
    pub parallelism: Option<u32>,
    pub batch_size: Option<u64>,
}

pub struct DatabaseConfiguration {
    pub endpoints: Vec<String>,
    // optional components of this configuration
    pub username: Option<String>,
    pub password: Option<String>,
    pub jwt_token: Option<String>,
    pub tls_cert_location: Option<String>,
}
