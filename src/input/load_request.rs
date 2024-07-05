pub struct CollectionDescription {
    pub name: String,
    pub fields: Vec<String>,
}

pub struct DataLoadRequest {
    pub database: String,
    pub vertex_collections: Vec<CollectionDescription>,
    pub edge_collections: Vec<CollectionDescription>,
    pub configuration: Configuration,
}

pub struct Configuration {
    pub database_config: DatabaseConfiguration,
    pub load_config: LoadConfiguration,
    // pub graph_config: GraphConfiguration,
}

impl Configuration {
    pub fn default() -> Configuration {
        Configuration {
            database_config: DatabaseConfiguration::default(),
            load_config: LoadConfiguration::default(),
            // graph_config: GraphConfiguration::default(),
        }
    }
}

#[derive(Clone)]
pub struct DatabaseConfiguration {
    pub endpoints: Vec<String>,
    // optional components of this configuration
    pub username: Option<String>,
    pub password: Option<String>,
    pub jwt_token: Option<String>,
    pub tls_cert: Option<String>,
}

impl DatabaseConfiguration {
    pub fn default() -> DatabaseConfiguration {
        DatabaseConfiguration {
            endpoints: vec!["http://localhost:8529".into()],
            username: Some("root".into()),
            password: Some("".into()),
            jwt_token: None,
            tls_cert: None,
        }
    }
}

// TODO Anthony: Abstract this into a GraphConfiguration struct
#[derive(Clone)]
pub struct NetworkXGraphConfig {
    pub load_node_dict: bool,
    pub load_adj_dict: bool,
    pub load_adj_dict_as_directed: bool,
    pub load_adj_dict_as_multigraph: bool,
    pub load_coo: bool,
}

impl NetworkXGraphConfig {
    pub fn default() -> NetworkXGraphConfig {
        NetworkXGraphConfig {
            load_node_dict: true,
            load_adj_dict: true,
            load_adj_dict_as_directed: true,
            load_adj_dict_as_multigraph: true,
            load_coo: true,
        }
    }
}

#[derive(Clone)]
pub struct LoadConfiguration {
    pub parallelism: Option<u32>,
    pub batch_size: Option<u64>,
    pub load_vertices: bool,
    pub load_edges: bool,
    pub load_all_attributes_via_aql: bool,
}

impl LoadConfiguration {
    pub fn default() -> LoadConfiguration {
        LoadConfiguration {
            parallelism: Some(10),
            batch_size: Some(1000000),
            load_vertices: true,
            load_edges: true,
            load_all_attributes_via_aql: false,
        }
    }
}
