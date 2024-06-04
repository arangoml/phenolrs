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
    pub dump_config: DumpConfiguration,
    // pub graph_config: GraphConfiguration,
}

impl DataLoadConfiguration {
    pub fn default() -> DataLoadConfiguration {
        DataLoadConfiguration {
            database_config: DatabaseConfiguration::default(),
            dump_config: DumpConfiguration::default(),
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

// #[derive(Clone)]
// pub struct GraphConfiguration {
//     pub load_node_dict: bool,
//     pub load_adj_dict: bool,
//     pub load_adj_dict_as_directed: bool,
//     pub load_coo: bool,
// }

// impl GraphConfiguration {
//     pub fn default() -> GraphConfiguration {
//         GraphConfiguration {
//             load_node_dict: true,
//             load_adj_dict: true,
//             load_adj_dict_as_directed: true,
//             load_coo: true,
//         }
//     }
// }

#[derive(Clone)]
pub struct DumpConfiguration {
    pub parallelism: Option<u32>,
    pub batch_size: Option<u64>,
}

impl DumpConfiguration {
    pub fn default() -> DumpConfiguration {
        DumpConfiguration {
            parallelism: Some(10),
            batch_size: Some(1000000),
        }
    }
}