use lightning::{CollectionInfo, DataLoadConfiguration, DatabaseConfiguration};

pub struct DataLoadRequest {
    pub vertex_collections: Vec<CollectionInfo>,
    pub edge_collections: Vec<CollectionInfo>,
    pub db_config: DatabaseConfiguration,
    pub load_config: DataLoadConfiguration,
    // pub graph_config: GraphConfiguration
}

pub struct NetworkXGraphConfig {
    pub load_adj_dict: bool,
    pub load_adj_dict_as_directed: bool,
    pub load_adj_dict_as_multigraph: bool,
    pub load_coo: bool,
}
