use arangors_graph_exporter::{CollectionInfo, DataLoadConfiguration, DatabaseConfiguration};

pub struct DataLoadRequest {
    pub vertex_collections: Vec<CollectionInfo>,
    pub edge_collections: Vec<CollectionInfo>,
    pub db_config: DatabaseConfiguration,
    pub load_config: DataLoadConfiguration,
}

pub struct NetworkXGraphConfig {
    pub load_adj_dict: bool,
    pub load_coo: bool,
    pub is_directed: bool,
    pub is_multigraph: bool,
    pub symmetrize_edges_if_directed: bool,
}
