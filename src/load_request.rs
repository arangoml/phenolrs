pub struct CollectionDescription {
    pub name: String,
    pub fields: Vec<String>,
}

pub struct GraphAnalyticsEngineDataLoadRequest {
    pub database: String,
    // The following map maps collection names as found in the
    // _id entries of vertices to the collections into which
    // the result data should be written. The list of fields
    // is the attributes into which the result is written.
    // An insert operation with overwritemode "update" is used.
    pub vertex_collections: Vec<CollectionDescription>,
    pub edge_collections: Vec<CollectionDescription>,
    pub parallelism: Option<u32>,
    // Optional batch size
    pub batch_size: Option<u64>,
}
