use arangors_graph_exporter::{
    AqlQuery, CollectionInfo, DataItem, DataLoadConfiguration, DataType, DatabaseConfiguration,
};
use std::collections::HashMap;

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

/// Request for AQL-based graph loading.
///
/// Queries are organized as a list of lists. The outer list is processed sequentially,
/// while inner lists are processed in parallel. Each query should return items of
/// the form `{"vertices": [...], "edges": [...]}`.
pub struct AqlDataLoadRequest {
    /// Database connection configuration (endpoints, credentials, TLS settings).
    pub db_config: DatabaseConfiguration,

    /// Number of documents to fetch per batch from ArangoDB.
    pub batch_size: u64,

    /// Schema definition for vertex attributes.
    ///
    /// Each [`DataItem`] specifies an attribute name and its expected type.
    /// Only attributes listed here will be extracted from vertex documents.
    pub vertex_attributes: Vec<DataItem>,

    /// Schema definition for edge attributes.
    ///
    /// Each [`DataItem`] specifies an attribute name and its expected type.
    /// Only attributes listed here will be extracted from edge documents.
    pub edge_attributes: Vec<DataItem>,

    /// AQL queries organized as sequential groups of parallel queries.
    ///
    /// - Outer `Vec`: Groups processed sequentially (one after another)
    /// - Inner `Vec`: Queries within a group processed in parallel
    pub queries: Vec<Vec<AqlQuery>>,

    /// Maximum number of type errors to report per batch before stopping.
    ///
    /// When parsing document attributes, type mismatches are collected and reported.
    /// This limit applies per batch, not overall. Set to `None` to use the library default.
    pub max_type_errors: Option<u64>,
}

/// Helper to convert string type names to DataType enum
pub fn parse_data_type(type_str: &str) -> Option<DataType> {
    match type_str.to_lowercase().as_str() {
        "bool" | "boolean" => Some(DataType::Bool),
        "string" | "str" => Some(DataType::String),
        "u64" | "uint64" | "unsigned" => Some(DataType::U64),
        "i64" | "int64" | "int" | "integer" => Some(DataType::I64),
        "f64" | "float64" | "float" | "double" | "number" => Some(DataType::F64),
        "json" | "object" | "any" => Some(DataType::JSON),
        _ => None,
    }
}

/// Helper to create DataItem from name and type string
pub fn create_data_item(name: String, type_str: &str) -> Result<DataItem, String> {
    parse_data_type(type_str)
        .map(|dt| DataItem::new(name.clone(), dt))
        .ok_or_else(|| format!("Unknown data type '{}' for attribute '{}'", type_str, name))
}

/// Helper to create AqlQuery from query string and bind vars
pub fn create_aql_query(query: String, bind_vars: HashMap<String, serde_json::Value>) -> AqlQuery {
    AqlQuery::new(query, bind_vars)
}
