"""Type definitions for AQL-based graph loading.

This module follows the design specification from:
https://github.com/arangodb/documents/blob/master/DesignDocuments/02_PLANNING/GetGraphsOutOfArangoDBviaAQL.md
"""

from typing import Any, Dict, List, Literal, TypedDict, Union

# Supported data types for attributes
DataType = Literal[
    "bool", "string", "u64", "i64", "f64", "json", "number", "int", "float"
]


class AqlQuery(TypedDict, total=False):
    """An AQL query with optional bind variables.

    Each query should return items of the form:
    {"vertices": [...], "edges": [...]}

    Both vertices and edges are optional in the return value.
    Vertices must have at least an _id attribute.
    Edges must have at least _from and _to attributes.
    """

    query: str  # Required: The AQL query string
    bindVars: Dict[str, Any]  # Optional: Bind variables for the query
    bind_vars: Dict[str, Any]  # Alternative name for bindVars


class AttributeItem(TypedDict):
    """A single attribute definition with name and type."""

    name: str
    type: DataType


# Attribute specification can be either:
# - A dict mapping attribute names to types: {"name": "string", "age": "i64"}
# - A list of attribute items: [{"name": "name", "type": "string"}, ...]
AttributeSpec = Union[Dict[str, DataType], List[AttributeItem]]


class DatabaseConfig(TypedDict, total=False):
    """Database connection configuration."""

    endpoints: List[str]  # List of ArangoDB endpoints
    database: str  # Database name (default: "_system")
    username: str  # Username for authentication
    password: str  # Password for authentication
    jwt_token: str  # JWT token for authentication (alternative to username/password)
    tls_cert: str  # TLS certificate for secure connections


class AqlDataLoadRequest(TypedDict, total=False):
    """Request for AQL-based graph loading.

    The queries field is a list of lists of AQL queries:
    - The outer list is processed **sequentially**
    - Each inner list of queries can be executed **in parallel**

    This structure allows for:
    1. Loading vertices first, then edges (for efficient memory usage)
    2. Parallel loading of multiple vertex/edge collections
    3. Sequential execution of dependent operations

    Example for use case 1 (filtered collections)::

        {
            "queries": [
                # First group: load all vertices in parallel
                [
                    {"query": "FOR x IN v1 RETURN {vertices: [x]}"},
                    {"query": "FOR x IN v2 RETURN {vertices: [x]}"}
                ],
                # Second group: load all edges in parallel
                [
                    {"query": "FOR e IN edges1 RETURN {edges: [e]}"},
                ]
            ]
        }

    Example for use case 2 (graph traversals)::

        {
            "queries": [[{
                "query": "FOR v, e IN 0..10 OUTBOUND @s GRAPH 'g' "
                         "RETURN {vertices: [v], edges: [e]}",
                "bindVars": {"s": "vertex/1"}
            }]]
        }
    """

    database_config: DatabaseConfig
    batch_size: int  # Number of items per batch (default: 10000)
    vertex_attributes: AttributeSpec  # Schema for vertex attributes
    edge_attributes: AttributeSpec  # Schema for edge attributes
    queries: List[List[AqlQuery]]  # List of query groups
