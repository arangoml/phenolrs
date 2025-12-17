"""AQL-based graph loading for retrieving subgraphs from ArangoDB.

This module provides utilities for loading graphs from ArangoDB using
custom AQL queries, following the design specification for flexible
graph export via AQL.

The key benefit of AQL-based loading is flexibility:
- Use indexes or traversals to find the right subgraph
- Filter vertices and edges with arbitrary AQL conditions
- Support for graph traversals to extract connected subgraphs
- Control over execution order (sequential groups, parallel queries)
"""

from typing import Any, Dict, List, Optional

from phenolrs import (
    PhenolError,
    graph_aql_to_networkx_format,
    graph_aql_to_numpy_format,
)

from .typings import AqlQuery, AttributeSpec, DatabaseConfig


class AqlLoader:
    """Loader for AQL-based graph extraction from ArangoDB.

    This loader allows flexible graph extraction using custom AQL queries.
    Queries are organized into groups:
    - Outer list: Groups processed sequentially
    - Inner list: Queries within a group processed in parallel

    Example usage:
    ```python
    from phenolrs.aql import AqlLoader

    # Load a subgraph using filtered collections
    loader = AqlLoader(
        hosts=["http://localhost:8529"],
        database="mydb",
        username="root",
        password="password"
    )

    # Define the queries
    queries = [
        # First group: load vertices
        [
            {"query": "FOR v IN users FILTER v.active RETURN {vertices: [v]}"},
            {"query": "FOR v IN products FILTER v.inStock RETURN {vertices: [v]}"}
        ],
        # Second group: load edges
        [
            {"query": "FOR e IN purchases RETURN {edges: [e]}"}
        ]
    ]

    # Load into numpy format
    result = loader.load_to_numpy(
        queries=queries,
        vertex_attributes={"name": "string", "age": "i64"},
        edge_attributes={"amount": "f64"}
    )
    ```
    """

    def __init__(
        self,
        hosts: List[str],
        database: str = "_system",
        username: Optional[str] = None,
        password: Optional[str] = None,
        user_jwt: Optional[str] = None,
        tls_cert: Optional[str] = None,
        batch_size: int = 10000,
    ):
        """Initialize the AQL loader.

        Args:
            hosts: List of ArangoDB endpoint URLs (e.g., ["http://localhost:8529"])
            database: Database name (default: "_system")
            username: Username for authentication
            password: Password for authentication
            user_jwt: JWT token for authentication (alternative to username/password)
            tls_cert: TLS certificate for secure connections
            batch_size: Number of items per batch (default: 10000)
        """
        self.hosts = hosts
        self.database = database
        self.username = username
        self.password = password
        self.user_jwt = user_jwt
        self.tls_cert = tls_cert
        self.batch_size = batch_size

    def _build_request(
        self,
        queries: List[List[AqlQuery]],
        vertex_attributes: Optional[AttributeSpec] = None,
        edge_attributes: Optional[AttributeSpec] = None,
    ) -> Dict[str, Any]:
        """Build the request object for the Rust backend."""
        db_config: DatabaseConfig = {
            "endpoints": self.hosts,
            "database": self.database,
            "username": self.username or "",
            "password": self.password or "",
            "jwt_token": self.user_jwt or "",
        }
        if self.tls_cert:
            db_config["tls_cert"] = self.tls_cert

        request: Dict[str, Any] = {
            "database_config": db_config,
            "batch_size": self.batch_size,
            "queries": queries,
        }

        if vertex_attributes is not None:
            request["vertex_attributes"] = vertex_attributes
        if edge_attributes is not None:
            request["edge_attributes"] = edge_attributes

        return request

    def load_to_numpy(
        self,
        queries: List[List[AqlQuery]],
        vertex_attributes: Optional[AttributeSpec] = None,
        edge_attributes: Optional[AttributeSpec] = None,
    ) -> Any:
        """Load a graph using AQL queries into numpy-compatible format.

        Args:
            queries: List of query groups. Outer list is sequential,
                inner lists are parallel.
                Each query should return {"vertices": [...], "edges": [...]}.
            vertex_attributes: Schema for vertex attributes. Either a dict
                mapping attribute names to types
                (e.g., {"name": "string", "age": "i64"})
                or a list of {"name": str, "type": str} objects.
            edge_attributes: Schema for edge attributes
                (same format as vertex_attributes).

        Returns:
            Tuple of (features_by_col, coo_map, col_to_key_to_ind,
            col_to_ind_to_key)
        """
        if not queries or all(len(group) == 0 for group in queries):
            raise PhenolError("At least one AQL query must be provided")

        request = self._build_request(queries, vertex_attributes, edge_attributes)
        return graph_aql_to_numpy_format(request)  # type: ignore[arg-type]

    def load_to_networkx(
        self,
        queries: List[List[AqlQuery]],
        vertex_attributes: Optional[AttributeSpec] = None,
        edge_attributes: Optional[AttributeSpec] = None,
        load_adj_dict: bool = True,
        load_coo: bool = True,
        is_directed: bool = True,
        is_multigraph: bool = True,
        symmetrize_edges_if_directed: bool = False,
    ) -> Any:
        """Load a graph using AQL queries into NetworkX-compatible format.

        Args:
            queries: List of query groups. Outer list is sequential,
                inner lists are parallel.
                Each query should return {"vertices": [...], "edges": [...]}.
            vertex_attributes: Schema for vertex attributes.
            edge_attributes: Schema for edge attributes.
            load_adj_dict: Whether to load adjacency dictionary (default: True)
            load_coo: Whether to load COO format (default: True)
            is_directed: Whether the graph is directed (default: True)
            is_multigraph: Whether to allow multiple edges (default: True)
            symmetrize_edges_if_directed: Add reverse edges (default: False)

        Returns:
            A tuple of (node_dict, adj_dict, src_indices, dst_indices,
            edge_indices, vertex_id_to_index, edge_values)
        """
        if not queries or all(len(group) == 0 for group in queries):
            raise PhenolError("At least one AQL query must be provided")

        request = self._build_request(queries, vertex_attributes, edge_attributes)

        graph_config = {
            "load_adj_dict": load_adj_dict,
            "load_coo": load_coo,
            "is_directed": is_directed,
            "is_multigraph": is_multigraph,
            "symmetrize_edges_if_directed": symmetrize_edges_if_directed,
        }

        return graph_aql_to_networkx_format(
            request, graph_config  # type: ignore[arg-type]
        )

    @staticmethod
    def create_vertex_query(
        collection: str,
        filter_condition: Optional[str] = None,
        projection: Optional[List[str]] = None,
        bind_vars: Optional[Dict[str, Any]] = None,
    ) -> AqlQuery:
        """Helper to create a vertex loading query.

        Args:
            collection: The vertex collection name
            filter_condition: Optional AQL filter condition
                (without FILTER keyword)
            projection: Optional list of fields to project.
                If None, returns full document.
            bind_vars: Optional bind variables

        Returns:
            An AqlQuery object ready to use

        Example:
            >>> AqlLoader.create_vertex_query(
            ...     "users", "doc.active == true", ["name", "age"]
            ... )
        """
        query_parts = [f"FOR doc IN {collection}"]

        if filter_condition:
            query_parts.append(f"FILTER {filter_condition}")

        if projection:
            # Build projection with _id always included
            fields = ["_id: doc._id"]
            fields.extend([f"{f}: doc.{f}" for f in projection])
            return_expr = "{" + ", ".join(fields) + "}"
            query_parts.append(f"RETURN {{vertices: [{return_expr}]}}")
        else:
            query_parts.append("RETURN {vertices: [doc]}")

        return {
            "query": " ".join(query_parts),
            "bindVars": bind_vars or {},
        }

    @staticmethod
    def create_edge_query(
        collection: str,
        filter_condition: Optional[str] = None,
        projection: Optional[List[str]] = None,
        bind_vars: Optional[Dict[str, Any]] = None,
    ) -> AqlQuery:
        """Helper to create an edge loading query.

        Args:
            collection: The edge collection name
            filter_condition: Optional AQL filter condition
                (without FILTER keyword)
            projection: Optional list of fields to project.
                If None, returns full document.
            bind_vars: Optional bind variables

        Returns:
            An AqlQuery object ready to use
        """
        query_parts = [f"FOR doc IN {collection}"]

        if filter_condition:
            query_parts.append(f"FILTER {filter_condition}")

        if projection:
            # Build projection with _from and _to always included
            fields = ["_from: doc._from", "_to: doc._to"]
            fields.extend(
                [f"{f}: doc.{f}" for f in projection if f not in ("_from", "_to")]
            )
            return_expr = "{" + ", ".join(fields) + "}"
            query_parts.append(f"RETURN {{edges: [{return_expr}]}}")
        else:
            query_parts.append("RETURN {edges: [doc]}")

        return {
            "query": " ".join(query_parts),
            "bindVars": bind_vars or {},
        }

    @staticmethod
    def create_traversal_query(
        start_vertex: str,
        graph_name: str,
        min_depth: int = 1,
        max_depth: int = 1,
        direction: str = "OUTBOUND",
        prune_condition: Optional[str] = None,
        filter_condition: Optional[str] = None,
        bind_vars: Optional[Dict[str, Any]] = None,
    ) -> AqlQuery:
        """Helper to create a graph traversal query.

        Args:
            start_vertex: The starting vertex ID (e.g., "collection/key")
                or bind variable
            graph_name: The named graph to traverse
            min_depth: Minimum traversal depth (default: 1)
            max_depth: Maximum traversal depth (default: 1)
            direction: Traversal direction - "OUTBOUND", "INBOUND", or "ANY"
                (default: "OUTBOUND")
            prune_condition: Optional PRUNE condition
            filter_condition: Optional FILTER condition
            bind_vars: Optional bind variables

        Returns:
            An AqlQuery object ready to use

        Example:
            >>> AqlLoader.create_traversal_query(
            ...     "@start", "myGraph", 0, 3, bind_vars={"start": "users/1"}
            ... )
        """
        # Use 0..max_depth to include the start vertex
        query_parts = [
            f"FOR v, e IN {min_depth}..{max_depth} {direction} "
            f"{start_vertex} GRAPH '{graph_name}'"
        ]

        if prune_condition:
            query_parts.append(f"PRUNE {prune_condition}")

        if filter_condition:
            query_parts.append(f"FILTER {filter_condition}")

        query_parts.append("RETURN {vertices: [v], edges: [e]}")

        return {
            "query": " ".join(query_parts),
            "bindVars": bind_vars or {},
        }
