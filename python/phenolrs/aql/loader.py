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

import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import numpy as np

from phenolrs import (
    PhenolError,
    graph_aql_to_networkx_format,
    graph_aql_to_numpy_format,
)

from .typings import AqlQuery, AttributeSpec, DatabaseConfig

if TYPE_CHECKING:
    from torch_geometric.data import Data, HeteroData

try:
    import torch
    from torch_geometric.data import Data, HeteroData  # noqa: F811

    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# Valid AQL identifier: alphanumeric, underscore, hyphen; starts with letter/_
_VALID_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_\-]*$")


def _validate_identifier(name: str, param_name: str) -> None:
    """Validate that a name is a safe AQL identifier."""
    if not name or not _VALID_IDENTIFIER.match(name):
        raise ValueError(
            f"Invalid {param_name}: '{name}'. Must be alphanumeric with "
            "underscores/hyphens, starting with a letter or underscore."
        )


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
        max_type_errors: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Build the request object for the Rust backend."""
        db_config: DatabaseConfig = {
            "endpoints": self.hosts,
            "database": self.database,
        }
        # Only include credentials when provided (not empty strings)
        if self.username:
            db_config["username"] = self.username
        if self.password:
            db_config["password"] = self.password
        if self.user_jwt:
            db_config["jwt_token"] = self.user_jwt
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
        if max_type_errors is not None:
            request["max_type_errors"] = max_type_errors

        return request

    def load_to_numpy(
        self,
        queries: List[List[AqlQuery]],
        vertex_attributes: Optional[AttributeSpec] = None,
        edge_attributes: Optional[AttributeSpec] = None,
        max_type_errors: Optional[int] = None,
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
            max_type_errors: Maximum number of type errors to report
                before stopping. None uses the library default.

        Returns:
            Tuple of (features_by_col, coo_map, col_to_key_to_ind,
            col_to_ind_to_key)
        """
        if not queries or not any(len(group) > 0 for group in queries):
            raise PhenolError("At least one AQL query must be provided")

        request = self._build_request(
            queries, vertex_attributes, edge_attributes, max_type_errors
        )
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
        max_type_errors: Optional[int] = None,
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
            max_type_errors: Maximum number of type errors to report
                before stopping. None uses the library default.

        Returns:
            A tuple of (node_dict, adj_dict, src_indices, dst_indices,
            edge_indices, vertex_id_to_index, edge_values)
        """
        if not queries or not any(len(group) > 0 for group in queries):
            raise PhenolError("At least one AQL query must be provided")

        request = self._build_request(
            queries, vertex_attributes, edge_attributes, max_type_errors
        )

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

    def load_to_pyg_data(
        self,
        queries: List[List[AqlQuery]],
        vertex_attributes: Optional[AttributeSpec] = None,
        edge_attributes: Optional[AttributeSpec] = None,
        pyg_feature_mapping: Optional[Dict[str, List[str]]] = None,
        max_type_errors: Optional[int] = None,
    ) -> Tuple["Data", Dict[str, Dict[str, int]], Dict[str, Dict[int, str]]]:
        """Load a graph using AQL queries into PyTorch Geometric Data format.

        This method loads a homogeneous graph (single node type, single edge type)
        into a PyG Data object suitable for GNN training.

        Note:
            Edges are required for PyG Data format. The returned Data object
            will always have an edge_index tensor. Ensure your queries return
            edge documents in the "edges" array. For vertex-only graphs,
            use :meth:`load_to_networkx` or :meth:`load_to_numpy` instead.

        Args:
            queries: List of query groups. Outer list is sequential,
                inner lists are parallel.
                Each query should return {"vertices": [...], "edges": [...]}.
            vertex_attributes: Schema for vertex attributes.
                Maps attribute names to types (e.g., {"features": "f64"}).
                Attributes must be numeric (f64, i64) for PyG compatibility.
            edge_attributes: Schema for edge attributes.
            pyg_feature_mapping: Optional mapping from PyG attribute names to
                loaded attribute names.
                Example: {"x": ["feat1", "feat2"], "y": ["label"]}
                If None, all numeric vertex attributes are stacked into 'x'.
            max_type_errors: Maximum number of type errors to report.

        Returns:
            A tuple of (Data, col_to_key_to_ind, col_to_ind_to_key)

        Raises:
            ImportError: If PyTorch/PyG dependencies are not installed.
            PhenolError: If no queries are provided, no vertex/edge data is loaded,
                multiple vertex collections or edge types are found (use
                :meth:`load_to_pyg_heterodata` for heterogeneous graphs), or if
                attributes have incompatible types (strings/objects).

        Example:
            >>> loader = AqlLoader(hosts=["http://localhost:8529"], database="mydb")
            >>> queries = [[{"query": "FOR v IN users RETURN {vertices: [v]}"}],
            ...            [{"query": "FOR e IN follows RETURN {edges: [e]}"}]]
            >>> data, key_to_ind, ind_to_key = loader.load_to_pyg_data(
            ...     queries=queries,
            ...     vertex_attributes={"features": "f64", "label": "i64"},
            ...     pyg_feature_mapping={"x": ["features"], "y": ["label"]}
            ... )
        """
        if not TORCH_AVAILABLE:
            raise ImportError(
                "Missing required dependencies. "
                "Install with `pip install phenolrs[torch]`"
            )

        if not queries or not any(len(group) > 0 for group in queries):
            raise PhenolError("At least one AQL query must be provided")

        # Load data as numpy first
        request = self._build_request(
            queries, vertex_attributes, edge_attributes, max_type_errors
        )
        (
            features_by_col,
            coo_map,
            col_to_adb_key_to_ind,
            col_to_ind_to_adb_key,
        ) = graph_aql_to_numpy_format(request)  # type: ignore[arg-type]  # fmt: skip

        # For homogeneous graph, we expect exactly one vertex collection
        vertex_cols = [c for c in features_by_col.keys() if c != "@collection_name"]
        if len(vertex_cols) == 0:
            raise PhenolError("No vertex data loaded from AQL queries")
        if len(vertex_cols) > 1:
            m = (
                f"Multiple vertex collections ({vertex_cols}) found. "
                "Use load_to_pyg_heterodata for heterogeneous graphs."
            )
            raise PhenolError(m)

        v_col_name = vertex_cols[0]
        v_features = features_by_col[v_col_name]

        data = Data()

        # Build feature mapping
        if pyg_feature_mapping is not None:
            # User specified mapping
            for pyg_name, attr_list in pyg_feature_mapping.items():
                tensors = []
                for attr_name in attr_list:
                    if attr_name not in v_features:
                        raise PhenolError(
                            f"Attribute '{attr_name}' not found in loaded data. "
                            f"Available: {list(v_features.keys())}"
                        )
                    arr = v_features[attr_name]
                    # Check if attribute is string type (not convertible to numeric)
                    if arr.dtype.kind in ("U", "S", "O"):
                        raise PhenolError(
                            f"Attribute '{attr_name}' has string/object type "
                            "which cannot be converted to PyG tensors. "
                            "PyG requires numeric types (i64, f64, bool)."
                        )
                    if arr.ndim == 1:
                        arr = arr.reshape(-1, 1)
                    tensors.append(torch.from_numpy(arr.astype(np.float64)))

                if tensors:
                    combined = torch.cat(tensors, dim=1)
                    if combined.numel() > 0:
                        data[pyg_name] = combined
        else:
            # Auto-mapping: stack all numeric attributes into 'x'
            tensors = []
            for attr_name, arr in v_features.items():
                if attr_name == "@collection_name":
                    continue
                # Check if attribute is string type (not convertible to numeric)
                if arr.dtype.kind in ("U", "S", "O"):
                    raise PhenolError(
                        f"Attribute '{attr_name}' has string/object type "
                        "which cannot be converted to PyG tensors. "
                        "PyG requires numeric types (i64, f64, bool). "
                        "Exclude string attributes or use pyg_feature_mapping."
                    )
                if arr.ndim == 1:
                    arr = arr.reshape(-1, 1)
                tensors.append(torch.from_numpy(arr.astype(np.float64)))

            if tensors:
                combined = torch.cat(tensors, dim=1)
                if combined.numel() > 0:
                    data.x = combined

        # Add edges - expect exactly one edge type for homogeneous graph
        if len(coo_map) == 0:
            raise PhenolError("No edge data loaded from AQL queries")
        if len(coo_map) > 1:
            m = (
                "Multiple edge types found. "
                "Use load_to_pyg_heterodata for heterogeneous graphs."
            )
            raise PhenolError(m)

        edge_key = list(coo_map.keys())[0]
        edge_index = torch.from_numpy(coo_map[edge_key].astype(np.int64))
        # Always assign edge_index, even if empty (use proper empty tensor shape)
        if edge_index.numel() > 0:
            data.edge_index = edge_index
        else:
            data.edge_index = torch.empty((2, 0), dtype=torch.long)

        return data, col_to_adb_key_to_ind, col_to_ind_to_adb_key

    def load_to_pyg_heterodata(
        self,
        queries: List[List[AqlQuery]],
        vertex_attributes: Optional[AttributeSpec] = None,
        edge_attributes: Optional[AttributeSpec] = None,
        pyg_feature_mapping: Optional[Dict[str, Dict[str, List[str]]]] = None,
        max_type_errors: Optional[int] = None,
    ) -> Tuple["HeteroData", Dict[str, Dict[str, int]], Dict[str, Dict[int, str]]]:
        """Load a graph using AQL queries into PyTorch Geometric HeteroData format.

        This method loads a heterogeneous graph (multiple node/edge types)
        into a PyG HeteroData object suitable for heterogeneous GNN training.

        Args:
            queries: List of query groups. Outer list is sequential,
                inner lists are parallel.
                Each query should return {"vertices": [...], "edges": [...]}.
            vertex_attributes: Schema for vertex attributes.
            edge_attributes: Schema for edge attributes.
            pyg_feature_mapping: Optional nested mapping from collection names to
                PyG attribute mappings. Example:
                {"Users": {"x": ["feat1"], "y": ["label"]},
                 "Products": {"x": ["features"]}}
                If None, all numeric attributes per collection are stacked into 'x'.
            max_type_errors: Maximum number of type errors to report.

        Returns:
            A tuple of (HeteroData, col_to_key_to_ind, col_to_ind_to_key)

        Example:
            >>> loader = AqlLoader(hosts=["http://localhost:8529"], database="mydb")
            >>> queries = [
            ...     [{"query": "FOR v IN users RETURN {vertices: [v]}"},
            ...      {"query": "FOR v IN products RETURN {vertices: [v]}"}],
            ...     [{"query": "FOR e IN purchases RETURN {edges: [e]}"}]
            ... ]
            >>> data, key_to_ind, ind_to_key = loader.load_to_pyg_heterodata(
            ...     queries=queries,
            ...     vertex_attributes={"features": "f64", "label": "i64"},
            ...     pyg_feature_mapping={
            ...         "users": {"x": ["features"], "y": ["label"]},
            ...         "products": {"x": ["features"]}
            ...     }
            ... )
        """
        if not TORCH_AVAILABLE:
            raise ImportError(
                "Missing required dependencies. "
                "Install with `pip install phenolrs[torch]`"
            )

        if not queries or not any(len(group) > 0 for group in queries):
            raise PhenolError("At least one AQL query must be provided")

        # Load data as numpy first
        request = self._build_request(
            queries, vertex_attributes, edge_attributes, max_type_errors
        )
        (
            features_by_col,
            coo_map,
            col_to_adb_key_to_ind,
            col_to_ind_to_adb_key,
        ) = graph_aql_to_numpy_format(request)  # type: ignore[arg-type]  # fmt: skip

        data = HeteroData()

        # Validate that collections referenced in pyg_feature_mapping exist
        # This catches cases where string attributes were requested (which are
        # silently dropped by the Rust backend, resulting in no vertex data)
        if pyg_feature_mapping is not None:
            for col_name in pyg_feature_mapping:
                if col_name not in features_by_col:
                    raise PhenolError(
                        f"No vertex data loaded for collection '{col_name}'. "
                        "This may occur if only string/object type attributes were "
                        "requested, which cannot be converted to PyG tensors."
                    )

        # Process vertex features per collection
        for col_name, col_features in features_by_col.items():
            if pyg_feature_mapping is not None and col_name in pyg_feature_mapping:
                # User specified mapping for this collection
                col_mapping = pyg_feature_mapping[col_name]
                for pyg_name, attr_list in col_mapping.items():
                    tensors = []
                    for attr_name in attr_list:
                        if attr_name not in col_features:
                            raise PhenolError(
                                f"Attribute '{attr_name}' not found in collection "
                                f"'{col_name}'. Available: {list(col_features.keys())}"
                            )
                        arr = col_features[attr_name]
                        # Check if attribute is string type (not convertible to numeric)
                        if arr.dtype.kind in ("U", "S", "O"):
                            raise PhenolError(
                                f"Attribute '{attr_name}' in '{col_name}' has "
                                "string/object type which cannot be converted "
                                "to PyG tensors. Requires numeric (i64, f64, bool)."
                            )
                        if arr.ndim == 1:
                            arr = arr.reshape(-1, 1)
                        tensors.append(torch.from_numpy(arr.astype(np.float64)))

                    if tensors:
                        combined = torch.cat(tensors, dim=1)
                        if combined.numel() > 0:
                            data[col_name][pyg_name] = combined
            else:
                # Auto-mapping: stack all numeric attributes into 'x'
                tensors = []
                for attr_name, arr in col_features.items():
                    if attr_name == "@collection_name":
                        continue
                    # Check if attribute is string type (not convertible to numeric)
                    if arr.dtype.kind in ("U", "S", "O"):
                        raise PhenolError(
                            f"Attribute '{attr_name}' in '{col_name}' has "
                            "string/object type which cannot be converted "
                            "to PyG tensors. Requires numeric (i64, f64, bool). "
                            "Exclude string attrs or use pyg_feature_mapping."
                        )
                    if arr.ndim == 1:
                        arr = arr.reshape(-1, 1)
                    tensors.append(torch.from_numpy(arr.astype(np.float64)))

                if tensors:
                    combined = torch.cat(tensors, dim=1)
                    if combined.numel() > 0:
                        data[col_name].x = combined

        # Add edges per edge type
        for edge_key, edge_coo in coo_map.items():
            edge_col_name, from_col, to_col = edge_key
            edge_index = torch.from_numpy(edge_coo.astype(np.int64))
            if edge_index.numel() > 0:
                data[(from_col, edge_col_name, to_col)].edge_index = edge_index

        return data, col_to_adb_key_to_ind, col_to_ind_to_adb_key

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
                (without FILTER keyword). Security note: Use bind_vars
                for any user-provided values to prevent AQL injection.
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
        _validate_identifier(collection, "collection")
        query_parts = [f"FOR doc IN `{collection}`"]

        if filter_condition:
            query_parts.append(f"FILTER {filter_condition}")

        if projection:
            # Build projection with _id always included
            # Validate field names to prevent injection
            for f in projection:
                _validate_identifier(f, "projection field")
            fields = ["_id: doc._id"]
            # Skip _id if already in projection to avoid duplicate keys
            fields.extend([f"`{f}`: doc.`{f}`" for f in projection if f != "_id"])
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
                (without FILTER keyword). Security note: Use bind_vars
                for any user-provided values to prevent AQL injection.
            projection: Optional list of fields to project.
                If None, returns full document.
            bind_vars: Optional bind variables

        Returns:
            An AqlQuery object ready to use
        """
        _validate_identifier(collection, "collection")
        query_parts = [f"FOR doc IN `{collection}`"]

        if filter_condition:
            query_parts.append(f"FILTER {filter_condition}")

        if projection:
            # Build projection with _from and _to always included
            # Validate field names to prevent injection
            for f in projection:
                if f not in ("_from", "_to"):
                    _validate_identifier(f, "projection field")
            fields = ["_from: doc._from", "_to: doc._to"]
            fields.extend(
                [f"`{f}`: doc.`{f}`" for f in projection if f not in ("_from", "_to")]
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
            start_vertex: The starting vertex. Must be either:
                - A bind variable reference (e.g., "@start")
                - A quoted literal (e.g., "'users/alice'")
                Use bind_vars for user-provided values to prevent injection.
            graph_name: The named graph to traverse
            min_depth: Minimum traversal depth (default: 1)
            max_depth: Maximum traversal depth (default: 1)
            direction: Traversal direction - "OUTBOUND", "INBOUND", or "ANY"
                (default: "OUTBOUND")
            prune_condition: Optional PRUNE condition. Security note: Use
                bind_vars for user-provided values.
            filter_condition: Optional FILTER condition. Security note: Use
                bind_vars for user-provided values.
            bind_vars: Optional bind variables

        Returns:
            An AqlQuery object ready to use

        Example:
            >>> AqlLoader.create_traversal_query(
            ...     "@start", "myGraph", 0, 3, bind_vars={"start": "users/1"}
            ... )
        """
        # Validate start_vertex format to prevent malformed queries
        if not (
            start_vertex.startswith("@")
            or (start_vertex.startswith("'") and start_vertex.endswith("'"))
        ):
            raise ValueError(
                "start_vertex must be a bind variable (@var) or quoted literal "
                f"('value'), got: {start_vertex}"
            )

        # Validate direction parameter
        valid_directions = ("OUTBOUND", "INBOUND", "ANY")
        if direction not in valid_directions:
            raise ValueError(
                f"direction must be one of {valid_directions}, got: '{direction}'"
            )

        # Validate depth parameters
        if min_depth < 0:
            raise ValueError(f"min_depth must be non-negative, got: {min_depth}")
        if max_depth < 0:
            raise ValueError(f"max_depth must be non-negative, got: {max_depth}")
        if max_depth < min_depth:
            raise ValueError(
                f"max_depth ({max_depth}) must be >= min_depth ({min_depth})"
            )

        # Use 0..max_depth to include the start vertex
        _validate_identifier(graph_name, "graph_name")
        query_parts = [
            f"FOR v, e IN {min_depth}..{max_depth} {direction} "
            f"{start_vertex} GRAPH `{graph_name}`"
        ]

        if prune_condition:
            query_parts.append(f"PRUNE {prune_condition}")

        if filter_condition:
            query_parts.append(f"FILTER {filter_condition}")

        # Handle null edges when min_depth=0 (start vertex has no edge)
        query_parts.append("RETURN {vertices: [v], edges: (e == null ? [] : [e])}")

        return {
            "query": " ".join(query_parts),
            "bindVars": bind_vars or {},
        }
