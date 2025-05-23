from typing import Any, Set, Tuple

from phenolrs import PhenolError, graph_to_networkx_format

from .typings import (
    ArangoIDtoIndex,
    DiGraphAdjDict,
    DstIndices,
    EdgeIndices,
    EdgeValuesDict,
    GraphAdjDict,
    MultiDiGraphAdjDict,
    MultiGraphAdjDict,
    NodeDict,
    SrcIndices,
)


class NetworkXLoader:
    @staticmethod
    def load_into_networkx(
        database: str,
        metagraph: dict[str, dict[str, Set[str]]],
        hosts: list[str],
        user_jwt: str | None = None,
        username: str | None = None,
        password: str | None = None,
        tls_cert: Any | None = None,
        parallelism: int | None = None,
        batch_size: int | None = None,
        load_adj_dict: bool = True,
        load_coo: bool = True,
        load_all_vertex_attributes: bool = True,
        load_all_edge_attributes: bool = True,
        is_directed: bool = True,
        is_multigraph: bool = True,
        symmetrize_edges_if_directed: bool = False,
    ) -> Tuple[
        NodeDict,
        GraphAdjDict | DiGraphAdjDict | MultiGraphAdjDict | MultiDiGraphAdjDict,
        SrcIndices,
        DstIndices,
        EdgeIndices,
        ArangoIDtoIndex,
        EdgeValuesDict,
    ]:
        if "vertexCollections" not in metagraph:
            raise PhenolError("vertexCollections not found in metagraph")

        if "edgeCollections" not in metagraph:
            raise PhenolError("edgeCollections not found in metagraph")

        if len(metagraph["vertexCollections"]) + len(metagraph["edgeCollections"]) == 0:
            m = "vertexCollections and edgeCollections cannot both be empty"
            raise PhenolError(m)

        if len(metagraph["edgeCollections"]) == 0 and (load_adj_dict or load_coo):
            m = "edgeCollections must be non-empty if **load_adj_dict** or **load_coo** is True"  # noqa
            raise PhenolError(m)

        if load_all_vertex_attributes:
            for entries in metagraph["vertexCollections"].values():
                if len(entries) > 0:
                    m = f"load_all_vertex_attributes is True, but a vertexCollections entry contains attributes: {entries}"  # noqa
                    raise PhenolError(m)

        if load_all_edge_attributes:
            for entries in metagraph["edgeCollections"].values():
                if len(entries) > 0:
                    m = f"load_all_edge_attributes is True, but an edgeCollections entry contains attributes: {entries}"  # noqa
                    raise PhenolError(m)

        if len(metagraph["edgeCollections"]) != 0 and not (load_coo or load_adj_dict):
            m = "load_coo and load_adj_dict cannot both be False if edgeCollections is non-empty"  # noqa
            raise PhenolError(m)

        # TODO: replace with pydantic validation
        db_config_options: dict[str, Any] = {
            "endpoints": hosts,
            "database": database,
        }

        load_config_options: dict[str, Any] = {
            "parallelism": parallelism if parallelism is not None else 8,
            "batch_size": batch_size if batch_size is not None else 100000,
            "prefetch_count": 5,
            "load_all_vertex_attributes": load_all_vertex_attributes,
            "load_all_edge_attributes": load_all_edge_attributes,
        }

        if username:
            db_config_options["username"] = username
        if password:
            db_config_options["password"] = password
        if user_jwt:
            db_config_options["jwt_token"] = user_jwt
        if tls_cert:
            db_config_options["tls_cert"] = tls_cert

        graph_config = {
            "load_adj_dict": load_adj_dict,
            "load_coo": load_coo,
            "is_directed": is_directed,
            "is_multigraph": is_multigraph,
            "symmetrize_edges_if_directed": symmetrize_edges_if_directed,
        }

        vertex_collections = [
            {"name": v_col_name, "fields": list(entries)}
            for v_col_name, entries in metagraph["vertexCollections"].items()
        ]

        edge_collections = [
            {"name": e_col_name, "fields": list(entries)}
            for e_col_name, entries in metagraph["edgeCollections"].items()
        ]

        (
            node_dict,
            adj_dict,
            src_indices,
            dst_indices,
            edge_indices,
            id_to_index_map,
            edge_values,
        ) = graph_to_networkx_format(
            request={
                "vertex_collections": vertex_collections,
                "edge_collections": edge_collections,
                "database_config": db_config_options,
                "load_config": load_config_options,
            },
            graph_config=graph_config,  # TODO Anthony: Move into request
        )

        return (
            node_dict,
            adj_dict,
            src_indices,
            dst_indices,
            edge_indices,
            id_to_index_map,
            edge_values,
        )
