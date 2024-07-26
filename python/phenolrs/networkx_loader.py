from typing import Any, Set, Tuple

import numpy as np
import numpy.typing as npt

from phenolrs import PhenolError, graph_to_networkx_format

from .typings import DiGraph, Graph, MultiDiGraph, MultiGraph


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
        symmterize_edges_if_directed: bool = False,
    ) -> Tuple[
        dict[str, dict[str, Any]],
        Graph | DiGraph | MultiGraph | MultiDiGraph,
        npt.NDArray[np.int64],
        npt.NDArray[np.int64],
        dict[str, int],
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

        if load_all_vertex_attributes and any(
            [len(entries) > 0 for entries in metagraph["vertexCollections"].values()]
        ):
            m = "load_all_vertex_attributes is True, but vertexCollections contain attributes"  # noqa
            raise PhenolError(m)

        if load_all_edge_attributes and any(
            [len(entries) > 0 for entries in metagraph["edgeCollections"].values()]
        ):
            m = "load_all_edge_attributes is True, but edgeCollections contain attributes"  # noqa
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
            "symmterize_edges_if_directed": symmterize_edges_if_directed,
        }

        vertex_collections = [
            {"name": v_col_name, "fields": list(entries)}
            for v_col_name, entries in metagraph["vertexCollections"].items()
        ]

        edge_collections = [
            {"name": e_col_name, "fields": list(entries)}
            for e_col_name, entries in metagraph["edgeCollections"].items()
        ]

        node_dict, adj_dict, src_indices, dst_indices, id_to_index_map = (
            graph_to_networkx_format(
                request={
                    "vertex_collections": vertex_collections,
                    "edge_collections": edge_collections,
                    "database_config": db_config_options,
                    "load_config": load_config_options,
                },
                graph_config=graph_config,  # TODO Anthony: Move into request
            )
        )

        return node_dict, adj_dict, src_indices, dst_indices, id_to_index_map
