from typing import Any, Tuple

import numpy as np
import numpy.typing as npt

from phenolrs import PhenolError, graph_to_networkx_format


class NetworkXLoader:
    @staticmethod
    def load_into_networkx(
        database: str,
        metagraph: dict[str, Any],
        hosts: list[str],
        user_jwt: str | None = None,
        username: str | None = None,
        password: str | None = None,
        tls_cert: Any | None = None,
        parallelism: int = 10,
        batch_size: int = 1000000,
        load_node_dict: bool = True,
        load_adj_dict: bool = True,
        load_adj_dict_as_directed: bool = True,
        load_adj_dict_as_multigraph: bool = True,
        load_coo: bool = True,
    ) -> Tuple[
        dict[str, dict[str, Any]],
        dict[str, dict[str, dict[str, Any] | dict[int, dict[str, Any]]]],
        npt.NDArray[np.int64],
        npt.NDArray[np.int64],
        dict[str, int],
    ]:

        if "vertexCollections" not in metagraph:
            raise PhenolError("vertexCollections not found in metagraph")

        if "edgeCollections" not in metagraph:
            raise PhenolError("edgeCollections not found in metagraph")

        if len(metagraph["vertexCollections"]) == 0:
            raise PhenolError(
                "vertexCollections must map to non-empty dictionary"
            )  # noqa

        if len(metagraph["edgeCollections"]) == 0:
            raise PhenolError(
                "edgeCollections must map to non-empty dictionary"
            )  # noqa

        if not load_node_dict and not load_adj_dict and not load_coo:
            raise PhenolError(
                "At least one of load_node_dict, load_adj_dict, or load_coo must be True"  # noqa
            )

        db_config_options: dict[str, Any] = {
            "endpoints": hosts,
        }

        if username:
            db_config_options["username"] = username
        if password:
            db_config_options["password"] = password
        if user_jwt:
            db_config_options["jwt_token"] = user_jwt
        if tls_cert:
            db_config_options["tls_cert"] = tls_cert

        load_config = {
            "parallelism": parallelism,
            "batch_size": batch_size,
            "load_vertices": load_node_dict,
            "load_edges": load_adj_dict or load_coo,
            "load_all_attributes_via_aql": True,
        }

        graph_config = {
            "load_node_dict": load_node_dict,
            "load_adj_dict": load_adj_dict,
            "load_adj_dict_as_directed": load_adj_dict_as_directed,
            "load_adj_dict_as_multigraph": load_adj_dict_as_multigraph,
            "load_coo": load_coo,
        }

        vertex_collections = [
            {"name": v_col_name, "fields": []}
            for v_col_name, _ in metagraph["vertexCollections"].items()
        ]

        edge_collections = [
            {"name": e_col_name, "fields": []}
            for e_col_name, _ in metagraph["edgeCollections"].items()
        ]

        node_dict, adj_dict, src_indices, dst_indices, id_to_index_map = (
            graph_to_networkx_format(
                request={
                    "database": database,
                    "vertex_collections": vertex_collections,
                    "edge_collections": edge_collections,
                    "configuration": {
                        "database_config": db_config_options,
                        "load_config": load_config,
                    },
                },
                graph_config=graph_config,  # TODO Anthony: Move into request
            )
        )

        return node_dict, adj_dict, src_indices, dst_indices, id_to_index_map
