import typing

import numpy as np

from phenolrs import PhenolError, graph_to_json_format


class GraphLoader:
    @staticmethod
    def load(
        database: str,
        metagraph: dict[str, typing.Any],
        hosts: list[str],
        user_jwt: str | None = None,
        username: str | None = None,
        password: str | None = None,
        tls_cert: typing.Any | None = None,
        parallelism: int = 5,
        batch_size: int = 400000,
        load_node_dict: bool = True,
        load_adj_dict: bool = True,
        load_adj_dict_as_undirected: bool = False,
        load_coo: bool = True,
    ) -> typing.Tuple[
        dict[str, dict[str, typing.Any]],
        dict[str, dict[str, dict[str, typing.Any]]],
        np.ndarray,
        np.ndarray,
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

        # if not load_node_dict and not load_adj_dict and not load_coo:
        #     raise PhenolError(
        #         "At least one of load_node_dict, load_adj_dict, or load_coo must be True"  # noqa
        #     )

        db_config_options: dict[str, typing.Any] = {
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

        dump_config = {
            "parallelism": parallelism,
            "batch_size": batch_size,
        }

        load_config = {
            "load_node_dict": load_node_dict,
            "load_adj_dict": load_adj_dict,
            "load_adj_dict_as_undirected": load_adj_dict_as_undirected,
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

        return graph_to_json_format(
            {
                "database": database,
                "vertex_collections": vertex_collections,
                "edge_collections": edge_collections,
                "configuration": {
                    "database_config": db_config_options,
                    "dump_config": dump_config,
                    "load_config": load_config,
                },
            }
        )
