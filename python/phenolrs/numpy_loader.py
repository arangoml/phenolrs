import typing

import numpy as np
import numpy.typing as npt

from phenolrs import PhenolError, graph_to_pyg_format


class NumpyLoader:
    @staticmethod
    def load_graph_to_numpy(
        database: str,
        metagraph: dict[str, typing.Any],
        hosts: list[str],
        user_jwt: str | None = None,
        username: str | None = None,
        password: str | None = None,
        tls_cert: typing.Any | None = None,
        parallelism: int | None = None,
        batch_size: int | None = None,
    ) -> typing.Tuple[
        dict[str, dict[str, npt.NDArray[np.float64]]],
        dict[typing.Tuple[str, str, str], npt.NDArray[np.float64]],
        dict[str, dict[str, int]],
        dict[str, dict[str, str]],
    ]:
        # TODO: replace with pydantic validation
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

        config: dict[str, typing.Any] = {"database_config": db_config_options}
        if parallelism:
            config["parallelism"] = parallelism
        if batch_size:
            config["batch_size"] = batch_size

        if "vertexCollections" not in metagraph:
            raise PhenolError("vertexCollections not found in metagraph")

        vertex_collections = [
            {"name": v_col_name, "fields": list(entries.values())}
            for v_col_name, entries in metagraph["vertexCollections"].items()
        ]
        vertex_cols_source_to_output = {
            v_col_name: {
                source_name: output_name for output_name, source_name in entries.items()
            }
            for v_col_name, entries in metagraph["vertexCollections"].items()
        }

        edge_collections = []
        if "edgeCollections" in metagraph:
            edge_collections = [
                {"name": e_col_name, "fields": list(entries.values())}
                for e_col_name, entries in metagraph["edgeCollections"].items()
            ]

        features_by_col, coo_map, col_to_key_inds = graph_to_pyg_format(
            {
                "database": database,
                "vertex_collections": vertex_collections,
                "edge_collections": edge_collections,
                "configuration": {"database_config": db_config_options},
            }
        )

        return features_by_col, coo_map, col_to_key_inds, vertex_cols_source_to_output
