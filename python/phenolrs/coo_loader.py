import typing

import numpy as np
import numpy.typing as npt

from phenolrs import PhenolError, graph_to_coo_format


class CooLoader:
    def load_coo(
        database: str,
        metagraph: dict[str, typing.Any],
        hosts: list[str],
        user_jwt: str | None = None,
        username: str | None = None,
        password: str | None = None,
        tls_cert: typing.Any | None = None,
        parallelism: int | None = None,
        batch_size: int | None = None,
    ) -> dict[typing.Tuple[str, str, str], npt.NDArray[np.float64]]:

        if "vertexCollections" not in metagraph:
            raise PhenolError("vertexCollections not found in metagraph")

        if "edgeCollections" not in metagraph:
            raise PhenolError("edgeCollections not found in metagraph")

        if len(metagraph["vertexCollections"]) == 0:
            raise PhenolError("vertexCollections must map to non-empty dictionary")

        if len(metagraph["edgeCollections"]) == 0:
            raise PhenolError("edgeCollections must map to non-empty dictionary")

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

        vertex_collections = [
            {"name": v_col_name, "fields": []}
            for v_col_name, _ in metagraph["vertexCollections"].items()
        ]

        edge_collections = [
            {"name": e_col_name, "fields": []}
            for e_col_name, _ in metagraph["edgeCollections"].items()
        ]

        return graph_to_coo_format(
            {
                "database": database,
                "vertex_collections": vertex_collections,
                "edge_collections": edge_collections,
                "configuration": {"database_config": db_config_options},
            }
        )
