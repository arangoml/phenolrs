import typing

import numpy as np
import numpy.typing as npt
from arango import ArangoClient
from arango.database import StandardDatabase

from phenolrs import PhenolError, graph_to_pyg_format


class NumpyLoader:
    @staticmethod
    def _get_highest_pyg_index(
        db: StandardDatabase, col_name: str, pyg_index_field: str
    ) -> int:
        aql = """
            FOR doc IN @@col
                SORT doc.@pyg_index_field DESC
                LIMIT 1
                RETURN doc.@pyg_index_field
        """
        bind_vars = {"@col": col_name, "pyg_index_field": pyg_index_field}
        cursor = db.aql.execute(aql, bind_vars=bind_vars)
        res = cursor.next()

        return res if res is not None else -1

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
        pyg_index_field: str = "_pyg_ind",
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

        db = ArangoClient(hosts=hosts, verify_override=tls_cert).db(
            database,
            username=username,
            password=password,
            user_token=user_jwt,
            verify=True,
        )

        # Address the possibility of having something like this:
        # "USER": {"x": {"features": None}}
        # Should be converted to:
        # "USER": {"x": "features"}
        entries: dict[str, typing.Any]
        for v_col_name, entries in metagraph["vertexCollections"].items():
            for source_name, value in entries.items():
                if isinstance(value, dict):
                    if len(value) != 1:
                        m = f"Only one feature field should be specified per attribute. Found {value}"  # noqa: E501
                        raise PhenolError(m)

                    value_key = list(value.keys())[0]
                    if value[value_key] is not None:
                        m = f"Invalid value for feature {source_name}: {value_key}. Found {value[value_key]}"  # noqa: E501
                        raise PhenolError(m)

                    metagraph["vertexCollections"][v_col_name][source_name] = value_key

        vertex_collections = []
        vertex_cols_source_to_output = {}
        for v_col_name, entries in metagraph["vertexCollections"].items():
            vertex_collections.append(
                {
                    "name": v_col_name,
                    "fields": list(set(entries.values()) | {pyg_index_field}),
                    "highest_pyg_index": NumpyLoader._get_highest_pyg_index(
                        db, v_col_name, pyg_index_field
                    ),
                }
            )
            vertex_cols_source_to_output[v_col_name] = {
                source_name: output_name for output_name, source_name in entries.items()
            }

        edge_collections = []
        if "edgeCollections" in metagraph:
            edge_collections = [
                {"name": e_col_name, "fields": list(entries.values())}
                for e_col_name, entries in metagraph["edgeCollections"].items()
            ]

        features_by_col, coo_map, col_to_adb_id_to_ind = graph_to_pyg_format(
            {
                "database": database,
                "vertex_collections": vertex_collections,
                "edge_collections": edge_collections,
                "configuration": {"database_config": db_config_options},
            }
        )

        return (
            features_by_col,
            coo_map,
            col_to_adb_id_to_ind,
            vertex_cols_source_to_output,
        )
