from typing import Any, Tuple

from phenolrs import PhenolError, graph_to_numpy_format

from .typings import (
    ArangoCollectionSourceToOutput,
    ArangoCollectionToArangoKeyToIndex,
    ArangoCollectionToIndexToArangoKey,
    ArangoCollectionToNodeFeatures,
    COOByEdgeType,
)


class NumpyLoader:
    @staticmethod
    def load_graph_to_numpy(
        database: str,
        metagraph: dict[str, Any],
        hosts: list[str],
        user_jwt: str | None = None,
        username: str | None = None,
        password: str | None = None,
        tls_cert: Any | None = None,
        parallelism: int | None = None,
        batch_size: int | None = None,
    ) -> Tuple[
        ArangoCollectionToNodeFeatures,
        COOByEdgeType,
        ArangoCollectionToArangoKeyToIndex,
        ArangoCollectionToIndexToArangoKey,
        ArangoCollectionSourceToOutput,
    ]:
        # TODO: replace with pydantic validation
        db_config_options: dict[str, Any] = {
            "endpoints": hosts,
            "database": database,
        }
        load_config_options: dict[str, Any] = {
            "parallelism": parallelism if parallelism is not None else 8,
            "batch_size": batch_size if batch_size is not None else 100000,
            "prefetch_count": 5,
            "load_all_vertex_attributes": False,
            "load_all_edge_attributes": False,
        }
        if username:
            db_config_options["username"] = username
        if password:
            db_config_options["password"] = password
        if user_jwt:
            db_config_options["jwt_token"] = user_jwt
        if tls_cert:
            db_config_options["tls_cert"] = tls_cert

        if "vertexCollections" not in metagraph:
            raise PhenolError("vertexCollections not found in metagraph")

        # Address the possibility of having something like this:
        # "USER": {"x": {"features": None}}
        # Should be converted to:
        # "USER": {"x": "features"}
        entries: dict[str, Any]
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

        (
            features_by_col,
            coo_map,
            col_to_adb_key_to_ind,
            col_to_ind_to_adb_key,
        ) = graph_to_numpy_format(
            {
                "vertex_collections": vertex_collections,
                "edge_collections": edge_collections,
                "database_config": db_config_options,
                "load_config": load_config_options,
            }
        )

        return (
            features_by_col,
            coo_map,
            col_to_adb_key_to_ind,
            col_to_ind_to_adb_key,
            vertex_cols_source_to_output,
        )
