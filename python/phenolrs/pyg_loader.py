import typing

import numpy as np
import torch
from torch_geometric.data import Data, HeteroData  # type: ignore[import]

from phenolrs import graph_to_pyg_format


class PygLoader:
    def __init__(self):
        pass

    @staticmethod
    def load_into_pyg_data() -> Data:
        pass

    @staticmethod
    def load_into_pyg_heterodata(
        database: str,
        vertex_collections: list[dict[str, typing.Any]],
        edge_collections: list[dict[str, typing.Any]],
        hosts: list[str],
        user_jwt: str | None = None,
        username: str | None = None,
        password: str | None = None,
        tls_cert: typing.Any | None = None,
        parallelism: int | None = None,
        batch_size: int | None = None,
    ) -> HeteroData:
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
        features_by_col, coo_map, col_to_key_inds = graph_to_pyg_format(
            {
                "database": database,
                "vertex_collections": vertex_collections,
                "edge_collections": edge_collections,
                "configuration": {"database_config": db_config_options},
            }
        )

        data = HeteroData()
        for col in features_by_col.keys():
            for feature in features_by_col[col].keys():
                data[col][feature] = torch.from_numpy(
                    features_by_col[col][feature].astype(np.float64)
                )

        for edge_col in coo_map.keys():
            data[edge_col].edge_index = torch.from_numpy(
                coo_map[edge_col].astype(np.int64)
            )

        return data
