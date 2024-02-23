import typing

from torch_geometric.data import HeteroData, Data
import torch
import numpy as np

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
        user_jwt: str = None,
        username: str = None,
        password: str = None,
        tls_cert: typing.Any = None,
    ) -> HeteroData:
        config_options = {
            "endpoints": hosts,
        }
        if username:
            config_options["username"] = username
        if password:
            config_options["password"] = password
        if user_jwt:
            config_options["jwt_token"] = user_jwt
        if tls_cert:
            config_options["tls_cert"] = tls_cert
        features_by_col, coo_map, col_to_key_inds = graph_to_pyg_format(
            {
                "database": database,
                "vertex_collections": vertex_collections,
                "edge_collections": edge_collections,
                "configuration": {
                    "database_config": config_options
                }
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


if __name__ == "__main__":
    PygLoader.load_into_pyg_heterodata()
