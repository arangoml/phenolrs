import typing

import numpy as np
import torch
from torch_geometric.data import Data, HeteroData

from phenolrs import PhenolError
from phenolrs.numpy_loader import NumpyLoader


class PygLoader:
    @staticmethod
    def load_into_pyg_data(
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
    ) -> tuple[Data, dict[str, dict[str, int]]]:
        if "vertexCollections" not in metagraph:
            raise PhenolError("vertexCollections not found in metagraph")
        if "edgeCollections" not in metagraph:
            raise PhenolError("edgeCollections not found in metagraph")

        if len(metagraph["vertexCollections"]) == 0:
            raise PhenolError("vertexCollections must map to non-empty dictionary")
        if len(metagraph["edgeCollections"]) == 0:
            raise PhenolError("edgeCollections must map to non-empty dictionary")

        if len(metagraph["vertexCollections"]) > 1:
            raise PhenolError(
                "More than one vertex collection specified for homogeneous dataset"
            )
        if len(metagraph["edgeCollections"]) > 1:
            raise PhenolError(
                "More than one edge collection specified for homogeneous dataset"
            )

        v_col_spec_name = list(metagraph["vertexCollections"].keys())[0]
        v_col_spec = list(metagraph["vertexCollections"].values())[0]

        (
            features_by_col,
            coo_map,
            col_to_adb_id_to_ind,
            vertex_cols_source_to_output,
        ) = NumpyLoader.load_graph_to_numpy(
            database,
            metagraph,
            hosts,
            user_jwt,
            username,
            password,
            tls_cert,
            parallelism,
            batch_size,
            pyg_index_field,
        )

        data = Data()
        # add the features
        if v_col_spec_name not in features_by_col:
            raise PhenolError(f"Unable to load data for collection {v_col_spec_name}")
        for feature in v_col_spec.keys() - {pyg_index_field}:
            feature_source_key = v_col_spec[feature]
            if feature_source_key not in features_by_col[v_col_spec_name]:
                raise PhenolError(
                    f"Unable to load features {feature_source_key} for collection {v_col_spec_name}"  # noqa: E501
                )
            result = torch.from_numpy(
                features_by_col[v_col_spec_name][feature_source_key].astype(np.float64)
            )
            if result.numel() > 0:
                data[feature] = result

        # finally add the edges
        edge_col_name = list(metagraph["edgeCollections"].keys())[0]
        for e_tup in coo_map.keys():
            e_name, from_name, to_name = e_tup
            if e_name == edge_col_name:
                result = torch.from_numpy(coo_map[e_tup].astype(np.int64))
                if result.numel() > 0:
                    data["edge_index"] = result

        return data, col_to_adb_id_to_ind

    @staticmethod
    def load_into_pyg_heterodata(
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
    ) -> tuple[HeteroData, dict[str, dict[str, int]]]:
        if "vertexCollections" not in metagraph:
            raise PhenolError("vertexCollections not found in metagraph")
        if "edgeCollections" not in metagraph:
            raise PhenolError("edgeCollections not found in metagraph")

        if len(metagraph["vertexCollections"]) == 0:
            raise PhenolError("vertexCollections must map to non-empty dictionary")
        if len(metagraph["edgeCollections"]) == 0:
            raise PhenolError("edgeCollections must map to non-empty dictionary")

        (
            features_by_col,
            coo_map,
            col_to_adb_id_to_ind,
            vertex_cols_source_to_output,
        ) = NumpyLoader.load_graph_to_numpy(
            database,
            metagraph,
            hosts,
            user_jwt,
            username,
            password,
            tls_cert,
            parallelism,
            batch_size,
            pyg_index_field,
        )
        data = HeteroData()
        for col in features_by_col.keys():
            col_mapping = vertex_cols_source_to_output[col]
            for feature in features_by_col[col].keys() - {pyg_index_field}:
                target_name = col_mapping[feature]
                result = torch.from_numpy(
                    features_by_col[col][feature].astype(np.float64)
                )
                if result.numel() > 0:
                    data[col][target_name] = result

        for edge_col in coo_map.keys():
            edge_col_name, from_name, to_name = edge_col
            result = torch.from_numpy(coo_map[edge_col].astype(np.int64))
            if result.numel() > 0:
                data[(from_name, edge_col_name, to_name)].edge_index = result

        return data, col_to_adb_id_to_ind
