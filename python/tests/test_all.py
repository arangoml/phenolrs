import arango
import pytest
from torch_geometric.data import HeteroData

from phenolrs import PhenolError
from phenolrs.numpy_loader import NumpyLoader
from phenolrs.pyg_loader import PygLoader


def test_phenol_abide_hetero(
    load_abide: None, connection_information: dict[str, str]
) -> None:
    result = PygLoader.load_into_pyg_heterodata(
        connection_information["dbName"],
        {
            "vertexCollections": {
                "Subjects": {"x": "brain_fmri_features", "y": "label"}
            },
            "edgeCollections": {"medical_affinity_graph": {}},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
    )

    data, col_to_adb_id_to_ind = result
    assert isinstance(data, HeteroData)
    assert data["Subjects"]["x"].shape == (871, 2000)
    assert len(col_to_adb_id_to_ind["Subjects"]) == 871

    # Metagraph variation
    result = PygLoader.load_into_pyg_heterodata(
        connection_information["dbName"],
        {
            "vertexCollections": {
                "Subjects": {"x": {"brain_fmri_features": None}, "y": "label"}
            },
            "edgeCollections": {"medical_affinity_graph": {}},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
    )

    data, col_to_adb_id_to_ind = result
    assert isinstance(data, HeteroData)
    assert data["Subjects"]["x"].shape == (871, 2000)
    assert len(col_to_adb_id_to_ind["Subjects"]) == 871


def test_phenol_abide_numpy(
    load_abide: None, connection_information: dict[str, str]
) -> None:
    (
        features_by_col,
        coo_map,
        col_to_adb_id_to_ind,
        vertex_cols_source_to_output,
    ) = NumpyLoader.load_graph_to_numpy(
        connection_information["dbName"],
        {
            "vertexCollections": {"Subjects": {"x": "brain_fmri_features"}},
            "edgeCollections": {"medical_affinity_graph": {}},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
    )

    assert features_by_col["Subjects"]["brain_fmri_features"].shape == (871, 2000)
    assert coo_map[("medical_affinity_graph", "Subjects", "Subjects")].shape == (
        2,
        606770,
    )
    assert len(col_to_adb_id_to_ind["Subjects"]) == 871
    assert vertex_cols_source_to_output == {"Subjects": {"brain_fmri_features": "x"}}

    (
        features_by_col,
        coo_map,
        col_to_adb_id_to_ind,
        vertex_cols_source_to_output,
    ) = NumpyLoader.load_graph_to_numpy(
        connection_information["dbName"],
        {
            "vertexCollections": {"Subjects": {"x": "brain_fmri_features"}},
            # "edgeCollections": {"medical_affinity_graph": {}},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
    )

    assert features_by_col["Subjects"]["brain_fmri_features"].shape == (871, 2000)
    assert len(coo_map) == 0
    assert len(col_to_adb_id_to_ind["Subjects"]) == 871
    assert vertex_cols_source_to_output == {"Subjects": {"brain_fmri_features": "x"}}
