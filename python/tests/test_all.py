import numpy
from torch_geometric.data import HeteroData

from phenolrs.networkx_loader import NetworkXLoader
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

    data, col_to_adb_key_to_ind, col_to_ind_to_adb_key = result
    assert isinstance(data, HeteroData)
    assert data["Subjects"]["x"].shape == (871, 2000)
    assert (
        len(col_to_adb_key_to_ind["Subjects"])
        == len(col_to_ind_to_adb_key["Subjects"])
        == 871
    )

    assert data[("Subjects", "medical_affinity_graph", "Subjects")][
        "edge_index"
    ].shape == (2, 606770)

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

    data, col_to_adb_key_to_ind, col_to_ind_to_adb_key = result
    assert isinstance(data, HeteroData)
    assert data["Subjects"]["x"].shape == (871, 2000)
    assert (
        len(col_to_adb_key_to_ind["Subjects"])
        == len(col_to_ind_to_adb_key["Subjects"])
        == 871
    )

    assert data[("Subjects", "medical_affinity_graph", "Subjects")][
        "edge_index"
    ].shape == (2, 606770)


def test_phenol_abide_numpy(
    load_abide: None, connection_information: dict[str, str]
) -> None:
    (
        features_by_col,
        coo_map,
        col_to_adb_key_to_ind,
        col_to_ind_to_adb_key,
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
    assert (
        len(col_to_adb_key_to_ind["Subjects"])
        == len(col_to_ind_to_adb_key["Subjects"])
        == 871
    )
    assert vertex_cols_source_to_output == {"Subjects": {"brain_fmri_features": "x"}}

    (
        features_by_col,
        coo_map,
        col_to_adb_key_to_ind,
        col_to_ind_to_adb_key,
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
    assert (
        len(col_to_adb_key_to_ind["Subjects"])
        == len(col_to_ind_to_adb_key["Subjects"])
        == 871
    )
    assert vertex_cols_source_to_output == {"Subjects": {"brain_fmri_features": "x"}}


def test_phenol_abide_networkx(
    load_abide: None, connection_information: dict[str, str]
) -> None:
    res = NetworkXLoader.load_into_networkx(
        connection_information["dbName"],
        {
            "vertexCollections": {"Subjects": {}},
            "edgeCollections": {"medical_affinity_graph": {}},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
    )
    assert isinstance(res, tuple)
    node_dict, adj_dict, src_indices, dst_indices, vertex_ids_to_indices = res

    assert isinstance(node_dict, dict)
    assert isinstance(adj_dict, dict)
    assert isinstance(src_indices, numpy.ndarray)
    assert isinstance(dst_indices, numpy.ndarray)
    assert isinstance(vertex_ids_to_indices, dict)
    assert len(node_dict) == len(vertex_ids_to_indices) > 0
    assert len(src_indices) == len(dst_indices) > 0
