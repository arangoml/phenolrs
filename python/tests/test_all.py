import numpy
import pytest
from torch_geometric.data import HeteroData

from phenolrs import PhenolError
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
    # MutliDiGraph
    res = NetworkXLoader.load_into_networkx(
        connection_information["dbName"],
        {
            "vertexCollections": {"Subjects": set()},
            "edgeCollections": {"medical_affinity_graph": set()},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        is_directed=True,
        is_multigraph=True,
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

    from_key = next(iter(adj_dict.keys()))
    assert isinstance(adj_dict[from_key], dict)
    to_key = next(iter(adj_dict[from_key].keys()))
    assert isinstance(adj_dict[from_key][to_key], dict)

    assert len(adj_dict[from_key][to_key]) == 1
    index_key = next(iter(adj_dict[from_key][to_key].keys()))
    assert index_key == 0
    assert isinstance(adj_dict[from_key][to_key][index_key], dict)  # type: ignore

    # DiGraph
    res = NetworkXLoader.load_into_networkx(
        connection_information["dbName"],
        {
            "vertexCollections": {},  # No vertexCollections
            "edgeCollections": {"medical_affinity_graph": set()},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        load_coo=False,
        is_directed=True,
        is_multigraph=False,
    )
    node_dict, adj_dict, src_indices, dst_indices, vertex_ids_to_indices = res

    assert (
        len(node_dict)
        == len(src_indices)
        == len(dst_indices)
        == len(vertex_ids_to_indices)
        == 0
    )
    assert len(adj_dict[from_key][to_key].keys()) > 1
    for key in adj_dict[from_key][to_key].keys():
        assert isinstance(key, str)

    # MultiGraph
    res = NetworkXLoader.load_into_networkx(
        connection_information["dbName"],
        {
            "vertexCollections": {},  # No vertexCollections
            "edgeCollections": {"medical_affinity_graph": set()},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        load_coo=False,
        is_directed=False,
        is_multigraph=True,
    )
    node_dict, adj_dict, src_indices, dst_indices, vertex_ids_to_indices = res

    assert len(adj_dict[from_key][to_key]) == 2
    for key in adj_dict[from_key][to_key].keys():
        assert isinstance(key, int)

    # Graph
    res = NetworkXLoader.load_into_networkx(
        connection_information["dbName"],
        {
            "vertexCollections": {},  # No vertexCollections
            "edgeCollections": {"medical_affinity_graph": set()},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        load_coo=False,
        is_directed=False,
        is_multigraph=False,
    )
    node_dict, adj_dict, src_indices, dst_indices, vertex_ids_to_indices = res

    assert len(adj_dict[from_key][to_key]) > 1
    for key in adj_dict[from_key][to_key].keys():
        assert isinstance(key, str)

    # Graph (no vertex/edge attributes)
    res = NetworkXLoader.load_into_networkx(
        connection_information["dbName"],
        {
            "vertexCollections": {"Subjects": set()},
            "edgeCollections": {"medical_affinity_graph": set()},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        load_adj_dict=True,
        load_coo=False,
        load_all_vertex_attributes=False,  # no node data
        load_all_edge_attributes=False,  # no edge data
        is_directed=False,
        is_multigraph=False,
    )
    node_dict, adj_dict, _, _, _ = res

    assert len(node_dict) == len(adj_dict) == 871
    for v in node_dict.values():
        assert isinstance(v, dict)
        assert len(v) == 0

    for v1 in adj_dict.values():
        for v2 in v1.values():
            assert isinstance(v2, dict)
            assert list(v2.keys()) == ["_id"]

    # Graph (custom vertex/edge attributes)
    with pytest.raises(PhenolError):
        res = NetworkXLoader.load_into_networkx(
            connection_information["dbName"],
            {
                "vertexCollections": {"Subjects": {"label"}},
                "edgeCollections": {"medical_affinity_graph": {"_rev"}},
            },
            [connection_information["url"]],
            username=connection_information["username"],
            password=connection_information["password"],
            load_all_vertex_attributes=True,  # v collection contain attributes
        )

    with pytest.raises(PhenolError):
        res = NetworkXLoader.load_into_networkx(
            connection_information["dbName"],
            {
                "vertexCollections": {"Subjects": {"label"}},
                "edgeCollections": {"medical_affinity_graph": {"_rev"}},
            },
            [connection_information["url"]],
            username=connection_information["username"],
            password=connection_information["password"],
            load_all_vertex_attributes=False,
            load_all_edge_attributes=True,  # e collection contain attributes
        )

    res = NetworkXLoader.load_into_networkx(
        connection_information["dbName"],
        {
            "vertexCollections": {"Subjects": {"label"}},
            "edgeCollections": {"medical_affinity_graph": {"_rev"}},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        load_adj_dict=True,
        load_coo=False,
        load_all_vertex_attributes=False,
        load_all_edge_attributes=False,
        is_directed=False,
        is_multigraph=False,
    )

    node_dict, adj_dict, _, _, _ = res

    assert len(node_dict) == len(adj_dict) == 871
    for v in node_dict.values():
        assert isinstance(v, dict)
        assert list(v.keys()) == ["label"]

    for v1 in adj_dict.values():
        for v2 in v1.values():
            assert isinstance(v2, dict)
            assert list(v2.keys()) == ["_id", "_rev"]
