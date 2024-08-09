from typing import Any

import numpy
import pytest
from torch_geometric.data import HeteroData

from phenolrs import PhenolError
from phenolrs.networkx import NetworkXLoader
from phenolrs.numpy import NumpyLoader
from phenolrs.pyg import PygLoader


def test_abide_hetero(
    load_abide: None, abide_db_name: str, connection_information: dict[str, str]
) -> None:
    result = PygLoader.load_into_pyg_heterodata(
        abide_db_name,
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
        abide_db_name,
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


def test_abide_numpy(
    load_abide: None, abide_db_name: str, connection_information: dict[str, str]
) -> None:
    (
        features_by_col,
        coo_map,
        col_to_adb_key_to_ind,
        col_to_ind_to_adb_key,
        vertex_cols_source_to_output,
    ) = NumpyLoader.load_graph_to_numpy(
        abide_db_name,
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
        abide_db_name,
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


def test_karate_networkx(
    load_karate: None, karate_db_name: str, connection_information: dict[str, str]
) -> None:
    adj_dict: Any
    from_key = "person/1"
    to_key = "person/2"

    # MultiDiGraph
    res = NetworkXLoader.load_into_networkx(
        karate_db_name,
        {
            "vertexCollections": {"person": set()},
            "edgeCollections": {"knows": set()},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        is_directed=True,
        is_multigraph=True,
    )
    assert isinstance(res, tuple)
    (
        node_dict,
        adj_dict,
        src_indices,
        dst_indices,
        edge_indices,
        vertex_ids_to_indices,
        edge_values,
    ) = res

    assert isinstance(node_dict, dict)
    assert isinstance(adj_dict, dict)
    assert isinstance(src_indices, numpy.ndarray)
    assert isinstance(dst_indices, numpy.ndarray)
    assert isinstance(vertex_ids_to_indices, dict)
    assert isinstance(edge_values, dict)
    assert len(node_dict) == len(vertex_ids_to_indices) == 34
    assert len(src_indices) == len(dst_indices) == len(edge_indices) == 78
    assert len(edge_values) == 0

    assert set(adj_dict.keys()) == {"succ", "pred"}
    succ = adj_dict["succ"]
    assert isinstance(succ[from_key], dict)
    to_key = list(succ[from_key].keys())[0]
    assert isinstance(succ[from_key][to_key], dict)

    assert len(succ[from_key][to_key]) == 1
    index_key = list(succ[from_key][to_key].keys())[0]
    assert index_key == 0
    assert isinstance(succ[from_key][to_key][index_key], dict)

    pred = adj_dict["pred"]
    assert from_key in pred
    assert to_key in pred
    assert from_key in pred[to_key]
    assert len(pred[to_key][from_key]) == 1
    assert pred[to_key][from_key][index_key] == succ[from_key][to_key][index_key]

    assert from_key not in succ[to_key]
    assert to_key not in pred[from_key]

    for from_id, adj in adj_dict["succ"].items():
        for to_id, edge in adj.items():
            assert isinstance(edge, dict)
            assert edge == adj_dict["pred"][to_id][from_id]

    assert set(node_dict[from_key].keys()) == {"_id", "_key", "_rev", "club"}

    # MultiDiGraph (with edge symmetry)
    res = NetworkXLoader.load_into_networkx(
        karate_db_name,
        {
            "vertexCollections": {"person": set()},
            "edgeCollections": {"knows": set()},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        is_directed=True,
        is_multigraph=True,
        symmetrize_edges_if_directed=True,
    )
    (
        node_dict,
        adj_dict,
        src_indices,
        dst_indices,
        edge_indices,
        vertex_ids_to_indices,
        edge_values,
    ) = res
    assert from_key in adj_dict["succ"][to_key]
    assert to_key in adj_dict["pred"][from_key]
    assert len(src_indices) == len(dst_indices) == len(edge_indices) == 156
    assert isinstance(edge_values, dict)
    assert len(edge_values) == 0

    # DiGraph
    res = NetworkXLoader.load_into_networkx(
        karate_db_name,
        {
            "vertexCollections": {},  # No vertexCollections
            "edgeCollections": {"knows": set()},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        load_coo=True,
        is_directed=True,
        is_multigraph=False,
    )
    (
        node_dict,
        adj_dict,
        src_indices,
        dst_indices,
        edge_indices,
        vertex_ids_to_indices,
        edge_values,
    ) = res

    assert len(src_indices) == len(dst_indices) == 78
    assert len(edge_indices) == 0
    for from_id, adj in adj_dict["succ"].items():
        for to_id, edge in adj.items():
            assert isinstance(edge, dict)
            assert edge == adj_dict["pred"][to_id][from_id]
    assert isinstance(edge_values, dict)
    assert len(edge_values) == 0

    # MultiGraph
    res = NetworkXLoader.load_into_networkx(
        karate_db_name,
        {
            "vertexCollections": {},  # No vertexCollections
            "edgeCollections": {"knows": set()},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        load_coo=False,
        is_directed=False,
        is_multigraph=True,
    )
    (
        node_dict,
        adj_dict,
        src_indices,
        dst_indices,
        edge_indices,
        vertex_ids_to_indices,
        edge_values,
    ) = res

    assert (
        len(node_dict)
        == len(src_indices)
        == len(dst_indices)
        == len(edge_indices)
        == len(vertex_ids_to_indices)
        == 0
    )

    assert len(adj_dict[from_key][to_key]) == 1
    assert type(next(iter(adj_dict[from_key][to_key].keys()))) is int
    assert isinstance(adj_dict[from_key][to_key][0], dict)  # type: ignore
    assert isinstance(edge_values, dict)
    assert len(edge_values) == 0

    # Graph
    res = NetworkXLoader.load_into_networkx(
        karate_db_name,
        {
            "vertexCollections": {},  # No vertexCollections
            "edgeCollections": {"knows": set()},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        load_coo=False,
        is_directed=False,
        is_multigraph=False,
    )
    (
        node_dict,
        adj_dict,
        src_indices,
        dst_indices,
        edge_indices,
        vertex_ids_to_indices,
        edge_values,
    ) = res

    assert len(edge_indices) == 0
    assert len(adj_dict[from_key][to_key]) > 1
    for key in adj_dict[from_key][to_key].keys():
        assert isinstance(key, str)
    assert isinstance(edge_values, dict)
    assert len(edge_values) == 0

    # Graph (no vertex/edge attributes)
    res = NetworkXLoader.load_into_networkx(
        karate_db_name,
        {
            "vertexCollections": {"person": set()},
            "edgeCollections": {"knows": set()},
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
    (
        node_dict,
        adj_dict,
        _,
        _,
        _,
        _,
        _,
    ) = res

    assert len(node_dict) == len(adj_dict) > 0
    for v in node_dict.values():
        assert isinstance(v, dict)
        assert len(v) == 0

    for v1 in adj_dict.values():
        for v2 in v1.values():
            assert isinstance(v2, dict)
            assert len(v) == 0

    # Graph (custom vertex/edge attributes)
    with pytest.raises(PhenolError):
        res = NetworkXLoader.load_into_networkx(
            karate_db_name,
            {
                "vertexCollections": {"person": {"club"}},
                "edgeCollections": {"knows": {"weight"}},
            },
            [connection_information["url"]],
            username=connection_information["username"],
            password=connection_information["password"],
            load_all_vertex_attributes=True,  # v collection contain attributes
        )

    with pytest.raises(PhenolError):
        res = NetworkXLoader.load_into_networkx(
            karate_db_name,
            {
                "vertexCollections": {"person": {"club"}},
                "edgeCollections": {"knows": {"weight"}},
            },
            [connection_information["url"]],
            username=connection_information["username"],
            password=connection_information["password"],
            load_all_vertex_attributes=False,
            load_all_edge_attributes=True,  # e collection contain attributes
        )

    res = NetworkXLoader.load_into_networkx(
        karate_db_name,
        {
            "vertexCollections": {"person": {"club"}},
            "edgeCollections": {"knows": {"weight"}},
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

    node_dict, adj_dict, _, _, _, _, _ = res

    assert len(node_dict) == len(adj_dict) > 0
    for v in node_dict.values():
        assert isinstance(v, dict)
        assert list(v.keys()) == ["club"]

    for v1 in adj_dict.values():
        for v2 in v1.values():
            assert isinstance(v2, dict)
            assert list(v2.keys()) == ["weight"]

    # Test that numeric values out of edges can be read
    res = NetworkXLoader.load_into_networkx(
        karate_db_name,
        {
            "vertexCollections": {"person": {"club"}},
            "edgeCollections": {"knows": {"weight"}},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        load_adj_dict=True,
        load_coo=True,
        load_all_vertex_attributes=False,
        load_all_edge_attributes=False,
        is_directed=False,
        is_multigraph=False,
    )

    _, _, _, _, _, _, edge_values = res

    assert isinstance(edge_values, dict)
    assert "weight" in edge_values
    assert isinstance(edge_values["weight"], list)
    assert len(edge_values["weight"]) == 78
    assert all(isinstance(x, (int, float)) for x in edge_values["weight"])

    # Test that non-numeric read of edge values will fail
    try:
        res = NetworkXLoader.load_into_networkx(
            karate_db_name,
            {
                "vertexCollections": {"person": {"club"}},
                # Selecting _key here as this is guaranteed to be a string
                "edgeCollections": {"knows": {"_key"}},
            },
            [connection_information["url"]],
            username=connection_information["username"],
            password=connection_information["password"],
            load_adj_dict=True,
            load_coo=True,
            load_all_vertex_attributes=False,
            load_all_edge_attributes=False,
            is_directed=False,
            is_multigraph=False,
        )
    except PhenolError as e:
        # TODO: Propagate the real error message from the loader
        assert "Could not insert edge" in str(e)
        assert "Edge data must be a numeric value" in str(e)


def test_multigraph_networkx(
    load_multigraph: None,
    multigraph_db_name: str,
    connection_information: dict[str, str],
) -> None:
    res = NetworkXLoader.load_into_networkx(
        multigraph_db_name,
        {
            "vertexCollections": {},
            "edgeCollections": {"to": set()},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        load_adj_dict=False,
        load_coo=True,
        is_directed=False,
        is_multigraph=True,
    )

    (
        _,
        _,
        src_indices,
        dst_indices,
        edge_indices,
        _,
        _,  # edge_values
    ) = res

    assert list(src_indices) == [0, 1, 0, 1, 1, 2, 2, 3, 2, 3]
    assert list(dst_indices) == [1, 0, 1, 0, 2, 1, 3, 2, 3, 2]
    assert list(edge_indices) == [0, 0, 1, 1, 0, 0, 0, 0, 1, 1]

    res = NetworkXLoader.load_into_networkx(
        multigraph_db_name,
        {
            "vertexCollections": {},
            "edgeCollections": {"to": set()},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        load_adj_dict=False,
        load_coo=True,
        is_directed=True,
        is_multigraph=True,
    )

    (
        _,
        _,
        src_indices,
        dst_indices,
        edge_indices,
        _,
        _,  # edge_values
    ) = res

    assert list(src_indices) == [0, 0, 1, 2, 2]
    assert list(dst_indices) == [1, 1, 2, 3, 3]
    assert list(edge_indices) == [0, 1, 0, 0, 1]
