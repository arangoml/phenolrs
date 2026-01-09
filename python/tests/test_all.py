from typing import Any, Callable

import numpy
import pytest

try:
    from torch_geometric.data import Data, HeteroData

    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

    class Data:  # type: ignore
        pass

    class HeteroData:  # type: ignore
        pass


from phenolrs import PhenolError
from phenolrs.aql import AqlLoader, AqlQuery
from phenolrs.networkx import NetworkXLoader
from phenolrs.numpy import NumpyLoader
from phenolrs.pyg import PygLoader


@pytest.mark.skipif(not TORCH_AVAILABLE, reason="PyTorch is not available")
@pytest.mark.parametrize(
    "pyg_load_function, datatype",
    [
        (PygLoader.load_into_pyg_data, Data),
        (PygLoader.load_into_pyg_heterodata, HeteroData),
    ],
)
def test_abide_pyg(
    pyg_load_function: Callable[..., Any],
    datatype: type[Data],
    load_abide: None,
    abide_db_name: str,
    connection_information: dict[str, str],
) -> None:
    metagraphs = [
        {
            "vertexCollections": {
                "Subjects": {"x": "brain_fmri_features", "y": "label"}
            },
            "edgeCollections": {"medical_affinity_graph": {}},
        },
        {
            "vertexCollections": {
                "Subjects": {"x": {"brain_fmri_features": None}, "y": "label"}
            },
            "edgeCollections": {"medical_affinity_graph": {}},
        },
    ]

    for metagraph in metagraphs:
        result = pyg_load_function(
            abide_db_name,
            metagraph,
            [connection_information["url"]],
            username=connection_information["username"],
            password=connection_information["password"],
        )

        data, col_to_adb_key_to_ind, col_to_ind_to_adb_key = result
        assert isinstance(data, datatype)

        nodes = edges = data
        if isinstance(data, HeteroData):
            nodes = data["Subjects"]
            edges = data[("Subjects", "medical_affinity_graph", "Subjects")]

        assert nodes["x"].shape == (871, 2000)
        assert (
            len(col_to_adb_key_to_ind["Subjects"])
            == len(col_to_ind_to_adb_key["Subjects"])
            == 871
        )

        assert edges["edge_index"].shape == (2, 606770)


@pytest.mark.skipif(not TORCH_AVAILABLE, reason="PyTorch is not available")
def test_imdb_pyg(
    load_imdb: None,
    imdb_db_name: str,
    connection_information: dict[str, str],
) -> None:
    metagraph = {
        "vertexCollections": {
            "MOVIE": {"x": "features", "y": "should_recommend"},
            "USER": {"x": "features"},
        },
        "edgeCollections": {"VIEWS": {}},
    }

    result = PygLoader.load_into_pyg_heterodata(
        imdb_db_name,
        metagraph,
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
    )

    data, col_to_adb_key_to_ind, col_to_ind_to_adb_key = result
    assert isinstance(data, HeteroData)
    assert set(data.node_types) == {"MOVIE", "USER"}
    assert data.edge_types == [("USER", "VIEWS", "MOVIE")]
    assert data["MOVIE"]["y"].shape == (1682, 1)
    assert data["MOVIE"]["x"].shape == (1682, 403)
    assert (
        len(col_to_adb_key_to_ind["MOVIE"])
        == len(col_to_ind_to_adb_key["MOVIE"])
        == 1682
    )

    assert data["USER"]["x"].shape == (943, 385)
    assert (
        len(col_to_adb_key_to_ind["USER"]) == len(col_to_ind_to_adb_key["USER"]) == 943
    )

    edges = data[("USER", "VIEWS", "MOVIE")]
    assert edges["edge_index"].shape == (2, 100000)


@pytest.mark.skipif(not TORCH_AVAILABLE, reason="PyTorch is not available")
def test_dblp_pyg(
    load_dblp: None,
    dblp_db_name: str,
    connection_information: dict[str, str],
) -> None:
    metagraph_1 = {
        "vertexCollections": {
            "author": {"x": "x"},
            "paper": {"x": "x"},
            "term": {"x": "x"},
            "conference": {},
        },
        "edgeCollections": {
            "to": {},
        },
    }

    result_1 = PygLoader.load_into_pyg_heterodata(
        dblp_db_name,
        metagraph_1,
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
    )

    metagraph_2 = {
        "vertexCollections": {
            "author": {"x": "x"},
            "paper": {"x": "x"},
            "term": {"x": "x"},
        },
        "edgeCollections": {
            "to": {},
        },
    }

    result_2 = PygLoader.load_into_pyg_heterodata(
        dblp_db_name,
        metagraph_2,
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
    )

    for result in [result_1, result_2]:
        data, col_to_adb_key_to_ind, col_to_ind_to_adb_key = result

        assert isinstance(data, HeteroData)
        assert set(data.node_types) == {"author", "paper", "term"}
        assert set(data.edge_types) == {
            ("term", "to", "paper"),
            ("author", "to", "paper"),
            ("paper", "to", "term"),
            ("paper", "to", "author"),
        }
        assert data["author"]["x"].shape == (4057, 334)
        assert data["paper"]["x"].shape == (14328, 4231)
        assert data["term"]["x"].shape == (7723, 50)

        assert (
            len(col_to_adb_key_to_ind["author"])
            == len(col_to_ind_to_adb_key["author"])
            == 4057
        )
        assert (
            len(col_to_adb_key_to_ind["paper"])
            == len(col_to_ind_to_adb_key["paper"])
            == 14328
        )
        assert (
            len(col_to_adb_key_to_ind["term"])
            == len(col_to_ind_to_adb_key["term"])
            == 7723
        )

        edges = data[("author", "to", "paper")]
        assert edges["edge_index"].shape == (2, 19645)

        edges = data[("paper", "to", "author")]
        assert edges["edge_index"].shape == (2, 19645)

        edges = data[("term", "to", "paper")]
        assert edges["edge_index"].shape == (2, 85810)

        edges = data[("paper", "to", "term")]
        assert edges["edge_index"].shape == (2, 85810)


@pytest.mark.numpy
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


@pytest.mark.networkx
def test_karate_networkx(
    load_karate: None, karate_db_name: str, connection_information: dict[str, str]
) -> None:
    adj_dict: Any
    from_key = "person/1"
    to_key = "person/2"
    # TODO: This value is actually never used. This var
    # is going to be overwritten.

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
    assert isinstance(adj_dict[from_key][to_key][0], dict)
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
        NetworkXLoader.load_into_networkx(
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
        NetworkXLoader.load_into_networkx(
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
    # -> In this case, strings are being tested.
    with pytest.raises(PhenolError) as e:
        NetworkXLoader.load_into_networkx(
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
        assert "Could not insert edge" in str(e)
        assert "Edge data must be a numeric value" in str(e)


@pytest.mark.networkx
def test_coo_edge_values_networkx(
    load_line_graph: None,
    custom_graph_db_name: str,
    connection_information: dict[str, str],
) -> None:
    # Non-numeric: Booleans
    with pytest.raises(PhenolError) as e:
        NetworkXLoader.load_into_networkx(
            custom_graph_db_name,
            {
                "vertexCollections": {"line_graph_vertices": set()},
                "edgeCollections": {"line_graph_edges": {"boolean_weight"}},
            },
            [connection_information["url"]],
            username=connection_information["username"],
            password=connection_information["password"],
            load_adj_dict=False,
            load_coo=True,
            load_all_vertex_attributes=False,
            load_all_edge_attributes=False,
            is_directed=False,
            is_multigraph=False,
        )
        assert "Could not insert edge" in str(e)
        assert "Edge data must be a numeric value" in str(e)

    # Numeric: Ints
    res = NetworkXLoader.load_into_networkx(
        custom_graph_db_name,
        {
            "vertexCollections": {"line_graph_vertices": set()},
            "edgeCollections": {"line_graph_edges": {"int_value"}},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        load_adj_dict=False,
        load_coo=True,
        load_all_vertex_attributes=False,
        load_all_edge_attributes=False,
        is_directed=False,
        is_multigraph=False,
    )
    _, _, _, _, _, _, edge_values = res

    assert isinstance(edge_values, dict)
    assert "int_value" in edge_values
    assert isinstance(edge_values["int_value"], list)
    assert len(edge_values["int_value"]) == 4
    assert all(isinstance(x, float) for x in edge_values["int_value"])

    # Numeric: Floats
    res = NetworkXLoader.load_into_networkx(
        custom_graph_db_name,
        {
            "vertexCollections": {"line_graph_vertices": set()},
            "edgeCollections": {"line_graph_edges": {"float_value"}},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        load_adj_dict=False,
        load_coo=True,
        load_all_vertex_attributes=False,
        load_all_edge_attributes=False,
        is_directed=False,
        is_multigraph=False,
    )
    _, _, _, _, _, _, edge_values = res

    assert isinstance(edge_values, dict)
    assert "float_value" in edge_values
    assert isinstance(edge_values["float_value"], list)
    assert len(edge_values["float_value"]) == 4
    assert all(isinstance(x, float) for x in edge_values["float_value"])


@pytest.mark.networkx
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


@pytest.mark.networkx
def test_imdb_networkx(
    load_imdb: None,
    imdb_db_name: str,
    connection_information: dict[str, str],
) -> None:
    metagraph: dict[str, Any] = {
        "vertexCollections": {
            "MOVIE": {},
            "USER": {},
        },
        "edgeCollections": {"VIEWS": {}},
    }

    node_dict, adj_dict, *_ = NetworkXLoader.load_into_networkx(
        imdb_db_name,
        metagraph,
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        is_directed=True,
        is_multigraph=True,
        load_all_vertex_attributes=False,
        load_all_edge_attributes=False,
        load_coo=False,
    )

    assert isinstance(adj_dict, dict)
    assert len(adj_dict["succ"]) == len(adj_dict["pred"]) == len(node_dict) == 2625
    assert len(adj_dict["succ"]["USER/1"]) == 272
    assert node_dict["USER/1"] == {}
    assert adj_dict["succ"]["USER/1"]["MOVIE/1"] == {0: {}}  # type: ignore

    metagraph = {
        "vertexCollections": {
            "MOVIE": {"title"},
            "USER": {},
        },
        "edgeCollections": {"VIEWS": {"timestamp"}},
    }

    node_dict, adj_dict, *_ = NetworkXLoader.load_into_networkx(
        imdb_db_name,
        metagraph,
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        is_directed=True,
        is_multigraph=True,
        load_all_vertex_attributes=False,
        load_all_edge_attributes=False,
        load_coo=False,
    )

    assert adj_dict["succ"]["USER/1"]["MOVIE/1"] == {0: {"timestamp": 874965758}}  # type: ignore  # noqa: E501
    assert node_dict["MOVIE/1"] == {"title": "Toy Story (1995)"}
    assert node_dict["USER/1"] == {}

    metagraph = {
        "vertexCollections": {
            "MOVIE": {"title", "release_date"},
            "USER": {"occupation"},
        },
        "edgeCollections": {"VIEWS": {"timestamp"}},
    }

    node_dict, adj_dict, *_ = NetworkXLoader.load_into_networkx(
        imdb_db_name,
        metagraph,
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        is_directed=True,
        is_multigraph=True,
        load_all_vertex_attributes=False,
        load_all_edge_attributes=False,
        load_coo=False,
    )

    assert node_dict["MOVIE/1"] == {
        "release_date": "01-Jan-1995",
        "title": "Toy Story (1995)",
    }
    assert node_dict["USER/1"] == {"occupation": "technician"}

    metagraph = {
        "vertexCollections": {
            "MOVIE": {},
            "USER": {},
        },
        "edgeCollections": {"VIEWS": {}},
    }

    node_dict, adj_dict, *_ = NetworkXLoader.load_into_networkx(
        imdb_db_name,
        metagraph,
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        is_directed=True,
        is_multigraph=True,
        load_all_vertex_attributes=True,
        load_all_edge_attributes=True,
        load_coo=False,
    )

    for node_id, node in node_dict.items():
        assert isinstance(node_id, str)
        assert isinstance(node, dict)
        for key, value in node.items():
            assert isinstance(key, str)
            assert value is not None

    for adj_key, adj in adj_dict.items():
        assert isinstance(adj_key, str)
        assert isinstance(adj, dict)
        for from_node_id, from_node_adj in adj.items():
            assert isinstance(from_node_id, str)
            assert isinstance(from_node_adj, dict)
            for to_node_id, edges in from_node_adj.items():
                assert isinstance(to_node_id, str)
                assert isinstance(edges, dict)
                for edge_id, edge in edges.items():
                    assert isinstance(edge_id, int)  # TODO: Switch to str?
                    assert isinstance(edge, dict)
                    for key, value in edge.items():
                        assert isinstance(key, str)
                        assert value is not None


@pytest.mark.networkx
def test_isolated_node_networkx(
    load_isolated_node: None,
    isolated_node_db_name: str,
    connection_information: dict[str, str],
) -> None:
    metagraph: dict[str, Any] = {
        "vertexCollections": {"node": set()},
        "edgeCollections": {"edge": set()},
    }

    node_dict, adj_dict, *_ = NetworkXLoader.load_into_networkx(
        isolated_node_db_name,
        metagraph,
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        is_directed=False,
        is_multigraph=False,
    )

    assert len(node_dict) == 3
    assert len(adj_dict) == 3
    assert len(adj_dict["node/0"]) == 1
    assert len(adj_dict["node/1"]) == 1
    assert len(adj_dict["node/2"]) == 0

    node_dict, adj_dict, *_ = NetworkXLoader.load_into_networkx(
        isolated_node_db_name,
        metagraph,
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        is_directed=True,
        is_multigraph=False,
    )

    assert len(node_dict) == 3
    assert len(adj_dict["succ"]) == 3
    assert len(adj_dict["pred"]) == 3
    assert len(adj_dict["succ"]["node/0"]) == 1
    assert len(adj_dict["succ"]["node/1"]) == 0
    assert len(adj_dict["succ"]["node/2"]) == 0
    assert len(adj_dict["pred"]["node/0"]) == 0
    assert len(adj_dict["pred"]["node/1"]) == 1
    assert len(adj_dict["pred"]["node/2"]) == 0

    node_dict, adj_dict, *_ = NetworkXLoader.load_into_networkx(
        isolated_node_db_name,
        metagraph,
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        is_directed=False,
        is_multigraph=True,
    )

    assert len(node_dict) == 3
    assert len(adj_dict) == 3
    assert len(adj_dict["node/0"]) == 1
    assert len(adj_dict["node/1"]) == 1
    assert len(adj_dict["node/2"]) == 0

    node_dict, adj_dict, *_ = NetworkXLoader.load_into_networkx(
        isolated_node_db_name,
        metagraph,
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
        is_directed=True,
        is_multigraph=True,
    )

    assert len(node_dict) == 3
    assert len(adj_dict["succ"]) == 3
    assert len(adj_dict["pred"]) == 3
    assert len(adj_dict["succ"]["node/0"]) == 1
    assert len(adj_dict["succ"]["node/1"]) == 0
    assert len(adj_dict["succ"]["node/2"]) == 0
    assert len(adj_dict["pred"]["node/0"]) == 0
    assert len(adj_dict["pred"]["node/1"]) == 1
    assert len(adj_dict["pred"]["node/2"]) == 0


# =============================================================================
# AQL-based Graph Loading Tests
# =============================================================================


@pytest.mark.aql
class TestAqlLoader:
    """Tests for AQL-based graph loading."""

    @pytest.mark.usefixtures("load_aql_test_graph")
    def test_aql_load_vertices_only(
        self,
        aql_test_db_name: str,
        connection_information: dict[str, str],
    ) -> None:
        """Test loading only vertices via AQL."""
        loader = AqlLoader(
            hosts=[connection_information["url"]],
            database=aql_test_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        # Load only users
        queries: list[list[AqlQuery]] = [
            [{"query": "FOR v IN users RETURN {vertices: [v]}"}]
        ]

        result = loader.load_to_networkx(
            queries=queries,
            is_directed=True,
            is_multigraph=False,
            load_coo=False,
        )

        node_dict, adj_dict, src_indices, dst_indices, *_ = result

        assert len(node_dict) == 3
        assert "users/alice" in node_dict
        assert "users/bob" in node_dict
        assert "users/charlie" in node_dict

    @pytest.mark.usefixtures("load_aql_test_graph")
    def test_aql_load_vertices_and_edges_sequential(
        self,
        aql_test_db_name: str,
        connection_information: dict[str, str],
    ) -> None:
        """Test loading vertices first, then edges (sequential groups)."""
        loader = AqlLoader(
            hosts=[connection_information["url"]],
            database=aql_test_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        # First group: load all vertices (parallel)
        # Second group: load all edges
        queries: list[list[AqlQuery]] = [
            # Sequential group 1: vertices
            [
                {"query": "FOR v IN users RETURN {vertices: [v]}"},
                {"query": "FOR v IN products RETURN {vertices: [v]}"},
            ],
            # Sequential group 2: edges
            [
                {"query": "FOR e IN purchases RETURN {edges: [e]}"},
            ],
        ]

        result = loader.load_to_networkx(
            queries=queries,
            is_directed=True,
            is_multigraph=False,
        )

        node_dict, adj_dict, src_indices, dst_indices, *_ = result

        # Check vertices: 3 users + 2 products = 5 vertices
        assert len(node_dict) == 5
        assert "users/alice" in node_dict
        assert "users/bob" in node_dict
        assert "users/charlie" in node_dict
        assert "products/laptop" in node_dict
        assert "products/phone" in node_dict

        # Check edges: 3 purchases
        assert len(src_indices) == 3

    @pytest.mark.usefixtures("load_aql_test_graph")
    def test_aql_load_with_filter(
        self,
        aql_test_db_name: str,
        connection_information: dict[str, str],
    ) -> None:
        """Test loading with AQL filter conditions."""
        loader = AqlLoader(
            hosts=[connection_information["url"]],
            database=aql_test_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        # Load only active users
        queries: list[list[AqlQuery]] = [
            [{"query": "FOR v IN users FILTER v.active == true RETURN {vertices: [v]}"}]
        ]

        result = loader.load_to_networkx(
            queries=queries,
            is_directed=True,
            is_multigraph=False,
            load_coo=False,
        )

        node_dict, *_ = result

        # Only 2 active users (alice and bob)
        assert len(node_dict) == 2
        assert "users/alice" in node_dict
        assert "users/bob" in node_dict
        # charlie is NOT included because filter excludes inactive users
        assert "users/charlie" not in node_dict

    @pytest.mark.usefixtures("load_aql_test_graph")
    def test_aql_load_with_bind_vars(
        self,
        aql_test_db_name: str,
        connection_information: dict[str, str],
    ) -> None:
        """Test loading with AQL bind variables."""
        loader = AqlLoader(
            hosts=[connection_information["url"]],
            database=aql_test_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        # Load users with age >= min_age
        queries: list[list[AqlQuery]] = [
            [
                {
                    "query": (
                        "FOR v IN users FILTER v.age >= @minAge "
                        "RETURN {vertices: [v]}"
                    ),
                    "bindVars": {"minAge": 30},
                }
            ]
        ]

        result = loader.load_to_networkx(
            queries=queries,
            is_directed=True,
            is_multigraph=False,
            load_coo=False,
        )

        node_dict, *_ = result

        # Only users with age >= 30 (alice: 30, charlie: 35)
        assert len(node_dict) == 2
        assert "users/alice" in node_dict
        assert "users/charlie" in node_dict
        # bob is NOT included because age filter excludes him (age=25 < 30)
        assert "users/bob" not in node_dict

    @pytest.mark.usefixtures("load_aql_test_graph")
    def test_aql_load_graph_traversal(
        self,
        aql_test_db_name: str,
        connection_information: dict[str, str],
    ) -> None:
        """Test loading via graph traversal."""
        loader = AqlLoader(
            hosts=[connection_information["url"]],
            database=aql_test_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        # Traverse from alice to find connected products
        queries: list[list[AqlQuery]] = [
            [
                {
                    "query": """
                        FOR v, e IN 0..1 OUTBOUND 'users/alice' GRAPH 'test_graph'
                        RETURN {vertices: [v], edges: [e]}
                    """,
                }
            ]
        ]

        result = loader.load_to_networkx(
            queries=queries,
            is_directed=True,
            is_multigraph=False,
        )

        node_dict, adj_dict, src_indices, *_ = result

        # Should find: alice + laptop + phone = 3 vertices
        assert len(node_dict) == 3
        assert "users/alice" in node_dict
        assert "products/laptop" in node_dict
        assert "products/phone" in node_dict

        # Should find 2 edges (alice -> laptop, alice -> phone)
        assert len(src_indices) == 2

    @pytest.mark.usefixtures("load_aql_test_graph")
    def test_aql_load_to_numpy(
        self,
        aql_test_db_name: str,
        connection_information: dict[str, str],
    ) -> None:
        """Test loading into numpy format via AQL."""
        loader = AqlLoader(
            hosts=[connection_information["url"]],
            database=aql_test_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        queries: list[list[AqlQuery]] = [
            [
                {"query": "FOR v IN users RETURN {vertices: [v]}"},
                {"query": "FOR v IN products RETURN {vertices: [v]}"},
            ],
            [
                {"query": "FOR e IN purchases RETURN {edges: [e]}"},
            ],
        ]

        (
            features_by_col,
            coo_map,
            col_to_key_to_ind,
            col_to_ind_to_key,
        ) = loader.load_to_numpy(
            queries=queries,
            vertex_attributes={"age": "i64", "price": "f64"},
            edge_attributes={"amount": "f64"},
        )

        # We should have entries for users and products collections
        assert isinstance(col_to_key_to_ind, dict)
        assert isinstance(col_to_ind_to_key, dict)
        assert isinstance(features_by_col, dict)
        assert isinstance(coo_map, dict)

        # Verify users collection has 3 vertices
        assert "users" in col_to_key_to_ind
        assert len(col_to_key_to_ind["users"]) == 3

        # Verify products collection has 2 vertices
        assert "products" in col_to_key_to_ind
        assert len(col_to_key_to_ind["products"]) == 2

        # Verify alice is in users index mapping
        assert "alice" in col_to_key_to_ind["users"]

        # Verify edge COO structure exists
        # Edge collection name format: "users_to_products"
        assert len(coo_map) > 0

    def test_aql_helper_create_vertex_query(self) -> None:
        """Test the create_vertex_query helper."""
        # Simple query
        query = AqlLoader.create_vertex_query("users")
        assert "FOR doc IN `users`" in query["query"]
        assert "RETURN {vertices: [doc]}" in query["query"]

        # With filter
        query = AqlLoader.create_vertex_query(
            "users", filter_condition="doc.active == true"
        )
        assert "FILTER doc.active == true" in query["query"]

        # With projection
        query = AqlLoader.create_vertex_query("users", projection=["name", "age"])
        assert "_id: doc._id" in query["query"]
        assert "`name`: doc.`name`" in query["query"]
        assert "`age`: doc.`age`" in query["query"]

    def test_aql_helper_create_edge_query(self) -> None:
        """Test the create_edge_query helper."""
        # Simple query
        query = AqlLoader.create_edge_query("purchases")
        assert "FOR doc IN `purchases`" in query["query"]
        assert "RETURN {edges: [doc]}" in query["query"]

        # With filter
        query = AqlLoader.create_edge_query(
            "purchases", filter_condition="doc.amount > 1"
        )
        assert "FILTER doc.amount > 1" in query["query"]

        # With projection
        query = AqlLoader.create_edge_query("purchases", projection=["amount"])
        assert "_from: doc._from" in query["query"]
        assert "_to: doc._to" in query["query"]
        assert "`amount`: doc.`amount`" in query["query"]

    def test_aql_helper_create_traversal_query(self) -> None:
        """Test the create_traversal_query helper."""
        # Simple traversal
        query = AqlLoader.create_traversal_query(
            start_vertex="@start",
            graph_name="test_graph",
            min_depth=0,
            max_depth=2,
            bind_vars={"start": "users/alice"},
        )
        assert "0..2 OUTBOUND @start GRAPH `test_graph`" in query["query"]
        assert "RETURN {vertices: [v], edges: (e == null ? [] : [e])}" in query["query"]
        assert query["bindVars"]["start"] == "users/alice"

        # With filter and prune
        query = AqlLoader.create_traversal_query(
            start_vertex="'users/alice'",
            graph_name="test_graph",
            min_depth=1,
            max_depth=3,
            direction="ANY",
            prune_condition="v.visited",
            filter_condition="e.weight > 0",
        )
        assert "1..3 ANY 'users/alice' GRAPH `test_graph`" in query["query"]
        assert "PRUNE v.visited" in query["query"]
        assert "FILTER e.weight > 0" in query["query"]

        # Test invalid direction raises error
        with pytest.raises(ValueError, match="direction must be one of"):
            AqlLoader.create_traversal_query(
                start_vertex="@start",
                graph_name="test_graph",
                direction="INVALID",
            )

        # Test negative min_depth raises error
        with pytest.raises(ValueError, match="min_depth must be non-negative"):
            AqlLoader.create_traversal_query(
                start_vertex="@start",
                graph_name="test_graph",
                min_depth=-1,
            )

        # Test negative max_depth raises error
        with pytest.raises(ValueError, match="max_depth must be non-negative"):
            AqlLoader.create_traversal_query(
                start_vertex="@start",
                graph_name="test_graph",
                max_depth=-1,
            )

        # Test max_depth < min_depth raises error
        with pytest.raises(ValueError, match="max_depth.*must be >= min_depth"):
            AqlLoader.create_traversal_query(
                start_vertex="@start",
                graph_name="test_graph",
                min_depth=3,
                max_depth=1,
            )

    def test_aql_empty_queries_raises_error(
        self,
        connection_information: dict[str, str],
    ) -> None:
        """Test that empty queries raise an error."""
        loader = AqlLoader(
            hosts=[connection_information["url"]],
            database="_system",
            username=connection_information["username"],
            password=connection_information["password"],
        )

        with pytest.raises(PhenolError):
            loader.load_to_networkx(queries=[])

        with pytest.raises(PhenolError):
            loader.load_to_networkx(queries=[[]])

        with pytest.raises(PhenolError):
            loader.load_to_numpy(queries=[])

    def test_aql_max_type_errors_passed_to_request(self) -> None:
        """Test that max_type_errors is correctly passed through to the request."""
        loader = AqlLoader(
            hosts=["http://localhost:8529"],
            database="_system",
        )

        # Verify max_type_errors is included when specified
        request = loader._build_request(
            queries=[[{"query": "RETURN 1"}]],
            max_type_errors=5,
        )
        assert request["max_type_errors"] == 5

        # Verify max_type_errors is not included when None
        request_no_limit = loader._build_request(
            queries=[[{"query": "RETURN 1"}]],
        )
        assert "max_type_errors" not in request_no_limit

        # Verify zero is a valid value
        request_zero = loader._build_request(
            queries=[[{"query": "RETURN 1"}]],
            max_type_errors=0,
        )
        assert request_zero["max_type_errors"] == 0

    @pytest.mark.usefixtures("load_aql_test_graph")
    def test_aql_load_with_correct_type_mappings(
        self,
        aql_test_db_name: str,
        connection_information: dict[str, str],
    ) -> None:
        """Test loading with correct type mappings for all supported types."""
        loader = AqlLoader(
            hosts=[connection_information["url"]],
            database=aql_test_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        queries: list[list[AqlQuery]] = [[AqlLoader.create_vertex_query("users")]]

        # Test with correct type mappings:
        # - name: string (actual: "Alice", "Bob", "Charlie")
        # - age: i64 (actual: 30, 25, 35)
        # - active: bool (actual: True, True, False)
        result = loader.load_to_networkx(
            queries=queries,
            vertex_attributes={"name": "string", "age": "i64", "active": "bool"},
            is_directed=True,
            is_multigraph=False,
            load_coo=False,
        )
        node_dict, *_ = result
        assert len(node_dict) == 3

        # Test f64 type with price field
        product_queries: list[list[AqlQuery]] = [
            [AqlLoader.create_vertex_query("products")]
        ]
        result = loader.load_to_networkx(
            queries=product_queries,
            vertex_attributes={"title": "string", "price": "f64"},
            is_directed=True,
            is_multigraph=False,
            load_coo=False,
        )
        node_dict, *_ = result
        assert len(node_dict) == 2  # laptop, phone

        # Test edge_attributes with correct type mappings
        # - amount: f64 (actual: 1.0, 2.0, 1.0)
        full_graph_queries: list[list[AqlQuery]] = [
            [
                AqlLoader.create_vertex_query("users"),
                AqlLoader.create_vertex_query("products"),
            ],
            [AqlLoader.create_edge_query("purchases")],
        ]
        result = loader.load_to_networkx(
            queries=full_graph_queries,
            vertex_attributes={"name": "string", "age": "i64"},
            edge_attributes={"amount": "f64"},
            is_directed=True,
            is_multigraph=False,
        )
        node_dict, adj_dict, src_indices, *_ = result
        assert len(node_dict) == 5  # 3 users + 2 products
        assert len(src_indices) == 3  # 3 purchase edges

    @pytest.mark.usefixtures("load_aql_test_graph")
    def test_aql_load_with_wrong_type_mappings(
        self,
        aql_test_db_name: str,
        connection_information: dict[str, str],
    ) -> None:
        """Test behavior with wrong type mappings.

        When type mappings don't match data, the library silently converts
        to default values (0 for numeric types) rather than raising errors.
        """
        loader = AqlLoader(
            hosts=[connection_information["url"]],
            database=aql_test_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        queries: list[list[AqlQuery]] = [[AqlLoader.create_vertex_query("users")]]

        # Wrong mapping: name is actually a string, not i64
        # The library converts unparseable values to defaults (0 for i64)
        result = loader.load_to_networkx(
            queries=queries,
            vertex_attributes={"name": "i64"},  # Wrong: name is string
            is_directed=True,
            is_multigraph=False,
            load_coo=False,
        )
        node_dict, *_ = result
        # Vertices are loaded, but name values become 0 (default for failed i64 parse)
        assert len(node_dict) == 3
        for vertex_data in node_dict.values():
            assert vertex_data.get("name") == 0  # Default value for failed conversion

        # Test edge_attributes with type mismatch (also specify vertex attributes)
        full_graph_queries: list[list[AqlQuery]] = [
            [
                AqlLoader.create_vertex_query("users"),
                AqlLoader.create_vertex_query("products"),
            ],
            [AqlLoader.create_edge_query("purchases")],
        ]
        result = loader.load_to_networkx(
            queries=full_graph_queries,
            vertex_attributes={"name": "string"},  # Correct vertex mapping
            edge_attributes={"amount": "i64"},  # Wrong: amount is f64, mapping as i64
            is_directed=True,
            is_multigraph=False,
        )
        node_dict, adj_dict, src_indices, *_ = result
        assert len(node_dict) == 5  # 3 users + 2 products
        assert len(src_indices) == 3  # 3 edges loaded

    @pytest.mark.usefixtures("load_aql_test_graph")
    def test_aql_load_with_max_type_errors_limit(
        self,
        aql_test_db_name: str,
        connection_information: dict[str, str],
    ) -> None:
        """Test that max_type_errors parameter is passed through correctly.

        This is a smoke test verifying the parameter doesn't break loading.
        Full type error limit testing depends on the underlying Rust library.
        """
        loader = AqlLoader(
            hosts=[connection_information["url"]],
            database=aql_test_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        queries: list[list[AqlQuery]] = [[AqlLoader.create_vertex_query("users")]]

        # Test load_to_networkx with max_type_errors (no type errors expected)
        result = loader.load_to_networkx(
            queries=queries,
            vertex_attributes={"name": "string", "age": "i64"},
            is_directed=True,
            is_multigraph=False,
            load_coo=False,
            max_type_errors=10,
        )
        node_dict, *_ = result
        assert len(node_dict) == 3  # alice, bob, charlie

        # Test load_to_numpy with max_type_errors
        result_numpy = loader.load_to_numpy(
            queries=queries,
            vertex_attributes={"age": "i64"},
            max_type_errors=10,
        )
        features_by_col, coo_map, col_to_key_to_ind, col_to_ind_to_key = result_numpy
        assert "users" in col_to_key_to_ind
        assert len(col_to_key_to_ind["users"]) == 3

    @pytest.mark.usefixtures("load_aql_test_graph")
    def test_aql_load_to_pyg_data(
        self,
        aql_test_db_name: str,
        connection_information: dict[str, str],
    ) -> None:
        """Test loading into PyG Data format via AQL (homogeneous graph).

        Uses follows edges (users->users) for true homogeneous graph.
        """
        pytest.importorskip("torch")
        pytest.importorskip("torch_geometric")

        loader = AqlLoader(
            hosts=[connection_information["url"]],
            database=aql_test_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        # Use follows edges for homogeneous graph (users->users)
        queries: list[list[AqlQuery]] = [
            [{"query": "FOR v IN users RETURN {vertices: [v]}"}],
            [{"query": "FOR e IN follows RETURN {edges: [e]}"}],
        ]

        # Load with explicit feature mapping
        data, key_to_ind, ind_to_key = loader.load_to_pyg_data(
            queries=queries,
            vertex_attributes={"age": "i64"},
            edge_attributes={"weight": "f64"},
            pyg_feature_mapping={"x": ["age"]},
        )

        # Verify PyG Data structure
        assert hasattr(data, "x")
        assert hasattr(data, "edge_index")
        assert data.x.shape[0] == 3  # 3 users
        assert data.x.shape[1] == 1  # 1 feature (age)
        assert data.edge_index.shape[0] == 2  # COO format (src, dst)
        assert data.edge_index.shape[1] == 2  # 2 follows edges

        # Verify mappings
        assert "users" in key_to_ind
        assert len(key_to_ind["users"]) == 3

    @pytest.mark.usefixtures("load_aql_test_graph")
    def test_aql_load_to_pyg_data_auto_mapping(
        self,
        aql_test_db_name: str,
        connection_information: dict[str, str],
    ) -> None:
        """Test loading into PyG Data format with auto feature mapping.

        Uses follows edges (users->users) for homogeneous graph testing.
        """
        pytest.importorskip("torch")
        pytest.importorskip("torch_geometric")

        loader = AqlLoader(
            hosts=[connection_information["url"]],
            database=aql_test_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        # Use follows edges for homogeneous graph (users->users)
        queries: list[list[AqlQuery]] = [
            [{"query": "FOR v IN users RETURN {vertices: [v]}"}],
            [{"query": "FOR e IN follows RETURN {edges: [e]}"}],
        ]

        # Load without explicit mapping - should auto-stack into 'x'
        data, _, _ = loader.load_to_pyg_data(
            queries=queries,
            vertex_attributes={"age": "i64", "active": "bool"},
            edge_attributes={"weight": "f64"},
        )

        # Both age and active should be in x
        assert hasattr(data, "x")
        assert data.x.shape[0] == 3  # 3 users
        assert data.x.shape[1] == 2  # 2 features (age, active)

    @pytest.mark.usefixtures("load_aql_test_graph")
    def test_aql_load_to_pyg_heterodata(
        self,
        aql_test_db_name: str,
        connection_information: dict[str, str],
    ) -> None:
        """Test loading into PyG HeteroData format via AQL."""
        pytest.importorskip("torch")
        pytest.importorskip("torch_geometric")

        loader = AqlLoader(
            hosts=[connection_information["url"]],
            database=aql_test_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        # Load full heterogeneous graph
        queries: list[list[AqlQuery]] = [
            [
                {"query": "FOR v IN users RETURN {vertices: [v]}"},
                {"query": "FOR v IN products RETURN {vertices: [v]}"},
            ],
            [{"query": "FOR e IN purchases RETURN {edges: [e]}"}],
        ]

        data, key_to_ind, ind_to_key = loader.load_to_pyg_heterodata(
            queries=queries,
            vertex_attributes={"age": "i64", "price": "f64"},
            edge_attributes={"amount": "f64"},
            pyg_feature_mapping={
                "users": {"x": ["age"]},
                "products": {"x": ["price"]},
            },
        )

        # Verify HeteroData structure
        assert "users" in data.node_types
        assert "products" in data.node_types
        assert data["users"].x.shape[0] == 3  # 3 users
        assert data["products"].x.shape[0] == 2  # 2 products
        assert len(data.edge_types) >= 1  # At least purchases edge type

        # Verify mappings
        assert "users" in key_to_ind
        assert "products" in key_to_ind

    def test_aql_load_to_pyg_missing_torch_raises(
        self,
        connection_information: dict[str, str],
    ) -> None:
        """Test that loading to PyG without torch raises ImportError."""

        # Skip this test if torch is installed (we can't uninstall it)
        try:
            import torch  # noqa: F401

            pytest.skip("torch is installed, cannot test missing import")
        except ImportError:
            pass

        loader = AqlLoader(
            hosts=[connection_information["url"]],
            database="_system",
            username=connection_information["username"],
            password=connection_information["password"],
        )

        queries: list[list[AqlQuery]] = [
            [{"query": "FOR v IN test RETURN {vertices: [v]}"}],
            [{"query": "FOR e IN test RETURN {edges: [e]}"}],
        ]

        with pytest.raises(ImportError, match="phenolrs\\[torch\\]"):
            loader.load_to_pyg_data(queries=queries)

        with pytest.raises(ImportError, match="phenolrs\\[torch\\]"):
            loader.load_to_pyg_heterodata(queries=queries)

    @pytest.mark.usefixtures("load_aql_test_graph")
    def test_aql_load_to_pyg_string_attribute_raises(
        self,
        aql_test_db_name: str,
        connection_information: dict[str, str],
    ) -> None:
        """Test that loading string attributes into PyG raises an error.

        PyG requires numeric tensors, so string attributes cannot be converted.
        This should raise a clear error rather than silently failing.
        Note: Depending on Python version, either the string type check or
        the empty data check may trigger first.
        """
        pytest.importorskip("torch")
        pytest.importorskip("torch_geometric")

        loader = AqlLoader(
            hosts=[connection_information["url"]],
            database=aql_test_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        # Use follows edges for homogeneous graph (users->users)
        queries: list[list[AqlQuery]] = [
            [{"query": "FOR v IN users RETURN {vertices: [v]}"}],
            [{"query": "FOR e IN follows RETURN {edges: [e]}"}],
        ]

        # Loading string attribute 'name' into PyG should fail
        # because PyG requires numeric tensors
        # Note: Error may be "string/object type" or "No vertex data" depending
        # on how the Rust backend handles string attributes
        with pytest.raises(PhenolError, match=r"(string/object type|No vertex data)"):
            loader.load_to_pyg_data(
                queries=queries,
                vertex_attributes={"name": "string"},  # String type not supported
                edge_attributes={"weight": "f64"},
                pyg_feature_mapping={"x": ["name"]},
            )

    @pytest.mark.usefixtures("load_aql_test_graph")
    def test_aql_load_to_pyg_heterodata_string_attribute_raises(
        self,
        aql_test_db_name: str,
        connection_information: dict[str, str],
    ) -> None:
        """Test that loading string attributes into PyG HeteroData raises error.

        Note: Depending on Python version, either the string type check or
        the empty data check may trigger first.
        """
        pytest.importorskip("torch")
        pytest.importorskip("torch_geometric")

        loader = AqlLoader(
            hosts=[connection_information["url"]],
            database=aql_test_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        queries: list[list[AqlQuery]] = [
            [{"query": "FOR v IN users RETURN {vertices: [v]}"}],
            [{"query": "FOR e IN purchases RETURN {edges: [e]}"}],
        ]

        # Loading string attribute into PyG HeteroData should also fail
        # Note: Error may be "string/object type", "No vertex data", or
        # "not found" depending on how the Rust backend handles string attrs
        with pytest.raises(
            PhenolError, match=r"(string/object type|No vertex data|not found)"
        ):
            loader.load_to_pyg_heterodata(
                queries=queries,
                vertex_attributes={"name": "string"},
                edge_attributes={"amount": "f64"},
                pyg_feature_mapping={"users": {"x": ["name"]}},
            )
