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

    def test_aql_load_vertices_only(
        self,
        load_aql_test_graph: None,
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

    def test_aql_load_vertices_and_edges_sequential(
        self,
        load_aql_test_graph: None,
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

    def test_aql_load_with_filter(
        self,
        load_aql_test_graph: None,
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
        assert "users/charlie" not in node_dict

    def test_aql_load_with_bind_vars(
        self,
        load_aql_test_graph: None,
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
        assert "users/bob" not in node_dict

    def test_aql_load_graph_traversal(
        self,
        load_aql_test_graph: None,
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

    def test_aql_load_to_numpy(
        self,
        load_aql_test_graph: None,
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

    def test_aql_helper_create_vertex_query(self) -> None:
        """Test the create_vertex_query helper."""
        # Simple query
        query = AqlLoader.create_vertex_query("users")
        assert "FOR doc IN users" in query["query"]
        assert "RETURN {vertices: [doc]}" in query["query"]

        # With filter
        query = AqlLoader.create_vertex_query(
            "users", filter_condition="doc.active == true"
        )
        assert "FILTER doc.active == true" in query["query"]

        # With projection
        query = AqlLoader.create_vertex_query("users", projection=["name", "age"])
        assert "_id: doc._id" in query["query"]
        assert "name: doc.name" in query["query"]
        assert "age: doc.age" in query["query"]

    def test_aql_helper_create_edge_query(self) -> None:
        """Test the create_edge_query helper."""
        # Simple query
        query = AqlLoader.create_edge_query("purchases")
        assert "FOR doc IN purchases" in query["query"]
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
        assert "amount: doc.amount" in query["query"]

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
        assert "0..2 OUTBOUND @start GRAPH 'test_graph'" in query["query"]
        assert "RETURN {vertices: [v], edges: [e]}" in query["query"]
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
        assert "1..3 ANY 'users/alice' GRAPH 'test_graph'" in query["query"]
        assert "PRUNE v.visited" in query["query"]
        assert "FILTER e.weight > 0" in query["query"]

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
