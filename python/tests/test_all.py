import numpy

from phenolrs.graph_loader import GraphLoader


def test_phenol_abide_hetero(
    load_abide: None, connection_information: dict[str, str]
) -> None:
    result = GraphLoader.load(
        connection_information["dbName"],
        {
            "vertexCollections": {"Subjects": {}},
            "edgeCollections": {"medical_affinity_graph": {}},
        },
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
    )
    assert isinstance(result, tuple)
    node_dict, adj_dict, src_indices, dst_indices, vertex_ids_to_indices = result  # noqa

    assert isinstance(node_dict, dict)
    assert isinstance(adj_dict, dict)
    assert isinstance(src_indices, numpy.ndarray)
    assert isinstance(dst_indices, numpy.ndarray)
    assert isinstance(vertex_ids_to_indices, dict)
    assert len(node_dict) == len(vertex_ids_to_indices)
    assert len(src_indices) == len(dst_indices)
