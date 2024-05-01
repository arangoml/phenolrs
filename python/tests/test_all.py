from phenolrs.coo_loader import CooLoader


def test_phenol_abide_coo(
    load_abide: None, connection_information: dict[str, str]
) -> None:
    result = CooLoader.load_coo(
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

    e_type = ("medical_affinity_graph", "Subjects", "Subjects")

    assert isinstance(result, dict)
    assert len(result) == 1
    assert e_type in result
    assert result[e_type].shape == (2, 606770)
