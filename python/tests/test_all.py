from torch_geometric.data import HeteroData

from phenolrs.pyg_loader import PygLoader


def test_phenol_abide_hetero(connection_information: dict[str, str]) -> None:
    result = PygLoader.load_into_pyg_heterodata(
        connection_information["dbName"],
        [{"name": "Subjects", "fields": ["label", "brain_fmri_features"]}],
        [{"name": "medical_affinity_graph"}],
        [connection_information["url"]],
        username=connection_information["username"],
        password=connection_information["password"],
    )
    assert isinstance(result, HeteroData)
    assert result["Subjects"]["brain_fmri_features"].shape == (871, 2000)
