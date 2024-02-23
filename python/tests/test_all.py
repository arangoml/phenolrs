from torch_geometric.data import HeteroData

from phenolrs.pyg_loader import PygLoader


def test_phenol_abide_hetero():
    result = PygLoader.load_into_pyg_heterodata(
        "abide",
        [{"name": "Subjects", "fields": ["label", "brain_fmri_features"]}],
        [{"name": "medical_affinity_graph"}],
        ["http://localhost:8529"],
        username="root",
        password="test",
    )
    assert isinstance(result, HeteroData)
    assert result["Subjects"]["brain_fmri_features"].shape == (871, 2000)
