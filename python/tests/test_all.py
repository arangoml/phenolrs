from torch_geometric.data import HeteroData

from phenolrs.pyg_loader import PygLoader


def test_phenol_abide_hetero():
    result = PygLoader.load_into_pyg_heterodata(
        "abide",
        [{"name": "Subjects", "fields": ["label", "brain_fmri_features"]}],
        [{"name": "medical_affinity_graph"}],
        ["http://localhost:8529"],
    )
    assert isinstance(result, HeteroData)
