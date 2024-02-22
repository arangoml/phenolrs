from torch_geometric.data import HeteroData

from phenolrs.pyg_loader import PygLoader


def test_phenol():
    result = PygLoader.load_into_pyg_heterodata({}, {}, [])
    assert isinstance(result, HeteroData)
