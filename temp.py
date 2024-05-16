from python.phenolrs.pyg_loader import PygLoader

res = PygLoader.load_into_pyg_heterodata(
    "abide",
    {
        "vertexCollections": {"Subjects": {"x": "brain_fmri_features"}},
        "edgeCollections": {"medical_affinity_graph": {'a': 'x'}},
    },
    ["http://localhost:8529"],
    None,
    "root",
    "passwd",
    batch_size=1000000,
    parallelism=10,
)
