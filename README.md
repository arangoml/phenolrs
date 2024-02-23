# Phenolrs

## Running/building

### Required:
- `python`
- `pytest`
- `rust/cargo`

### Build
Build using the `maturin` python package - can install using `pip install maturin`.

Dev build
`maturin develop`

Release build
`maturin develop -r`

### Tests

#### Rust
`cargo test --no-default-features`

#### Python
** WIP - assumes dataset loaded in local ArangoDB instance. **

##### Requirements
`pip install pytest arango-datasets`

##### Python db setup
```
import arango
from arango_datasets import Datasets

client = arango.ArangoClient("http://localhost:8529")
sys = client.db("_system", password="test")
sys.create_database("abide")

abide_db = client.db("abide", password="test")
dsets = Datasets(abide_db)
dsets.load("ABIDE")
```

##### Run python tests:
`maturin develop && pytest`
