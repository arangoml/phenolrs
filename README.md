# Phenolrs

A high-performance graph data loader for ArangoDB, written in Rust with Python bindings. Phenolrs efficiently loads graph data from ArangoDB into popular Python graph libraries including NumPy, NetworkX, and PyTorch Geometric (PyG).

## Table of Contents
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Loaders](#loaders)
  - [NumpyLoader](#numpyloader)
  - [NetworkXLoader](#networkxloader)
  - [PygLoader](#pygloader)
  - [AqlLoader](#aqlloader)
- [Development](#development)
- [Tests](#tests)

## Installation

```bash
pip install phenolrs

# For PyTorch Geometric support
pip install phenolrs[torch]
```

## Quick Start

All loaders require connection information to your ArangoDB instance:

```python
from phenolrs.numpy import NumpyLoader
from phenolrs.networkx import NetworkXLoader
from phenolrs.pyg import PygLoader

# Connection parameters
database = "your_database"
hosts = ["http://localhost:8529"]
username = "root"
password = "your_password"

# Define your graph structure
metagraph = {
    "vertexCollections": {
        "Users": {"x": "features", "y": "label"}
    },
    "edgeCollections": {
        "Follows": {}
    }
}
```

## Loaders

### NumpyLoader

Load graph data into NumPy arrays for numerical computing and custom processing.

#### Basic Usage

```python
from phenolrs.numpy import NumpyLoader

# Load graph data
(
    features_by_col,
    coo_map,
    col_to_adb_key_to_ind,
    col_to_ind_to_adb_key,
    vertex_cols_source_to_output,
) = NumpyLoader.load_graph_to_numpy(
    database="abide",
    metagraph={
        "vertexCollections": {
            "Subjects": {"x": "brain_fmri_features"}
        },
        "edgeCollections": {
            "medical_affinity_graph": {}
        }
    },
    hosts=["http://localhost:8529"],
    username="root",
    password="password",
    parallelism=8,  # Optional: number of parallel workers
    batch_size=100000,  # Optional: batch size for loading
)

# Access node features
node_features = features_by_col["Subjects"]["brain_fmri_features"]
print(f"Node features shape: {node_features.shape}")  # e.g., (871, 2000)

# Access edge indices in COO format
edge_key = ("medical_affinity_graph", "Subjects", "Subjects")
edge_indices = coo_map[edge_key]
print(f"Edge indices shape: {edge_indices.shape}")  # e.g., (2, 606770)

# Map between ArangoDB keys and indices
adb_key = "Subjects/123"
node_index = col_to_adb_key_to_ind["Subjects"][adb_key]
```

#### Return Values

- `features_by_col`: Dictionary mapping collection names to their features
  - Structure: `{collection_name: {feature_name: numpy_array}}`
- `coo_map`: Dictionary mapping edge types to COO format edge indices
  - Structure: `{(edge_collection, from_collection, to_collection): numpy_array}`
- `col_to_adb_key_to_ind`: Maps ArangoDB keys to integer indices
- `col_to_ind_to_adb_key`: Maps integer indices back to ArangoDB keys
- `vertex_cols_source_to_output`: Maps source field names to output names

#### Loading Vertices Only

```python
# Load only vertex data without edges
(
    features_by_col,
    coo_map,
    col_to_adb_key_to_ind,
    col_to_ind_to_adb_key,
    vertex_cols_source_to_output,
) = NumpyLoader.load_graph_to_numpy(
    database="abide",
    metagraph={
        "vertexCollections": {
            "Subjects": {"x": "brain_fmri_features"}
        }
        # No edgeCollections specified
    },
    hosts=["http://localhost:8529"],
    username="root",
    password="password",
)

# coo_map will be empty
assert len(coo_map) == 0
```

### NetworkXLoader

Load graph data into NetworkX-compatible formats for graph analysis and algorithms.

#### Basic Usage

```python
from phenolrs.networkx import NetworkXLoader

# Load a MultiDiGraph
(
    node_dict,
    adj_dict,
    src_indices,
    dst_indices,
    edge_indices,
    vertex_ids_to_indices,
    edge_values,
) = NetworkXLoader.load_into_networkx(
    database="karate",
    metagraph={
        "vertexCollections": {
            "person": set()  # Load all vertex attributes
        },
        "edgeCollections": {
            "knows": set()  # Load all edge attributes
        }
    },
    hosts=["http://localhost:8529"],
    username="root",
    password="password",
    is_directed=True,
    is_multigraph=True,
)

# Access node data
node_id = "person/1"
node_attributes = node_dict[node_id]
print(f"Node attributes: {node_attributes}")  # e.g., {'_id': '...', 'club': 'Mr. Hi'}

# Access adjacency information (for directed multigraph)
successors = adj_dict["succ"]
predecessors = adj_dict["pred"]

# Navigate edges
to_node = "person/2"
edges_between = successors[node_id][to_node]  # Dict of edges (by index)
```

#### Graph Type Options

```python
# Undirected Graph
NetworkXLoader.load_into_networkx(
    database="karate",
    metagraph=metagraph,
    hosts=hosts,
    username=username,
    password=password,
    is_directed=False,
    is_multigraph=False,
)

# Directed Graph (DiGraph)
NetworkXLoader.load_into_networkx(
    database="karate",
    metagraph=metagraph,
    hosts=hosts,
    username=username,
    password=password,
    is_directed=True,
    is_multigraph=False,
)

# MultiGraph (undirected with multiple edges)
NetworkXLoader.load_into_networkx(
    database="karate",
    metagraph=metagraph,
    hosts=hosts,
    username=username,
    password=password,
    is_directed=False,
    is_multigraph=True,
)

# MultiDiGraph (directed with multiple edges)
NetworkXLoader.load_into_networkx(
    database="karate",
    metagraph=metagraph,
    hosts=hosts,
    username=username,
    password=password,
    is_directed=True,
    is_multigraph=True,
)
```

#### Loading Specific Attributes

```python
# Load only specific vertex and edge attributes
(
    node_dict,
    adj_dict,
    src_indices,
    dst_indices,
    edge_indices,
    vertex_ids_to_indices,
    edge_values,
) = NetworkXLoader.load_into_networkx(
    database="imdb",
    metagraph={
        "vertexCollections": {
            "MOVIE": {"title", "release_date"},
            "USER": {"occupation"}
        },
        "edgeCollections": {
            "VIEWS": {"timestamp"}
        }
    },
    hosts=hosts,
    username=username,
    password=password,
    load_all_vertex_attributes=False,  # Only load specified attributes
    load_all_edge_attributes=False,
    is_directed=True,
    is_multigraph=True,
)

# Access specific attributes
movie_node = node_dict["MOVIE/1"]
print(movie_node["title"])  # "Toy Story (1995)"
print(movie_node["release_date"])  # "01-Jan-1995"
```

#### Edge Symmetrization

```python
# Symmetrize edges in directed graph (add reverse edges)
NetworkXLoader.load_into_networkx(
    database="karate",
    metagraph=metagraph,
    hosts=hosts,
    username=username,
    password=password,
    is_directed=True,
    symmetrize_edges_if_directed=True,  # Creates bidirectional edges
)
```

#### COO Format and Edge Values

```python
# Load with COO format and extract numeric edge attributes
(
    node_dict,
    adj_dict,
    src_indices,
    dst_indices,
    edge_indices,
    vertex_ids_to_indices,
    edge_values,
) = NetworkXLoader.load_into_networkx(
    database="karate",
    metagraph={
        "vertexCollections": {"person": {"club"}},
        "edgeCollections": {"knows": {"weight"}}
    },
    hosts=hosts,
    username=username,
    password=password,
    load_coo=True,
    load_all_vertex_attributes=False,
    load_all_edge_attributes=False,
)

# Access edge values (numeric attributes only)
weights = edge_values["weight"]  # List of floats
print(f"Number of edges: {len(weights)}")
```

#### Return Values

- `node_dict`: Dictionary mapping node IDs to their attributes
- `adj_dict`: Adjacency dictionary structure (format depends on graph type)
  - For directed graphs: `{"succ": {...}, "pred": {...}}`
  - For undirected graphs: `{node_id: {neighbor_id: edge_data}}`
- `src_indices`: NumPy array of source node indices (COO format)
- `dst_indices`: NumPy array of destination node indices (COO format)
- `edge_indices`: NumPy array of edge indices for multigraphs
- `vertex_ids_to_indices`: Mapping from ArangoDB IDs to integer indices
- `edge_values`: Dictionary of numeric edge attribute lists

### PygLoader

Load graph data into PyTorch Geometric `Data` or `HeteroData` objects for GNN training.

#### Installation

```bash
pip install phenolrs[torch]
```

#### Homogeneous Graphs (Data)

```python
from phenolrs.pyg import PygLoader

# Load a single node type and edge type
data, col_to_adb_key_to_ind, col_to_ind_to_adb_key = PygLoader.load_into_pyg_data(
    database="abide",
    metagraph={
        "vertexCollections": {
            "Subjects": {"x": "brain_fmri_features", "y": "label"}
        },
        "edgeCollections": {
            "medical_affinity_graph": {}
        }
    },
    hosts=["http://localhost:8529"],
    username="root",
    password="password",
)

# Access PyG Data object
print(f"Node features: {data.x.shape}")  # torch.Size([871, 2000])
print(f"Node labels: {data.y.shape}")    # torch.Size([871, 1])
print(f"Edge indices: {data.edge_index.shape}")  # torch.Size([2, 606770])

# Use with PyTorch Geometric
from torch_geometric.nn import GCNConv

class GNN(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.conv1 = GCNConv(data.x.shape[1], 64)
        self.conv2 = GCNConv(64, data.y.shape[1])
    
    def forward(self, data):
        x, edge_index = data.x, data.edge_index
        x = self.conv1(x, edge_index).relu()
        x = self.conv2(x, edge_index)
        return x
```

#### Alternative Feature Specification

```python
# Both formats are supported:
metagraph1 = {
    "vertexCollections": {
        "Subjects": {"x": "brain_fmri_features", "y": "label"}
    },
    "edgeCollections": {"medical_affinity_graph": {}}
}

metagraph2 = {
    "vertexCollections": {
        "Subjects": {"x": {"brain_fmri_features": None}, "y": "label"}
    },
    "edgeCollections": {"medical_affinity_graph": {}}
}

# Both produce the same result
```

#### Heterogeneous Graphs (HeteroData)

```python
from phenolrs.pyg import PygLoader

# Load multiple node and edge types
data, col_to_adb_key_to_ind, col_to_ind_to_adb_key = PygLoader.load_into_pyg_heterodata(
    database="imdb",
    metagraph={
        "vertexCollections": {
            "MOVIE": {"x": "features", "y": "should_recommend"},
            "USER": {"x": "features"}
        },
        "edgeCollections": {
            "VIEWS": {}
        }
    },
    hosts=["http://localhost:8529"],
    username="root",
    password="password",
)

# Access HeteroData object
print(data.node_types)  # ['MOVIE', 'USER']
print(data.edge_types)  # [('USER', 'VIEWS', 'MOVIE')]

# Access node features by type
print(f"Movie features: {data['MOVIE'].x.shape}")  # torch.Size([1682, 403])
print(f"Movie labels: {data['MOVIE'].y.shape}")    # torch.Size([1682, 1])
print(f"User features: {data['USER'].x.shape}")    # torch.Size([943, 385])

# Access edges by type
edge_type = ('USER', 'VIEWS', 'MOVIE')
print(f"Edge indices: {data[edge_type].edge_index.shape}")  # torch.Size([2, 100000])

# Use with PyTorch Geometric
from torch_geometric.nn import HeteroConv, GCNConv, Linear

class HeteroGNN(torch.nn.Module):
    def __init__(self, hidden_channels):
        super().__init__()
        self.conv1 = HeteroConv({
            ('USER', 'VIEWS', 'MOVIE'): GCNConv(-1, hidden_channels),
        })
        self.lin = Linear(hidden_channels, 1)
    
    def forward(self, x_dict, edge_index_dict):
        x_dict = self.conv1(x_dict, edge_index_dict)
        return self.lin(x_dict['MOVIE'])
```

#### Multi-type Heterogeneous Graph

```python
# Complex heterogeneous graph with multiple edge types
data, col_to_adb_key_to_ind, col_to_ind_to_adb_key = PygLoader.load_into_pyg_heterodata(
    database="dblp",
    metagraph={
        "vertexCollections": {
            "author": {"x": "x"},
            "paper": {"x": "x"},
            "term": {"x": "x"},
            "conference": {}  # No features
        },
        "edgeCollections": {
            "to": {}  # Single edge collection for all edge types
        }
    },
    hosts=["http://localhost:8529"],
    username="root",
    password="password",
)

print(data.node_types)  # ['author', 'paper', 'term']
print(data.edge_types)  # [('term', 'to', 'paper'), ('author', 'to', 'paper'), ...]

# Access features
print(f"Author features: {data['author'].x.shape}")  # torch.Size([4057, 334])
print(f"Paper features: {data['paper'].x.shape}")    # torch.Size([14328, 4231])
print(f"Term features: {data['term'].x.shape}")      # torch.Size([7723, 50])
```

### AqlLoader

Load graph data using custom AQL queries for maximum flexibility. Unlike the metagraph-based loaders, AqlLoader gives you full control over which data to extract using ArangoDB's powerful query language.

**Why use AqlLoader?**
- Use ArangoDB indexes for efficient data retrieval
- Filter vertices and edges with arbitrary AQL conditions
- Execute graph traversals to extract connected subgraphs
- Combine multiple queries with controlled execution order
- Access the full power of AQL for complex data extraction patterns

#### Basic Usage

```python
from phenolrs.aql import AqlLoader

# Initialize the loader
loader = AqlLoader(
    hosts=["http://localhost:8529"],
    database="mydb",
    username="root",
    password="password",
)

# Define queries - each returns {vertices: [...], edges: [...]}
queries = [
    # First group: load vertices (queries run in parallel)
    [
        {"query": "FOR v IN users RETURN {vertices: [v]}"},
        {"query": "FOR v IN products RETURN {vertices: [v]}"},
    ],
    # Second group: load edges (runs after first group completes)
    [
        {"query": "FOR e IN purchases RETURN {edges: [e]}"},
    ],
]

# Load into NetworkX format
result = loader.load_to_networkx(
    queries=queries,
    is_directed=True,
    is_multigraph=False,
)

node_dict, adj_dict, src_indices, dst_indices, *_ = result
print(f"Loaded {len(node_dict)} nodes")
```

#### Loading with Attribute Types

When you specify attribute types, the loader validates and converts values:

```python
# Load with typed attributes
result = loader.load_to_networkx(
    queries=queries,
    vertex_attributes={"name": "string", "age": "i64", "score": "f64"},
    edge_attributes={"weight": "f64", "active": "bool"},
    is_directed=True,
    is_multigraph=False,
)

# Supported types: "string", "i64", "f64", "bool"
```

#### Using Helper Methods

AqlLoader provides helper methods to build common query patterns safely:

```python
from phenolrs.aql import AqlLoader

# Create vertex query with filter
vertex_query = AqlLoader.create_vertex_query(
    collection="users",
    filter_condition="doc.active == @active",
    projection=["name", "age"],  # Optional: select specific fields
    bind_vars={"active": True},
)

# Create edge query
edge_query = AqlLoader.create_edge_query(
    collection="purchases",
    filter_condition="doc.amount > @minAmount",
    bind_vars={"minAmount": 100},
)

# Create graph traversal query
traversal_query = AqlLoader.create_traversal_query(
    start_vertex="@start",  # Use bind variable for safety
    graph_name="commerce_graph",
    min_depth=0,
    max_depth=2,
    direction="OUTBOUND",
    bind_vars={"start": "users/alice"},
)

# Execute queries
queries = [
    [vertex_query],
    [edge_query],
]

result = loader.load_to_networkx(
    queries=queries,
    vertex_attributes={"name": "string", "age": "i64"},
    edge_attributes={"amount": "f64"},
)
```

#### Loading to NumPy Format

```python
# Load into NumPy format for numerical computing
(
    features_by_col,
    coo_map,
    col_to_key_to_ind,
    col_to_ind_to_key,
) = loader.load_to_numpy(
    queries=queries,
    vertex_attributes={"age": "i64", "score": "f64"},
    edge_attributes={"weight": "f64"},
)

# Access features by collection
user_ages = features_by_col["users"]["age"]
print(f"User ages shape: {user_ages.shape}")

# Access edge indices in COO format
for edge_key, indices in coo_map.items():
    print(f"Edge type {edge_key}: {indices.shape}")
```

#### Query Structure

Queries are organized into groups for execution control:
- **Outer list**: Groups processed **sequentially** (one after another)
- **Inner list**: Queries within a group processed **in parallel**

```python
queries = [
    # Group 1: These run in parallel, must complete before Group 2
    [
        {"query": "FOR v IN users RETURN {vertices: [v]}"},
        {"query": "FOR v IN products RETURN {vertices: [v]}"},
    ],
    # Group 2: Runs after Group 1 completes
    [
        {"query": "FOR e IN purchases RETURN {edges: [e]}"},
    ],
]
```

Each query must return documents with `vertices` and/or `edges` arrays:
```aql
// Return vertices
FOR v IN users RETURN {vertices: [v]}

// Return edges
FOR e IN follows RETURN {edges: [e]}

// Return both (e.g., from traversal)
FOR v, e IN 1..2 OUTBOUND 'users/alice' GRAPH 'social'
RETURN {vertices: [v], edges: [e]}
```

## Common Parameters

All loaders support these optional parameters:

- `user_jwt`: JWT token for authentication (alternative to username/password)
- `tls_cert`: TLS certificate for secure connections
- `parallelism`: Number of parallel workers (default: 8)
- `batch_size`: Batch size for loading data (default: 100000)

```python
# Example with optional parameters
result = NumpyLoader.load_graph_to_numpy(
    database="mydb",
    metagraph=metagraph,
    hosts=["https://myserver.arangodb.cloud:8529"],
    user_jwt="your_jwt_token",
    tls_cert="/path/to/cert.pem",
    parallelism=16,
    batch_size=50000,
)
```

## Metagraph Format

The metagraph defines which collections and attributes to load:

```python
metagraph = {
    "vertexCollections": {
        "CollectionName1": {
            "output_name1": "source_field1",
            "output_name2": "source_field2",
        },
        "CollectionName2": {}  # Empty dict loads no attributes (NetworkX)
                               # or all attributes (NetworkX with load_all_vertex_attributes=True)
    },
    "edgeCollections": {
        "EdgeCollection1": {"attribute1", "attribute2"},  # NetworkX: set of attributes
        "EdgeCollection2": {},  # PyG/NumPy: dict/empty dict
    }
}
```

## Development

### Required:
- `python`
- `pytest`
- `rust/cargo`

### Build
Build using the `maturin` python package - can install using `pip install maturin`.

Dev build
```bash
maturin develop
```

Release build
```bash
maturin develop -r
```

## Tests

### Rust
```bash
cargo test --no-default-features
```

### Python

#### Requirements
```bash
pip install pytest arango-datasets
```

#### Python db setup
```python
import arango
from arango_datasets import Datasets

client = arango.ArangoClient("http://localhost:8529")
sys = client.db("_system", password="test")
sys.create_database("abide")

abide_db = client.db("abide", password="test")
dsets = Datasets(abide_db)
dsets.load("ABIDE")
```

#### Run python tests:
```bash
maturin develop && pytest
```
