from typing import Any, Dict

import arango
import networkx as nx
import pytest
from adbnx_adapter import ADBNX_Adapter
from arango.database import StandardDatabase
from arango_datasets import Datasets

connection_config: Dict[str, Any] = {}


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--url", action="store", default="http://localhost:8529")
    parser.addoption("--username", action="store", default="root")
    parser.addoption("--password", action="store", default="test")


def pytest_configure(config: pytest.Config) -> None:
    global connection_config
    connection_config = {
        "url": config.getoption("url", default=None),
        "username": config.getoption("username"),
        "password": config.getoption("password"),
    }


@pytest.fixture(scope="session")
def connection_information() -> Dict[str, Any]:
    return {
        "url": connection_config.get("url"),
        "username": connection_config.get("username"),
        "password": connection_config.get("password"),
    }


def get_db(db_name: str, connection_information: Dict[str, Any]) -> StandardDatabase:
    client = arango.ArangoClient(connection_information["url"])
    sys_db = client.db(
        "_system",
        username=connection_information["username"],
        password=connection_information["password"],
    )

    if not sys_db.has_database(db_name):
        sys_db.create_database(db_name)

    return client.db(
        db_name,
        username=connection_information["username"],
        password=connection_information["password"],
    )


@pytest.fixture(scope="module")
def load_abide(abide_db_name: str, connection_information: Dict[str, Any]) -> None:
    db = get_db(abide_db_name, connection_information)

    if not db.has_graph("ABIDE"):
        Datasets(db).load("ABIDE")


@pytest.fixture(scope="module")
def load_imdb(imdb_db_name: str, connection_information: Dict[str, Any]) -> None:
    db = get_db(imdb_db_name, connection_information)

    if not db.has_graph("IMDB_PLATFORM"):
        Datasets(db).load("IMDB_PLATFORM")


@pytest.fixture(scope="module")
def load_dblp(dblp_db_name: str, connection_information: Dict[str, Any]) -> None:
    db = get_db(dblp_db_name, connection_information)

    if not db.has_graph("DBLP"):
        Datasets(db).load("DBLP")


@pytest.fixture(scope="module")
def load_isolated_node(
    isolated_node_db_name: str, connection_information: Dict[str, Any]
) -> None:
    db = get_db(isolated_node_db_name, connection_information)

    if not db.has_graph("ISOLATED_NODE"):
        db.create_graph(
            "ISOLATED_NODE",
            edge_definitions=[
                {
                    "edge_collection": "edge",
                    "from_vertex_collections": ["node"],
                    "to_vertex_collections": ["node"],
                }
            ],
        )

        db.collection("node").insert({"_key": "0"})
        db.collection("node").insert({"_key": "1"})
        db.collection("node").insert({"_key": "2"})  # isolated node

        db.collection("edge").insert({"_from": "node/0", "_to": "node/1"})


@pytest.fixture(scope="module")
def abide_db_name() -> str:
    return "abide"


@pytest.fixture(scope="module")
def imdb_db_name() -> str:
    return "imdb"


@pytest.fixture(scope="module")
def dblp_db_name() -> str:
    return "dblp"


@pytest.fixture(scope="module")
def isolated_node_db_name() -> str:
    return "isolated_node"


@pytest.fixture(scope="module")
def custom_graph_db_name() -> str:
    return "custom_graph"


@pytest.fixture(scope="module")
def load_line_graph(
    custom_graph_db_name: str, connection_information: Dict[str, Any]
) -> None:
    client = arango.ArangoClient(connection_information["url"])
    sys_db = client.db(
        "_system",
        username=connection_information["username"],
        password=connection_information["password"],
    )

    if not sys_db.has_database(custom_graph_db_name):
        sys_db.create_database(custom_graph_db_name)
        custom_graph_db = client.db(
            custom_graph_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        edge_def = [
            {
                "edge_collection": "line_graph_edges",
                "from_vertex_collections": ["line_graph_vertices"],
                "to_vertex_collections": ["line_graph_vertices"],
            }
        ]

        G = nx.Graph()
        G.add_edge(0, 1, boolean_weight=True, int_value=1, float_value=1.1)
        G.add_edge(1, 2, boolean_weight=False, int_value=2, float_value=2.2)
        G.add_edge(2, 3, boolean_weight=True, int_value=3, float_value=3.3)
        G.add_edge(3, 4, boolean_weight=False, int_value=4, float_value=4.4)

        ADBNX_Adapter(custom_graph_db).networkx_to_arangodb(
            custom_graph_db_name, G, edge_def
        )


@pytest.fixture(scope="module")
def load_karate(karate_db_name: str, connection_information: Dict[str, Any]) -> None:
    client = arango.ArangoClient(connection_information["url"])
    sys_db = client.db(
        "_system",
        username=connection_information["username"],
        password=connection_information["password"],
    )

    if not sys_db.has_database(karate_db_name):
        sys_db.create_database(karate_db_name)
        karate_db = client.db(
            karate_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        edge_def = [
            {
                "edge_collection": "knows",
                "from_vertex_collections": ["person"],
                "to_vertex_collections": ["person"],
            }
        ]

        ADBNX_Adapter(karate_db).networkx_to_arangodb(
            karate_db_name, nx.karate_club_graph(), edge_def
        )


@pytest.fixture(scope="module")
def karate_db_name() -> str:
    return "karate"


@pytest.fixture(scope="module")
def load_multigraph(
    multigraph_db_name: str, connection_information: Dict[str, Any]
) -> None:
    client = arango.ArangoClient(connection_information["url"])
    sys_db = client.db(
        "_system",
        username=connection_information["username"],
        password=connection_information["password"],
    )

    if not sys_db.has_database(multigraph_db_name):
        sys_db.create_database(multigraph_db_name)
        multigraph_db = client.db(
            multigraph_db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )

        edge_def = [
            {
                "edge_collection": "to",
                "from_vertex_collections": ["node"],
                "to_vertex_collections": ["node"],
            }
        ]

        G = nx.MultiGraph()
        G.add_edge(0, 1, weight=1)
        G.add_edge(0, 1, weight=2)
        G.add_edge(1, 2, weight=3)
        G.add_edge(2, 3, weight=4)
        G.add_edge(2, 3, weight=7)

        ADBNX_Adapter(multigraph_db).networkx_to_arangodb(
            multigraph_db_name, G, edge_def
        )


@pytest.fixture(scope="module")
def multigraph_db_name() -> str:
    return "multigraph"


@pytest.fixture(scope="module")
def aql_test_db_name() -> str:
    return "aql_test"


@pytest.fixture(scope="module")
def load_aql_test_graph(
    aql_test_db_name: str, connection_information: Dict[str, Any]
) -> None:
    """Create a simple test graph for AQL-based loading tests.

    Creates:
    - users collection: 3 vertices with name (string) and age (int) attributes
    - products collection: 2 vertices with title (string) and price (float)
    - purchases collection: 3 edges (users->products) with amount (float)
    - follows collection: 2 edges (users->users) with weight (float)
    """
    client = arango.ArangoClient(connection_information["url"])
    sys_db = client.db(
        "_system",
        username=connection_information["username"],
        password=connection_information["password"],
    )

    if not sys_db.has_database(aql_test_db_name):
        sys_db.create_database(aql_test_db_name)

    db = client.db(
        aql_test_db_name,
        username=connection_information["username"],
        password=connection_information["password"],
    )

    # Create graph if not exists
    if not db.has_graph("test_graph"):
        db.create_graph(
            "test_graph",
            edge_definitions=[
                {
                    "edge_collection": "purchases",
                    "from_vertex_collections": ["users"],
                    "to_vertex_collections": ["products"],
                },
                {
                    "edge_collection": "follows",
                    "from_vertex_collections": ["users"],
                    "to_vertex_collections": ["users"],
                },
            ],
        )

        # Insert users
        users = db.collection("users")
        users.insert({"_key": "alice", "name": "Alice", "age": 30, "active": True})
        users.insert({"_key": "bob", "name": "Bob", "age": 25, "active": True})
        users.insert({"_key": "charlie", "name": "Charlie", "age": 35, "active": False})

        # Insert products
        products = db.collection("products")
        products.insert({"_key": "laptop", "title": "Laptop", "price": 999.99})
        products.insert({"_key": "phone", "title": "Phone", "price": 599.99})

        # Insert purchase edges
        purchases = db.collection("purchases")
        purchases.insert(
            {"_from": "users/alice", "_to": "products/laptop", "amount": 1.0}
        )
        purchases.insert(
            {"_from": "users/alice", "_to": "products/phone", "amount": 2.0}
        )
        purchases.insert({"_from": "users/bob", "_to": "products/phone", "amount": 1.0})

        # Insert follows edges (users->users for homogeneous graph testing)
        follows = db.collection("follows")
        follows.insert({"_from": "users/alice", "_to": "users/bob", "weight": 0.9})
        follows.insert({"_from": "users/bob", "_to": "users/charlie", "weight": 0.7})
