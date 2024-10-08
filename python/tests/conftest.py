from typing import Any, Dict

import arango
import networkx as nx
import pytest
from adbnx_adapter import ADBNX_Adapter
from arango_datasets import Datasets

connection_config: Dict[str, Any]


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
    global connection_config
    return {
        "url": connection_config.get("url"),
        "username": connection_config.get("username"),
        "password": connection_config.get("password"),
    }


def load_dataset(
    dataset: str, db_name: str, connection_information: Dict[str, Any]
) -> None:
    client = arango.ArangoClient(connection_information["url"])
    sys_db = client.db(
        "_system",
        username=connection_information["username"],
        password=connection_information["password"],
    )

    if not sys_db.has_database(db_name):
        sys_db.create_database(db_name)
        db = client.db(
            db_name,
            username=connection_information["username"],
            password=connection_information["password"],
        )
        dsets = Datasets(db)
        dsets.load(dataset)


@pytest.fixture(scope="module")
def load_abide(abide_db_name: str, connection_information: Dict[str, Any]) -> None:
    load_dataset("ABIDE", abide_db_name, connection_information)


@pytest.fixture(scope="module")
def load_imdb(imdb_db_name: str, connection_information: Dict[str, Any]) -> None:
    load_dataset("IMDB_PLATFORM", imdb_db_name, connection_information)


@pytest.fixture(scope="module")
def load_dblp(dblp_db_name: str, connection_information: Dict[str, Any]) -> None:
    load_dataset("DBLP", dblp_db_name, connection_information)


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
