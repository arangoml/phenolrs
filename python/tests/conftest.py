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


@pytest.fixture(scope="module")
def load_abide(connection_information: Dict[str, Any]) -> None:
    client = arango.ArangoClient(connection_information["url"])
    sys_db = client.db(
        "_system",
        username=connection_information["username"],
        password=connection_information["password"],
    )

    if not sys_db.has_database("abide"):
        sys_db.delete_database("abide", ignore_missing=True)
        sys_db.create_database("abide")
        abide_db = client.db(
            "abide",
            username=connection_information["username"],
            password=connection_information["password"],
        )
        dsets = Datasets(abide_db)
        dsets.load("ABIDE")


@pytest.fixture(scope="module")
def load_karate(connection_information: Dict[str, Any]) -> None:
    client = arango.ArangoClient(connection_information["url"])
    sys_db = client.db(
        "_system",
        username=connection_information["username"],
        password=connection_information["password"],
    )

    if not sys_db.has_database("karate"):
        sys_db.delete_database("karate", ignore_missing=True)
        sys_db.create_database("karate")
        karate_db = client.db(
            "karate",
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
            "karate", nx.karate_club_graph(), edge_def
        )


@pytest.fixture(scope="module")
def load_multigraph(connection_information: Dict[str, Any]) -> None:
    client = arango.ArangoClient(connection_information["url"])
    sys_db = client.db(
        "_system",
        username=connection_information["username"],
        password=connection_information["password"],
    )

    if not sys_db.has_database("multigraph"):
        sys_db.delete_database("multigraph", ignore_missing=True)
        sys_db.create_database("multigraph")
        multigraph_db = client.db(
            "multigraph",
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

        ADBNX_Adapter(multigraph_db).networkx_to_arangodb("multigraph", G, edge_def)
