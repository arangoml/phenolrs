from typing import Any, Dict

import arango
import pytest
from arango_datasets import Datasets

connection_config: Dict[str, Any]


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--url", action="store", default="http://localhost:8529")
    parser.addoption("--dbName", action="store", default="abide")
    parser.addoption("--username", action="store", default="root")
    parser.addoption("--password", action="store", default="test")


def pytest_configure(config: pytest.Config) -> None:
    global connection_config
    connection_config = {
        "url": config.getoption("url", default=None),
        "username": config.getoption("username"),
        "password": config.getoption("password"),
        "dbName": config.getoption("dbName"),
    }


@pytest.fixture(scope="session")
def connection_information() -> Dict[str, Any]:
    global connection_config
    return {
        "url": connection_config.get("url"),
        "username": connection_config.get("username"),
        "password": connection_config.get("password"),
        "dbName": connection_config.get("dbName"),
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
