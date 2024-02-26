from typing import Any, Dict

import pytest

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
