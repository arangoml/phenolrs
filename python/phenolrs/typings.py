from typing import Any

Graph = dict[str, dict[str, dict[str, Any]]]
DiGraph = dict[str, Graph]
MultiGraph = dict[str, dict[str, dict[int, dict[str, Any]]]]
MultiDiGraph = dict[str, MultiGraph]
