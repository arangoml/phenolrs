from typing import Any

Json = dict[str, Any]
Graph = dict[str, dict[str, Json]]
DiGraph = dict[str, Graph]
MultiGraph = dict[str, dict[str, dict[int, Json]]]
MultiDiGraph = dict[str, MultiGraph]
