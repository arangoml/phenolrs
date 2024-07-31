from typing import Any

Json = dict[str, Any]
GraphAdj = dict[str, dict[str, Json]]
DiGraphAdj = dict[str, GraphAdj]
MultiGraphAdj = dict[str, dict[str, dict[int, Json]]]
MultiDiGraphAdj = dict[str, MultiGraphAdj]
