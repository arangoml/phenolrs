from typing import Any

import numpy as np
import numpy.typing as npt

Json = dict[str, Any]
NodeDict = dict[str, Json]
GraphAdjDict = dict[str, dict[str, Json]]
DiGraphAdjDict = dict[str, GraphAdjDict]
MultiGraphAdjDict = dict[str, dict[str, dict[int, Json]]]
MultiDiGraphAdjDict = dict[str, MultiGraphAdjDict]
EdgeValuesDict = dict[str, list[int | float]]

SrcIndices = npt.NDArray[np.int64]
DstIndices = npt.NDArray[np.int64]
EdgeIndices = npt.NDArray[np.int64]
ArangoIDtoIndex = dict[str, int]
