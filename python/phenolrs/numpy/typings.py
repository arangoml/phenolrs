from typing import Tuple

import numpy as np
import numpy.typing as npt

EdgeType = Tuple[str, str, str]

ArangoCollectionToNodeFeatures = dict[str, dict[str, npt.NDArray[np.float64]]]
COOByEdgeType = dict[EdgeType, npt.NDArray[np.float64]]
ArangoCollectionToArangoKeyToIndex = dict[str, dict[str, int]]
ArangoCollectionToIndexToArangoKey = dict[str, dict[int, str]]
ArangoCollectionSourceToOutput = dict[str, dict[str, str]]
