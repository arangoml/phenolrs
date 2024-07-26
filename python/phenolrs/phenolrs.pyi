import typing

import numpy as np
import numpy.typing as npt

from .typings import DiGraph, Graph, MultiDiGraph, MultiGraph

def graph_to_numpy_format(request: dict[str, typing.Any]) -> typing.Tuple[
    dict[str, dict[str, npt.NDArray[np.float64]]],
    dict[typing.Tuple[str, str, str], npt.NDArray[np.float64]],
    dict[str, dict[str, int]],
    dict[str, dict[int, str]],
]: ...
def graph_to_networkx_format(
    request: dict[str, typing.Any], graph_config: dict[str, typing.Any]
) -> typing.Tuple[
    dict[str, dict[str, typing.Any]],
    Graph | DiGraph | MultiGraph | MultiDiGraph,
    npt.NDArray[np.int64],
    npt.NDArray[np.int64],
    dict[str, int],
]: ...

class PhenolError(Exception): ...
