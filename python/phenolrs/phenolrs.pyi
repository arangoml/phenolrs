import typing

import numpy as np
import numpy.typing as npt

from .networkx.typings import DiGraphAdj, GraphAdj, MultiDiGraphAdj, MultiGraphAdj

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
    GraphAdj | DiGraphAdj | MultiGraphAdj | MultiDiGraphAdj,
    npt.NDArray[np.int64],
    npt.NDArray[np.int64],
    npt.NDArray[np.int64],
    dict[str, int],
]: ...

class PhenolError(Exception): ...
