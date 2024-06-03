import typing

import numpy as np
import numpy.typing as npt

def graph_to_numpy_format(request: dict[str, typing.Any]) -> typing.Tuple[
    dict[str, dict[str, npt.NDArray[np.float64]]],
    dict[typing.Tuple[str, str, str], npt.NDArray[np.float64]],
    dict[str, dict[str, int]],
    dict[str, dict[int, str]],
]: ...

class PhenolError(Exception): ...
