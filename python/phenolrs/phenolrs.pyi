import typing

import numpy as np
import numpy.typing as npt

def graph_to_json_format(request: dict[str, typing.Any]) -> typing.Tuple[
    dict[str, dict[str, typing.Any]],
    dict[str, dict[str, dict[str, typing.Any]]],
    np.ndarray,
    np.ndarray,
    dict[str, int],
]: ...

class PhenolError(Exception): ...
