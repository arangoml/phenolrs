import typing

import numpy as np
import numpy.typing as npt

def graph_to_coo_format(
    request: dict[str, typing.Any]
) -> dict[typing.Tuple[str, str, str], npt.NDArray[np.float64]]: ...

class PhenolError(Exception): ...
