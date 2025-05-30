import typing

from .networkx.typings import (
    ArangoIDtoIndex,
    DiGraphAdjDict,
    DstIndices,
    EdgeIndices,
    EdgeValuesDict,
    GraphAdjDict,
    MultiDiGraphAdjDict,
    MultiGraphAdjDict,
    NodeDict,
    SrcIndices,
)
from .numpy.typings import (
    ArangoCollectionToArangoKeyToIndex,
    ArangoCollectionToIndexToArangoKey,
    ArangoCollectionToNodeFeatures,
    COOByEdgeType,
)

def graph_to_numpy_format(request: dict[str, typing.Any]) -> typing.Tuple[
    ArangoCollectionToNodeFeatures,
    COOByEdgeType,
    ArangoCollectionToArangoKeyToIndex,
    ArangoCollectionToIndexToArangoKey,
]: ...
def graph_to_networkx_format(
    request: dict[str, typing.Any], graph_config: dict[str, typing.Any]
) -> typing.Tuple[
    NodeDict,
    GraphAdjDict | DiGraphAdjDict | MultiGraphAdjDict | MultiDiGraphAdjDict,
    SrcIndices,
    DstIndices,
    EdgeIndices,
    ArangoIDtoIndex,
    EdgeValuesDict,
]: ...

class PhenolError(Exception): ...
