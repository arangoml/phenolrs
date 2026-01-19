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

# AQL-based graph loading types
class _AqlQueryRequired(typing.TypedDict):
    """Required fields for AqlQuery."""

    query: str  # The AQL query string

class AqlQuery(_AqlQueryRequired, total=False):
    """An AQL query with optional bind variables."""

    bindVars: dict[str, typing.Any]  # Optional: bind variables for the query

AttributeSpec = dict[str, str]  # {"attr_name": "type_name"}
# OR list of {"name": str, "type": str}

AqlDataLoadRequest = typing.TypedDict(
    "AqlDataLoadRequest",
    {
        "database_config": dict[str, typing.Any],
        "batch_size": int,
        "vertex_attributes": AttributeSpec | list[dict[str, str]],
        "edge_attributes": AttributeSpec | list[dict[str, str]],
        "queries": list[list[AqlQuery]],
    },
    total=False,
)

def graph_aql_to_numpy_format(request: AqlDataLoadRequest) -> typing.Tuple[
    ArangoCollectionToNodeFeatures,
    COOByEdgeType,
    ArangoCollectionToArangoKeyToIndex,
    ArangoCollectionToIndexToArangoKey,
]: ...
def graph_aql_to_networkx_format(
    request: AqlDataLoadRequest, graph_config: dict[str, typing.Any]
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
