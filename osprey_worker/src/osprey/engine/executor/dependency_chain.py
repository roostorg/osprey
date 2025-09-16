from dataclasses import dataclass
from typing import TYPE_CHECKING, Tuple

from osprey.engine.utils.types import add_slots

if TYPE_CHECKING:
    from osprey.engine.ast.grammar import ASTNode

    from .node_executor._base_node_executor import BaseNodeExecutor


@add_slots
@dataclass(frozen=True)
class DependencyChain:
    """The dependency chain stores the requisite dependency chains that must be executed before the node executor
    is able to be executed."""

    executor: 'BaseNodeExecutor[ASTNode, object]'
    """The executor that we are holding dependencies for."""

    dependent_on: Tuple['DependencyChain', ...]
    """The requisite dependency chains that must be resolved before we can execute the node `executor`."""
