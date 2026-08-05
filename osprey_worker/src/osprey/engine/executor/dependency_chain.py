from dataclasses import dataclass
from typing import TYPE_CHECKING, Tuple

from osprey.engine.utils.types import add_slots

if TYPE_CHECKING:
    from osprey.engine.ast.grammar import ASTNode

    from .node_executor._base_node_executor import BaseNodeExecutor


@add_slots
@dataclass(frozen=True, eq=False)
class DependencyChain:
    """The dependency chain stores the requisite dependency chains that must be executed before the node executor
    is able to be executed.

    eq=False: `executor` is a plain class with default identity equality, so the generated structural
    __eq__/__hash__ already bottomed out at executor-object identity (dataclass field equality falls back
    to `is` for any field without its own __eq__). Declaring eq=False makes that explicit and gives us
    O(1) identity hash instead of an O(size-of-subtree) structural hash recomputed on every call, since
    tuple.__hash__ does not cache its result the way str/frozenset do."""

    executor: 'BaseNodeExecutor[ASTNode, object]'
    """The executor that we are holding dependencies for."""

    dependent_on: Tuple['DependencyChain', ...]
    """The requisite dependency chains that must be resolved before we can execute the node `executor`."""
