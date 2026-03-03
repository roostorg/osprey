import operator
from typing import TYPE_CHECKING, Callable, List

from osprey.engine.ast.grammar import (
    ASTNode,
    BinaryComparison,
    Equals,
    GreaterThan,
    GreaterThanEquals,
    In,
    LessThan,
    LessThanEquals,
    NotEquals,
    NotIn,
)

from ..node_executor_registry import NodeExecutorRegistry
from ._base_node_executor import BaseNodeExecutor

if TYPE_CHECKING:
    from osprey.engine.ast_validator.validation_context import ValidatedSources

    from ..execution_context import ExecutionContext


@NodeExecutorRegistry.register_globally
class BinaryComparisonExecutor(BaseNodeExecutor[BinaryComparison, bool]):
    node_type = BinaryComparison

    def __init__(self, node: BinaryComparison, sources: 'ValidatedSources'):
        super().__init__(node=node, sources=sources)
        self.comparator: Callable[[object, object], bool] = _COMPARATORS[node.comparator.__class__]

        self.left_can_be_none = self.comparator in (
            _COMPARATORS[In],
            _COMPARATORS[NotIn],
            _COMPARATORS[Equals],
            _COMPARATORS[NotEquals],
        )
        self.right_can_be_none = self.comparator in (
            _COMPARATORS[Equals],
            _COMPARATORS[NotEquals],
        )
        # For numerical comparisons (<, <=, >, >=), we need to handle None at runtime
        # because the executor resolves all boolean operation dependencies before
        # short-circuiting. The static validator enforces null-check patterns, but
        # at runtime the comparison may still be evaluated with None values.
        self.handles_none_comparison = self.comparator in (
            _COMPARATORS[LessThan],
            _COMPARATORS[LessThanEquals],
            _COMPARATORS[GreaterThan],
            _COMPARATORS[GreaterThanEquals],
        )

    def execute(self, execution_context: 'ExecutionContext') -> bool:
        left = execution_context.resolved(self._node.left, return_none_for_failed_values=self.left_can_be_none)
        right = execution_context.resolved(self._node.right, return_none_for_failed_values=self.right_can_be_none)

        # Handle None values for numerical comparisons at runtime.
        # Even with null-check patterns like "X != None and X >= 90", the executor
        # resolves all dependencies before the boolean operation short-circuits.
        if self.handles_none_comparison and (left is None or right is None):
            return False

        return bool(self.comparator(left, right))

    def get_dependent_nodes(self) -> List[ASTNode]:
        return [self._node.left, self._node.right]


def in_operator(a, b):
    """
    This special operator protects the native python `in` and `not in` operators
    from raising a TypeError when provided optionals (or lists containing optionals)
    """
    if a is None and (b is None or (isinstance(b, list) and None in b)):
        # None in None (or None in [..., None])
        return True
    if a is None or b is None:
        # None in <something> or <something> in None
        return False
    return a in b


_COMPARATORS = {
    Equals: operator.eq,
    NotEquals: operator.ne,
    LessThan: operator.lt,
    LessThanEquals: operator.le,
    GreaterThan: operator.gt,
    GreaterThanEquals: operator.ge,
    In: in_operator,
    NotIn: lambda a, b: not in_operator(a, b),
}
