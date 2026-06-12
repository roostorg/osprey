import operator
from typing import TYPE_CHECKING, Any, Callable, List

from osprey.engine.ast.grammar import (
    Add,
    ASTNode,
    BinaryOperation,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    Divide,
    FloorDivide,
    LeftShift,
    Modulo,
    Multiply,
    Pow,
    RightShift,
    Subtract,
)

from ..node_executor_registry import NodeExecutorRegistry
from ._base_node_executor import BaseNodeExecutor

if TYPE_CHECKING:
    from osprey.engine.ast_validator.validation_context import ValidatedSources

    from ..execution_context import ExecutionContext


@NodeExecutorRegistry.register_globally
class BinaryOperationExecutor(BaseNodeExecutor[BinaryOperation, Any]):
    node_type = BinaryOperation

    def __init__(self, node: BinaryOperation, sources: 'ValidatedSources'):
        super().__init__(node=node, sources=sources)
        self.operator: Callable[[Any, Any], Any] = _BINARY_OPERATORS[node.operator.__class__]

    def execute(self, execution_context: 'ExecutionContext') -> Any:
        left = execution_context.resolved(self._node.left)
        right = execution_context.resolved(self._node.right)
        return self.operator(left, right)

    def get_dependent_nodes(self) -> List[ASTNode]:
        return [self._node.left, self._node.right]


def _safe_truediv(left: Any, right: Any) -> Any:
    if right == 0:
        return 0
    return operator.truediv(left, right)


def _safe_floordiv(left: Any, right: Any) -> Any:
    if right == 0:
        return 0
    return operator.floordiv(left, right)


def _safe_mod(left: Any, right: Any) -> Any:
    if right == 0:
        return 0
    return operator.mod(left, right)


_BINARY_OPERATORS = {
    Add: operator.add,
    Subtract: operator.sub,
    Multiply: operator.mul,
    Divide: _safe_truediv,
    FloorDivide: _safe_floordiv,
    Modulo: _safe_mod,
    Pow: operator.pow,
    LeftShift: operator.lshift,
    RightShift: operator.rshift,
    BitwiseOr: operator.or_,
    BitwiseAnd: operator.and_,
    BitwiseXor: operator.xor,
}
