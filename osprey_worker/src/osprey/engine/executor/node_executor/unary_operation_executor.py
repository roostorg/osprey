import operator
from typing import TYPE_CHECKING, Any, Callable, List, Mapping, Type

from osprey.engine.ast.grammar import ASTNode, Not, UnaryOperation, UnaryOperator, USub

from ..node_executor_registry import NodeExecutorRegistry
from ._base_node_executor import BaseNodeExecutor

if TYPE_CHECKING:
    from osprey.engine.ast_validator.validation_context import ValidatedSources

    from ..execution_context import ExecutionContext


@NodeExecutorRegistry.register_globally
class UnaryOperationExecutor(BaseNodeExecutor[UnaryOperation, Any]):
    node_type = UnaryOperation

    def __init__(self, node: UnaryOperation, sources: 'ValidatedSources'):
        super().__init__(node=node, sources=sources)
        self.operator = _UNARY_OPERATORS[node.operator.__class__]

    def execute(self, execution_context: 'ExecutionContext') -> Any:
        operand = execution_context.resolved(self._node.operand)
        return self.operator(operand)

    def get_dependent_nodes(self) -> List[ASTNode]:
        return [self._node.operand]


_UNARY_OPERATORS: Mapping[Type[UnaryOperator], Callable[[Any], Any]] = {
    Not: operator.not_,
    USub: operator.neg,
}
