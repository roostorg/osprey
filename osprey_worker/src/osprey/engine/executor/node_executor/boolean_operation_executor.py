from typing import TYPE_CHECKING, Callable, Iterator, Sequence

from osprey.engine.ast.grammar import And, ASTNode, BooleanOperation, Or

from ..node_executor_registry import NodeExecutorRegistry
from ._base_node_executor import BaseNodeExecutor

if TYPE_CHECKING:
    from osprey.engine.ast_validator.validation_context import ValidatedSources

    from ..execution_context import ExecutionContext


@NodeExecutorRegistry.register_globally
class BooleanOperationExecutor(BaseNodeExecutor[BooleanOperation, bool]):
    node_type = BooleanOperation

    def __init__(self, node: BooleanOperation, sources: 'ValidatedSources'):
        super().__init__(node=node, sources=sources)
        self.operator: Callable[[Iterator[object]], bool] = _OPERATORS[node.operand.__class__]

    def execute(self, execution_context: 'ExecutionContext') -> bool:
        # Treat failed nodes as falsey (None) - let's hope that's the right way for things to fail :fingerscrossed:
        resolved_values = (execution_context.resolved(n, return_none_for_failed_values=True) for n in self._node.values)
        return self.operator(resolved_values)

    def get_dependent_nodes(self) -> Sequence[ASTNode]:
        return self._node.values


_OPERATORS = {And: all, Or: any}
