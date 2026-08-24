from typing import TYPE_CHECKING, Any

from osprey.engine.ast.grammar import Assign, ASTNode

from ..execution_context import NodeFailurePropagationException
from ..node_executor_registry import NodeExecutorRegistry
from ._base_node_executor import BaseNodeExecutor

if TYPE_CHECKING:
    from ..execution_context import ExecutionContext


@NodeExecutorRegistry.register_globally
class AssignExecutor(BaseNodeExecutor[Assign, Any]):
    node_type = Assign

    def execute(self, execution_context: 'ExecutionContext') -> Any:
        node_result = execution_context.resolved_result(self._node.value)

        if self._node.should_extract:
            execution_context.set_output_value(self._node.target.identifier, node_result.ok())

        if node_result.is_err():
            raise NodeFailurePropagationException()
        return node_result.unwrap()

    def get_dependent_nodes(self) -> list[ASTNode]:
        return [self._node.value]
