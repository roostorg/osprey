from typing import TYPE_CHECKING, Any, List

from osprey.engine.ast.grammar import Assign, ASTNode

from ..node_executor_registry import NodeExecutorRegistry
from ._base_node_executor import BaseNodeExecutor

if TYPE_CHECKING:
    from ..execution_context import ExecutionContext


@NodeExecutorRegistry.register_globally
class AssignExecutor(BaseNodeExecutor[Assign, Any]):
    node_type = Assign

    def execute(self, execution_context: 'ExecutionContext') -> Any:
        # We want to store a value in the output even if the dependency node failed.
        resolved_maybe_error = execution_context.resolved(self._node.value, return_none_for_failed_values=True)

        if self._node.should_extract:
            execution_context.set_output_value(self._node.target.identifier, resolved_maybe_error)

        # Re-fetch to throw an exception in case our dependency node failed.
        resolved_not_error = execution_context.resolved(self._node.value, return_none_for_failed_values=False)
        return resolved_not_error

    def get_dependent_nodes(self) -> List[ASTNode]:
        return [self._node.value]
