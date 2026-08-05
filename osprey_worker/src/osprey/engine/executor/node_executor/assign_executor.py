from typing import TYPE_CHECKING, Any, List

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
        # Resolve the dependency node's value once, then derive both of `resolved()`'s failure
        # behaviors from it, rather than resolving it twice (once per behavior).
        node_result = execution_context.resolved_result(self._node.value)

        if self._node.should_extract:
            # We want to store a value in the output even if the dependency node failed.
            execution_context.set_output_value(self._node.target.identifier, node_result.ok())

        if node_result.is_err():
            # Propagate the failure, matching resolved()'s default (return_none_for_failed_values=False).
            raise NodeFailurePropagationException()
        return node_result.unwrap()

    def get_dependent_nodes(self) -> List[ASTNode]:
        return [self._node.value]
