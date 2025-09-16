from typing import TYPE_CHECKING, Sequence

from osprey.engine.ast.grammar import ASTNode, FormatString

from ...node_executor_registry import NodeExecutorRegistry
from .._base_node_executor import BaseNodeExecutor

if TYPE_CHECKING:
    from ...execution_context import ExecutionContext


@NodeExecutorRegistry.register_globally
class FormatStringExecutor(BaseNodeExecutor[FormatString, str]):
    node_type = FormatString

    def execute(self, execution_context: 'ExecutionContext') -> str:
        formatted_values = {n.identifier: execution_context.resolved(n) for n in self._node.names}
        return self._node.format_string.format(**formatted_values)

    def get_dependent_nodes(self) -> Sequence[ASTNode]:
        return self._node.names
