from typing import TYPE_CHECKING

from osprey.engine.ast.grammar import String

from ...node_executor_registry import NodeExecutorRegistry
from .._base_node_executor import BaseNodeExecutor

if TYPE_CHECKING:
    from ...execution_context import ExecutionContext


@NodeExecutorRegistry.register_globally
class StringExecutor(BaseNodeExecutor[String, str]):
    node_type = String

    def execute(self, execution_context: 'ExecutionContext') -> str:
        return self._node.value
