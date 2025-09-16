from typing import TYPE_CHECKING

from osprey.engine.ast.grammar import Boolean

from ...node_executor_registry import NodeExecutorRegistry
from .._base_node_executor import BaseNodeExecutor

if TYPE_CHECKING:
    from ...execution_context import ExecutionContext


@NodeExecutorRegistry.register_globally
class BooleanExecutor(BaseNodeExecutor[Boolean, bool]):
    node_type = Boolean

    def execute(self, execution_context: 'ExecutionContext') -> bool:
        return self._node.value
