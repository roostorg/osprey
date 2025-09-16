from typing import TYPE_CHECKING

from osprey.engine.ast.grammar import None_

from ...node_executor_registry import NodeExecutorRegistry
from .._base_node_executor import BaseNodeExecutor

if TYPE_CHECKING:
    from ...execution_context import ExecutionContext


@NodeExecutorRegistry.register_globally
class NoneExecutor(BaseNodeExecutor[None_, None]):
    node_type = None_

    def execute(self, execution_context: 'ExecutionContext') -> None:
        return None
