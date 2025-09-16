from typing import TYPE_CHECKING, Union

from osprey.engine.ast.grammar import Number

from ...node_executor_registry import NodeExecutorRegistry
from .._base_node_executor import BaseNodeExecutor

if TYPE_CHECKING:
    from ...execution_context import ExecutionContext


@NodeExecutorRegistry.register_globally
class NumberExecutor(BaseNodeExecutor[Number, Union[int, float]]):
    node_type = Number

    def execute(self, execution_context: 'ExecutionContext') -> Union[int, float]:
        return self._node.value
