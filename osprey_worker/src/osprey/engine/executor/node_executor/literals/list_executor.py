from typing import TYPE_CHECKING, Any, Sequence
from typing import List as ListT

from osprey.engine.ast.grammar import ASTNode, List

from ...node_executor_registry import NodeExecutorRegistry
from .._base_node_executor import BaseNodeExecutor

if TYPE_CHECKING:
    from ...execution_context import ExecutionContext


@NodeExecutorRegistry.register_globally
class ListExecutor(BaseNodeExecutor[List, ListT[Any]]):
    node_type = List

    def execute(self, execution_context: 'ExecutionContext') -> ListT[Any]:
        return [execution_context.resolved(n) for n in self._node.items]

    def get_dependent_nodes(self) -> Sequence[ASTNode]:
        return self._node.items
