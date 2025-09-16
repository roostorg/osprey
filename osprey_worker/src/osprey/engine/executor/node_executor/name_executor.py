from typing import TYPE_CHECKING, Any, List

from osprey.engine.ast.grammar import ASTNode, Load, Name

from ..node_executor_registry import NodeExecutorRegistry
from ._base_node_executor import BaseNodeExecutor

if TYPE_CHECKING:
    from osprey.engine.ast_validator.validation_context import ValidatedSources

    from ..execution_context import ExecutionContext


@NodeExecutorRegistry.register_globally
class NameExecutor(BaseNodeExecutor[Name, Any]):
    node_type = Name

    def __init__(self, node: Name, sources: 'ValidatedSources'):
        super().__init__(node=node, sources=sources)
        assert isinstance(node.context, Load), 'Name executor cannot operate on Store name nodes. See AssignExecutor'

    def execute(self, execution_context: 'ExecutionContext') -> Any:
        return execution_context.resolved(self._node)

    def get_dependent_nodes(self) -> List[ASTNode]:
        return [self._node]
