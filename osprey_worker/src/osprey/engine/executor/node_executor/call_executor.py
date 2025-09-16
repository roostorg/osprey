from typing import TYPE_CHECKING, Any, List

from osprey.engine.ast.grammar import ASTNode, Call

from ..node_executor_registry import NodeExecutorRegistry
from ._base_node_executor import BaseNodeExecutor

if TYPE_CHECKING:
    from ddtrace.span import Span
    from osprey.engine.ast_validator.validation_context import ValidatedSources
    from osprey.engine.udf.arguments import ArgumentsBase
    from osprey.engine.udf.base import UDFBase

    from ..execution_context import ExecutionContext


@NodeExecutorRegistry.register_globally
class CallExecutor(BaseNodeExecutor[Call, Any]):
    node_type = Call
    unresolved_arguments: 'ArgumentsBase'

    _udf: 'UDFBase[Any, Any]'

    def __init__(self, node: Call, sources: 'ValidatedSources'):
        from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs

        super().__init__(node=node, sources=sources)
        udf_map = sources.get_validator_result(ValidateCallKwargs)
        self._udf, self.unresolved_arguments = udf_map[id(node)]
        self.dependent_node_dict = self.unresolved_arguments.get_dependent_node_dict()

    def set_tracing_tags(self, span: 'Span') -> None:
        span.set_tag('udf', self._udf.__class__.__name__)

    def execute(self, execution_context: 'ExecutionContext') -> Any:
        resolved_arguments = self._udf.resolve_arguments(execution_context, self)
        result = self._udf.execute(execution_context, resolved_arguments)
        return self._udf.check_result_type(result)

    def get_dependent_nodes(self) -> List[ASTNode]:
        return list(self.dependent_node_dict.values())

    @property
    def execute_async(self) -> bool:
        return self._udf.execute_async
