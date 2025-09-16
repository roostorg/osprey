from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.grammar import Assign, Call, Name, Source

from ..base_validator import SourceValidator


class ValidateCallRValue(SourceValidator):
    """
    Validates that the return value of a call is used (if it has a result), and not attempted to be used
    if it does not have a result.
    """

    def validate_source(self, source: Source) -> None:
        for call_node in filter_nodes(source.ast_root, Call):
            self.validate_call_node(source, call_node)
            assert isinstance(call_node.func, Name)

    def validate_call_node(self, source: Source, node: Call) -> None:
        if not isinstance(node.func, Name):
            return

        udf_class = self.context.udf_registry.get(node.func.identifier)
        if not udf_class:
            return

        has_result = udf_class.has_result()
        result_used = node.parent is not source.ast_root

        if has_result and not result_used:
            self.context.add_error(
                message='you must use the result of this function call',
                hint=(
                    'this must must not be used as a statement within the source root, e.g.:\n'
                    f'- `Foo = {node.func.identifier}(...)` is OK\n'
                    f'- `SomeOtherFunction(argument={node.func.identifier}(...))` is OK\n'
                    f'- `{node.func.identifier}(...)` is not OK'
                ),
                span=node.span,
            )
            return

        if not has_result and result_used:
            if isinstance(node.parent, Assign):
                assign_node = node.parent
                self.context.add_error(
                    message='you cannot store the result of this function call because it has no return value',
                    hint=(
                        'this cannot appear to the right of a `=`, e.g.:\n'
                        f'- `{node.func.identifier}(...)` is OK\n'
                        f'- `{assign_node.target.identifier} = {node.func.identifier}(...)` is not OK'
                    ),
                    span=node.span,
                )

            else:
                self.context.add_error(
                    message='you cannot use the result of this function call because it has no return value',
                    hint=(
                        'this function has no result, and thus its result cannot be used:\n'
                        f'- `{node.func.identifier}(...)` is OK, however:\n'
                        f'- `Foo = Bar + {node.func.identifier}(...)` and\n'
                        f'- `Bar(qux={node.func.identifier}(...))` are not OK\n'
                    ),
                    span=node.span,
                )
