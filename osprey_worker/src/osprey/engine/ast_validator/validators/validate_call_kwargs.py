from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Mapping, Tuple

from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.grammar import Assign, Call, Literal, Name, Source, Store, UnaryOperation
from osprey.engine.udf.arguments import (
    EXTRA_ARGS_ATTR,
    ArgumentsBase,
    ConstExpr,
    ConstExprArgumentException,
    ConstExprTypeException,
    is_const_expr,
)
from osprey.engine.udf.base import UDFBase
from osprey.engine.utils.osprey_unary_executor import OspreyUnaryExecutor

from ..base_validator import HasResult, SourceValidator
from .unique_stored_names import IdentifierIndex, UniqueStoredNames

if TYPE_CHECKING:
    from ..validation_context import ValidationContext


UDFNodeMapping = Dict[int, Tuple[UDFBase[Any, Any], ArgumentsBase]]


class ValidateCallKwargs(SourceValidator, HasResult[UDFNodeMapping]):
    """
    Validates that functions are called with the correct keyword argument names, constructing the udf classes
    and creating a udf node mapping
    """

    def __init__(self, context: 'ValidationContext'):
        super().__init__(context)
        self._udf_node_mapping: UDFNodeMapping = {}
        self._identifier_to_resolved_local_literals_mapping: Mapping[str, Literal] = {}
        self._identifier_to_resolved_literal_mapping: IdentifierIndex = context.get_validator_result(UniqueStoredNames)

    def validate_source(self, source: Source) -> None:
        # self._identifier_to_resolved_literal_mapping = self._resolve_literals(source)
        self._identifier_to_resolved_local_literals_mapping = self._resolve_local_literals(source)
        for call_node in filter_nodes(source.ast_root, Call):
            self.validate_call_node(call_node)

    def get_result(self) -> UDFNodeMapping:
        return self._udf_node_mapping

    def validate_call_node(self, call_node: Call) -> None:
        assert isinstance(call_node.func, Name), 'call nodes with attribute not yet supported.'
        function_name = call_node.func.identifier

        # Step 1) Check to see if the function is actually defined in the registry.
        udf_class = self.context.udf_registry.get(function_name)
        if not udf_class:
            self.context.add_error(
                message=f'unknown function: `{function_name}`', span=call_node.span, hint='called here'
            )
            return

        # Step 2) Argument validation phase.
        node_arguments = call_node.argument_dict()
        arguments_type = udf_class.get_arguments_type()
        argument_items = arguments_type.items()
        argument_allows_kwargs = arguments_type.is_extra_arguments_allowed()
        # Construct union of all arguments in both the AST, and in the udf class.
        all_arguments = sorted(set(argument_items) | set(node_arguments))
        expected_kwargs_str = ', '.join(f'`{arg}`' for arg in sorted(argument_items))
        missing_kwargs = []

        resolved_literals = {}
        literal_failed_validation = False

        for argument in all_arguments:
            node_argument = node_arguments.get(argument)
            udf_argument = argument_items.get(argument)

            if not argument_allows_kwargs and node_argument and argument == EXTRA_ARGS_ATTR:
                # currently this can't happen but if `is_extra_arguments_allowed()` changes it might
                self.context.add_error(
                    message='kwargs is a reserved argument name',
                    span=node_argument.span,
                )
                continue

            # Unexpected keyword argument, meaning the AST has a node for that argument, but the UDF does not.
            if not argument_allows_kwargs and node_argument and not udf_argument:
                self.context.add_error(
                    message=f'unknown keyword argument: `{argument}`',
                    span=node_argument.span,
                    hint=f'valid keyword arguments are: [{expected_kwargs_str}]',
                )
                continue

            # Expected keyword argument, meaning, the UDF wanted the argument, but it did not exist in AST.
            if (
                udf_argument
                and not arguments_type.kwarg_has_default(argument)
                and node_argument is None
                and argument != EXTRA_ARGS_ATTR
            ):
                missing_kwargs.append(argument)
                continue

            # Statically resolve and type-check ConstExprs.
            if is_const_expr(udf_argument) and node_argument is not None:
                # If the literal refers to a name then attempt to resolve it to a literal one layer deep
                # This is a temporary solution to allow us to reuse literals

                # Long term a better solution is to have a literal resolution layer that simplifies the ast by replacing
                # name nodes with literal nodes before validation. Perhaps a preprocessing system?
                if isinstance(node_argument, Name):
                    if node_argument.identifier_key in self._identifier_to_resolved_literal_mapping:
                        ast_node = self._identifier_to_resolved_literal_mapping[node_argument.identifier_key].ast_node
                        assert isinstance(ast_node.parent, Assign)
                        node_argument = ast_node.parent.value
                    elif node_argument.identifier_key in self._identifier_to_resolved_local_literals_mapping:
                        node_argument = self._identifier_to_resolved_local_literals_mapping[
                            node_argument.identifier_key
                        ]
                    else:
                        self.context.add_error(
                            message=f'name `{node_argument.identifier}` could not be resolved to a `Literal',
                            span=node_argument.span,
                            hint=(
                                f"currently literal resolutions are only one layer deep so you can't do a = b = c = 1\n"
                                f'perhaps variable `{node_argument.identifier}` is undefined?'
                            ),
                        )
                # Pre-Process a valid UnaryOperation
                if isinstance(node_argument, UnaryOperation):
                    try:
                        node_argument = OspreyUnaryExecutor(node_argument).get_modified_node()
                        # the operand is what we need to evaluate into a literal
                        node_argument = node_argument.operand
                    except Exception:
                        assert isinstance(node_argument, UnaryOperation)
                        self.context.add_error(
                            message=(
                                f'`{node_argument.operator}` not supported\n'
                                f'or {node_argument.operand} is incorrect type'
                            ),
                            span=node_argument.span,
                            hint=(
                                'currently only the USub UnaryOperation is supported in arguments\n'
                                'the operand must be of type `int` or `float`'
                            ),
                        )
                try:
                    resolved_literals[argument] = ConstExpr.resolve(
                        udf_argument,
                        name=argument,
                        node=node_argument,
                    )
                except ConstExprTypeException as e:
                    literal_failed_validation = True
                    self.context.add_error(
                        message=f'invalid argument type: `{e.node.__class__.__name__}`',
                        span=e.node.span,
                        hint=f'expected type `{e.expected.__name__}`',
                    )

        # Report any missing kwargs.
        if missing_kwargs:
            missing_kwargs_str = ', '.join(f'`{kwarg}`' for kwarg in missing_kwargs)
            self.context.add_error(
                message=f'{len(missing_kwargs)} missing keyword argument(s)',
                span=call_node.span,
                hint=f'the following keyword arguments were not provided: [{missing_kwargs_str}]',
            )

        # If any literal failed validation, or we are missing kwargs, we can avoid performing further validation.
        if literal_failed_validation or missing_kwargs:
            return

        arguments = arguments_type(call_node=call_node, arguments=resolved_literals)
        try:
            # Try and construct the UDF class. It may throw, so let's catch.
            udf = udf_class(validation_context=self.context, arguments=arguments)
            # If it's a dynamic UDF, we'll add the RValueTypeChecker in ValidateDynamicCallsHaveAnnotatedRValue
            # Store the udf in the node mapping - this will be read in `CallExecutor` within the executor,
            # to then pluck the udf + arguments that we parsed here to be executed upon.
            self._udf_node_mapping[id(call_node)] = (udf, arguments)

        # Catch the ConstExprArgumentException - if any other exception is thrown, it's considered a
        # compilation error!
        except ConstExprArgumentException as e:
            self.context.add_error(
                message=(
                    e.message_override
                    or f'failed to validate argument `{e.const_expr.name}` for `{function_name}(...)`'
                ),
                span=e.const_expr.argument_span,
                hint=f'error: {e.wrapped_exception}',
            )

    def _resolve_local_literals(self, source: Source) -> Mapping[str, Literal]:
        """
        Walk the ast and create a mapping of `Name` identifiers to `Literal` nodes

        This is used later to resolve literals one layer deep
        """
        identifier_to_resolved_literal = {}
        for literal_node in filter_nodes(source.ast_root, Literal):
            if (
                isinstance(literal_node.parent, Assign)
                and isinstance(literal_node.parent.target, Name)
                and isinstance(literal_node.parent.target.context, Store)
            ):
                identifier_to_resolved_literal[literal_node.parent.target.identifier_key] = literal_node
        return identifier_to_resolved_literal
