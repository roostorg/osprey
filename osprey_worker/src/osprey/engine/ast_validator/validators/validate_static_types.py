from dataclasses import dataclass, replace
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Sequence, Set, Type, Union, cast

from osprey.engine.ast import grammar
from osprey.engine.ast.error_utils import SpanWithHint
from osprey.engine.language_types.entities import EntityT
from osprey.engine.language_types.post_execution_convertible import PostExecutionConvertible
from osprey.engine.stdlib.udfs.import_ import Import
from osprey.engine.udf.rvalue_type_checker import AnnotationConversionError, convert_ast_annotation_to_type_checker
from osprey.engine.udf.type_evaluator import is_compatible_type
from osprey.engine.udf.type_helpers import (
    AnyType,
    get_list_item_type,
    get_typevar_substitution,
    is_list,
    to_display_str,
)
from osprey.engine.utils.types import add_slots
from typing_extensions import get_origin

from ..base_validator import HasInput, HasResult, SourceValidator
from .imports_must_not_have_cycles import ImportsMustNotHaveCycles
from .unique_stored_names import UniqueStoredNames
from .validate_call_kwargs import UDFNodeMapping, ValidateCallKwargs
from .validate_dynamic_calls_have_annotated_rvalue import ValidateDynamicCallsHaveAnnotatedRValue
from .variables_must_be_defined import VariablesMustBeDefined

if TYPE_CHECKING:
    from ..validation_context import ValidationContext


@add_slots
@dataclass
class _TypeAndSpan:
    type: type
    span: grammar.Span
    should_extract: bool = True
    can_extract: bool = True
    source_annotation: Union[None, grammar.Annotation, grammar.AnnotationWithVariants] = None

    def copy(self, type: Optional[Type[Any]] = None) -> '_TypeAndSpan':
        return replace(self, type=type or self.type)


@add_slots
@dataclass
class ValidateStaticTypesResult:
    name_type_and_span_cache: Dict[str, _TypeAndSpan]
    nodes_to_unwrap: Set[int]


@add_slots
@dataclass
class _ValidTwoArgTypeTransition:
    valid_left_type: type
    valid_right_type: type
    resulting_type: type


_INT_OR_FLOAT_T = cast(type, Union[int, float])


class ValidateStaticTypes(SourceValidator, HasInput[Dict[str, _TypeAndSpan]], HasResult[ValidateStaticTypesResult]):
    def __init__(self, context: 'ValidationContext'):
        super().__init__(context)
        # Get type information passed in from previous runs, used to type check queries.
        self._name_type_and_span_cache: Dict[str, _TypeAndSpan] = context.get_validator_input(type(self), {})
        self._nodes_to_unwrap: Set[int] = set()
        self._checked_sources: Set[grammar.Source] = set()
        self._udf_node_mapping: UDFNodeMapping = context.get_validator_result(ValidateCallKwargs)

        # Allows us to skip cycle checking, assume unique/existing names, have rtype checkers set
        context.validator_depends_on(
            [
                ImportsMustNotHaveCycles,
                UniqueStoredNames,
                ValidateDynamicCallsHaveAnnotatedRValue,
                VariablesMustBeDefined,
            ]
        )

    @classmethod
    def to_post_execution_types(cls, result: ValidateStaticTypesResult) -> Dict[str, _TypeAndSpan]:
        """Converts a given result with the assumption that we are no longer in the primary rules execution context.
        Useful for type checking queries that run on execution results."""
        types = result.name_type_and_span_cache
        post_execution_types = {}
        for name, type_and_span in types.items():
            t = type_and_span.type
            post_execution_type = PostExecutionConvertible.maybe_post_execution_type(t)
            if post_execution_type is not None:
                t = post_execution_type

            post_execution_types[name] = type_and_span.copy(type=t)

        return post_execution_types

    def get_result(self) -> ValidateStaticTypesResult:
        # Pass along type information for future runs, namely queries.
        return ValidateStaticTypesResult(self._name_type_and_span_cache, self._nodes_to_unwrap)

    def _maybe_get_additional_span_for_identifier_definition(
        self, expression: grammar.Expression, expression_type_str: str
    ) -> Sequence[SpanWithHint]:
        if not isinstance(expression, grammar.Name):
            return []

        # TODO - Could be nice to point to the arg that causes a call node to be a given type for generics
        span = self._name_type_and_span_cache[expression.identifier_key].span
        return [
            SpanWithHint(
                span,
                hint=(
                    f'variable `{expression.identifier}` with incompatible type {expression_type_str}'
                    ' originally defined here'
                ),
            )
        ]

    def _check_compatible_type(
        self,
        type_t: type,
        accepted_by_t: type,
        message: str,
        node: grammar.ASTNode,
        hint: str = '',
        additional_spans_message: str = '',
        additional_spans: Sequence[Union[grammar.Span, SpanWithHint]] = tuple(),
    ) -> None:
        compatible_type_result = is_compatible_type(type_t, accepted_by_t)
        if compatible_type_result.is_ok():
            compatible_type_info = compatible_type_result.unwrap()
            if compatible_type_info is None:
                self.context.add_error(
                    message=message,
                    span=node.span,
                    hint=hint,
                    additional_spans_message=additional_spans_message,
                    additional_spans=additional_spans,
                )
            elif compatible_type_info.unwrapped_to_type is not None:
                self._nodes_to_unwrap.add(id(node))
        elif compatible_type_result.is_err():
            compatible_type_result.unwrap_err().add_error(self.context, node.span)

    def validate_source(self, source: grammar.Source) -> None:
        # Can assume no cycles here because we depend on ImportsMustNotHaveCycles
        if source in self._checked_sources:
            return

        self._checked_sources.add(source)

        for statement in source.ast_root.statements:
            if isinstance(statement, grammar.Assign):
                self._validate_assign(statement)
            elif isinstance(statement, grammar.Call):
                self._validate_expression(statement)
            else:
                raise TypeError(f'Unknown statement type {statement} ({type(statement)})')

    def _validate_assign(self, assign: grammar.Assign) -> None:
        value_type = self._validate_expression(assign.value)
        if assign.annotation is None:
            stored_type = value_type
        else:
            # Check that the return type is compatible with the annotation
            try:
                rvalue_type_checker = convert_ast_annotation_to_type_checker(assign.annotation)
            except AnnotationConversionError as e:
                self.context.add_error(
                    message=e.message,
                    span=e.span,
                    hint=e.hint,
                    additional_spans=e.additional_spans,
                    additional_spans_message=e.additional_spans_message,
                )
                # Default to using the value type
                stored_type = value_type
            else:
                annotation_type = rvalue_type_checker.to_typing_type()
                value_type_str = to_display_str(value_type)
                annotation_type_str = to_display_str(annotation_type)
                self._check_compatible_type(
                    type_t=value_type,
                    accepted_by_t=annotation_type,
                    message='incompatible types in assignment',
                    node=assign.value,
                    hint=f'has type {value_type_str}, expected {annotation_type_str}',
                    additional_spans=[
                        SpanWithHint(assign.annotation.span, f'expected {annotation_type_str} due to this'),
                        *self._maybe_get_additional_span_for_identifier_definition(assign.value, value_type_str),
                    ],
                )
                # Use the self-declared annotation type moving forward
                stored_type = annotation_type

        if assign.can_extract is False and (
            assign.annotation is None
            or assign.annotation.identifier not in (grammar.Annotations.Secret, grammar.Annotations.ExtractSecret)
        ):
            self.context.add_error(
                message='assignments can not include non-extractable features if they are not marked Secret',
                span=assign.span,
                hint=f"name '{assign.target.identifier}' is not marked as non-extractable",
            )

        # Store off return type into name
        self._name_type_and_span_cache[assign.target.identifier_key] = _TypeAndSpan(
            type=stored_type,
            span=assign.span,
            can_extract=assign.can_extract,
            should_extract=assign.should_extract,
            source_annotation=assign.annotation,
        )

    def _validate_expression(self, expression: grammar.Expression) -> type:
        if isinstance(expression, grammar.Name):
            return self._validate_name(expression)
        elif isinstance(expression, grammar.String):
            return self._validate_string(expression)
        elif isinstance(expression, grammar.Number):
            return self._validate_number(expression)
        elif isinstance(expression, grammar.Boolean):
            return self._validate_boolean(expression)
        elif isinstance(expression, grammar.None_):
            return self._validate_none(expression)
        elif isinstance(expression, grammar.List):
            return self._validate_list(expression)
        elif isinstance(expression, grammar.Call):
            return self._validate_call(expression)
        elif isinstance(expression, grammar.BinaryOperation):
            return self._validate_binary_operation(expression)
        elif isinstance(expression, grammar.UnaryOperation):
            return self._validate_unary_operation(expression)
        elif isinstance(expression, grammar.BinaryComparison):
            return self._validate_binary_comparison(expression)
        elif isinstance(expression, grammar.BooleanOperation):
            return self._validate_boolean_operation(expression)
        elif isinstance(expression, grammar.Attribute):
            return self._validate_attribute(expression)
        elif isinstance(expression, grammar.FormatString):
            return self._validate_format_string(expression)
        elif isinstance(expression, grammar.Annotation):
            return self._validate_annotation(expression)
        elif isinstance(expression, grammar.AnnotationWithVariants):
            return self._validate_annotation_with_variants(expression)
        else:
            raise TypeError(f'Unknown expression type {expression} ({type(expression)})')

    def _validate_name(self, name: grammar.Name) -> type:
        if name.identifier_key in self._name_type_and_span_cache:
            name.set_source_annotation(self._name_type_and_span_cache[name.identifier_key].source_annotation)
            return self._name_type_and_span_cache[name.identifier_key].type
        else:
            # This is handled elsewhere, don't need a second error for it.
            return AnyType

    # noinspection PyMethodMayBeStatic
    def _validate_string(self, _string: grammar.String) -> type:
        return str

    # noinspection PyMethodMayBeStatic
    def _validate_number(self, number: grammar.Number) -> type:
        return type(number.value)

    # noinspection PyMethodMayBeStatic
    def _validate_boolean(self, _boolean: grammar.Boolean) -> type:
        return bool

    # noinspection PyMethodMayBeStatic
    def _validate_none(self, _none: grammar.None_) -> type:
        return type(None)

    def _validate_list(self, list_: grammar.List) -> type:
        child_types = set(self._validate_expression(item) for item in list_.items)
        if len(child_types) > 1:
            child_type_strs = ', '.join(sorted(to_display_str(t) for t in child_types))
            self.context.add_error(
                'found multiple different types in list literal, unable to infer type',
                list_.span,
                hint=f'has types {child_type_strs}',
            )
            return List[Any]
        if len(child_types) == 0:
            # Can be list of any type, it's empty
            # If we're assigning this to a variable, make sure that has an annotation.
            if isinstance(list_.parent, grammar.Assign) and list_.parent.annotation is None:
                self.context.add_error(
                    message=(
                        'empty list literal has a dynamic item type, and thus must be assigned to a variable'
                        ' with a type annotation'
                    ),
                    span=list_.parent.span,
                    hint=(
                        f'give this variable a type annotation, eg:\n`{list_.parent.target.identifier}: List[str] = []`'
                    ),
                )
            return List[Any]
        (child_type,) = child_types
        return List[child_type]  # type: ignore # Doesn't like runtime types like this

    def _validate_call(self, call: grammar.Call) -> type:
        udf, arguments = self._udf_node_mapping[id(call)]

        # Special case to handle import. Need to do this so we populate name types.
        if isinstance(udf, Import):
            for source, _ in udf.sources_and_spans:
                self.validate_source(source)
            # Continue on with the rest of the function

        param_items = arguments.items()
        generic_item_names = arguments.get_generic_item_names(func_name=to_display_str(type(udf), include_quotes=False))
        is_extra_arguments_allowed = arguments.is_extra_arguments_allowed()

        # The resolved value of the generic type variable of the call, if there is one
        resolved_typevar_type = None
        previous_generic_kwarg = None
        for kwarg in call.arguments:
            arg_value_type = self._validate_expression(kwarg.value)
            # If get_generic_item_names returns a non-empty list, then we use the argument value types
            # to infer the value of the generic typevar. The typevar type is then used to determine the
            # UDF's return type. See UDFBase's docstring for more information on generic UDFs and how they
            # interplay with generic arguments.
            if kwarg.name in generic_item_names:
                generic_arg = arguments.items()[kwarg.name]

                # Special case for Optionals, since `get_typevar_substitution` is not intended to figure out that
                # `get_typevar_substitution(Optional[T], str) == str`, since it expects both arguments to have the same
                # structure.
                if self._is_optional_type(generic_arg):
                    # None is compatible with optional, but doesn't tell us what the value of the typevar would be.
                    # We can only skip it.
                    if arg_value_type is type(None):  # noqa: E721
                        continue

                    # If the arg is an Optional (e.g. the return value from a JsonData() call), then we perform
                    # substitution as normal. Note: we use _is_actual_optional_type here to avoid treating EntityT
                    # as Optional, which would cause get_typevar_substitution to fail due to different origins.
                    if self._is_actual_optional_type(arg_value_type):
                        next_resolved_typevar_type = get_typevar_substitution(generic_arg, arg_value_type)
                    else:
                        # Otherwise, we treat the arg_value_type as if it is the T in Optional[T]
                        next_resolved_typevar_type = arg_value_type
                else:
                    next_resolved_typevar_type = get_typevar_substitution(generic_arg, arg_value_type)
                if resolved_typevar_type is not None and next_resolved_typevar_type != resolved_typevar_type:
                    # Error for incompatible generic types
                    self.context.add_error(
                        'conflicting generic types',
                        kwarg.value.span,
                        hint=(
                            f'has type {to_display_str(next_resolved_typevar_type)}, '
                            f'expected {to_display_str(resolved_typevar_type)}'
                        ),
                        additional_spans=[previous_generic_kwarg.value.span],
                        additional_spans_message=(
                            f'argument `{previous_generic_kwarg.name}` has type {to_display_str(resolved_typevar_type)}'
                        ),
                    )
                else:
                    resolved_typevar_type = next_resolved_typevar_type
                    previous_generic_kwarg = kwarg
            else:
                if is_extra_arguments_allowed and kwarg.name not in param_items:
                    param_type = arguments.get_extra_arguments_values_type()
                else:
                    param_type = param_items[kwarg.name]
                arg_value_type_str = to_display_str(arg_value_type)
                self._check_compatible_type(
                    type_t=arg_value_type,
                    accepted_by_t=param_type,
                    message=f'argument `{kwarg.name}` to {to_display_str(type(udf))} has incompatible type',
                    node=kwarg.value,
                    hint=f'has type {arg_value_type_str}, expected {to_display_str(param_type)}',
                    additional_spans=self._maybe_get_additional_span_for_identifier_definition(
                        kwarg.value, arg_value_type_str
                    ),
                )

        # Handle return type. Relies on dynamic functions having their rvalue type checkers set.
        return udf.get_resolved_rvalue_type(resolved_typevar_type)

    def _validate_binary_operation(self, binary_operation: grammar.BinaryOperation) -> type:
        return self._validate_two_arg_type_transitions(
            binary_operation.span,
            binary_operation.operator.original_operator,
            binary_operation.left,
            binary_operation.right,
            _get_binary_operation_transitions(),
            allow_any=False,
        )

    def _validate_unary_operation(self, unary_operation: grammar.UnaryOperation) -> type:
        operand_type = self._validate_expression(unary_operation.operand)
        operand_type_str = to_display_str(operand_type)

        if isinstance(unary_operation.operator, grammar.Not):
            # Check if the unwrapped type is an boolean
            compat_item = is_compatible_type(operand_type, bool).unwrap()
            has_valid_transition = compat_item is not None and isinstance(compat_item.unwrapped_to_type, type(bool))
            has_valid_transition_without_unwrap = compat_item is not None and compat_item.unwrapped_to_type is None

            if not has_valid_transition:
                self._check_compatible_type(
                    type_t=operand_type,
                    accepted_by_t=bool,
                    message='`not` only works on `bool` types',
                    node=unary_operation,
                    hint='`not` should be used on `bool` types like `not (X == Y)`',
                    additional_spans=[
                        SpanWithHint(unary_operation.operand.span, hint=f'has type {operand_type_str}'),
                        *self._maybe_get_additional_span_for_identifier_definition(
                            unary_operation.operand, operand_type_str
                        ),
                    ],
                )
            elif not has_valid_transition_without_unwrap:
                self._nodes_to_unwrap.add(id(unary_operation.operand))
            return bool

        elif isinstance(unary_operation.operator, grammar.USub):
            compat_item = is_compatible_type(operand_type, _INT_OR_FLOAT_T).unwrap()
            # isinstance will complain if we compare to _INT_OR_FLOAT_T because generics are no longer instances of type
            has_valid_transition = compat_item is not None and isinstance(compat_item.unwrapped_to_type, (int, float))
            has_valid_transition_without_unwrap = compat_item is not None and compat_item.unwrapped_to_type is None

            if not has_valid_transition:
                self._check_compatible_type(
                    type_t=operand_type,
                    accepted_by_t=_INT_OR_FLOAT_T,
                    message=f'bad operand type for unary -: `{operand_type}`',
                    node=unary_operation,
                    hint='unary `-` should be used on `int` types or `float` types like `-3`',
                    additional_spans=[
                        SpanWithHint(unary_operation.operand.span, hint=f'has type {operand_type_str}'),
                        *self._maybe_get_additional_span_for_identifier_definition(
                            unary_operation.operand, operand_type_str
                        ),
                    ],
                )
            elif not has_valid_transition_without_unwrap:
                self._nodes_to_unwrap.add(id(unary_operation.operand))
            return operand_type

        else:
            raise AssertionError(f'Unknown operator {unary_operation.operator}')

    def _is_optional_type(self, t: type) -> bool:
        origin = get_origin(t)

        # We allow Entities to typecheck as optional in order to avoid having to update the type system
        # to support Optional[Entity[str]]
        if origin is Optional or origin is EntityT:
            return True
        elif origin is Union:
            return type(None) in t.__args__  # type: ignore[attr-defined]

        return False

    def _is_actual_optional_type(self, t: type) -> bool:
        """Check if a type is actually Optional/Union with None, not just treated as optional (like EntityT)."""
        origin = get_origin(t)

        if origin is Optional:
            return True
        elif origin is Union:
            return type(None) in t.__args__  # type: ignore[attr-defined]

        return False

    def _validate_binary_comparison(self, binary_comparison: grammar.BinaryComparison) -> type:
        def valid_transition_hook(left_type: type, right_type: type) -> None:
            # Some extra warnings for certain cases
            if isinstance(binary_comparison.comparator, (grammar.Equals, grammar.NotEquals)):
                # If there's no way either type is compatible with the other then error. We *should* have
                # already caught any unsupported types by this point, so just unwrap the results.
                compat_left_item = is_compatible_type(left_type, right_type).unwrap()
                compat_item_left = is_compatible_type(right_type, left_type).unwrap()

                # An invalid None comparison is when one side is None and the other side is non-optional
                is_invalid_none_comparison = (
                    left_type is type(None) or right_type is type(None)  # noqa: E721
                ) and not (self._is_optional_type(left_type) or self._is_optional_type(right_type))

                # TODO: remove this condition once we fully require variables to be Optional to compare with None
                is_comparing_none = left_type is type(None) or right_type is type(None)  # noqa: E721
                has_valid_transition = compat_left_item is not None or compat_item_left is not None or is_comparing_none
                has_valid_transition_without_unwrap = (
                    is_comparing_none
                    or (compat_left_item is not None and compat_left_item.unwrapped_to_type is None)
                    or (compat_item_left is not None and compat_item_left.unwrapped_to_type is None)
                )

                if is_invalid_none_comparison:
                    left_is_not_none = left_type is not type(None)  # noqa: E721

                    if left_is_not_none:
                        type_str = to_display_str(left_type)
                        additional_spans = [
                            *self._maybe_get_additional_span_for_identifier_definition(
                                binary_comparison.left, type_str
                            ),
                        ]
                    else:
                        type_str = to_display_str(right_type)
                        additional_spans = [
                            *self._maybe_get_additional_span_for_identifier_definition(
                                binary_comparison.right, type_str
                            ),
                        ]

                    self.context.add_warning(
                        message=f'type {type_str} incompatible with None',
                        span=binary_comparison.span,
                        hint=f'has type {type_str}',
                        additional_spans=additional_spans,
                    )

                if not has_valid_transition:
                    left_type_str = to_display_str(left_type)
                    right_type_str = to_display_str(right_type)
                    always_value = 'False' if isinstance(binary_comparison.comparator, grammar.Equals) else 'True'
                    self.context.add_error(
                        message='left and right sides have incompatible types',
                        span=binary_comparison.span,
                        hint=f'comparison will always result in `{always_value}`',
                        additional_spans=[
                            SpanWithHint(binary_comparison.left.span, hint=f'has type {left_type_str}'),
                            *self._maybe_get_additional_span_for_identifier_definition(
                                binary_comparison.left, left_type_str
                            ),
                            SpanWithHint(binary_comparison.right.span, hint=f'has type {right_type_str}'),
                            *self._maybe_get_additional_span_for_identifier_definition(
                                binary_comparison.right, right_type_str
                            ),
                        ],
                    )
                elif not has_valid_transition_without_unwrap:
                    if compat_left_item is not None and compat_left_item.unwrapped_to_type is not None:
                        self._nodes_to_unwrap.add(id(binary_comparison.left))
                    else:
                        self._nodes_to_unwrap.add(id(binary_comparison.right))

            elif isinstance(binary_comparison.comparator, (grammar.In, grammar.NotIn)):
                # If we have a list and the needle has a different type from the haystack items, issue an error.
                if is_list(right_type):
                    # We *should* have already caught any unsupported types by this point, so this should not raise
                    # and the comparison below should return Ok.
                    item_type = get_list_item_type(right_type)
                    compat_left_item = is_compatible_type(left_type, item_type).unwrap()
                    # We don't gave a good way from here to unwrap items in the list, so don't allow it.
                    compat_item_left = is_compatible_type(item_type, left_type, allow_unwrap=False).unwrap()

                    has_valid_transition = compat_left_item is not None or compat_item_left is not None
                    has_valid_transition_without_unwrap_left = (
                        # We don't need to check for item unwrapping since we didn't allow unwrapping for it.
                        compat_item_left is not None
                        or (compat_left_item is not None and compat_left_item.unwrapped_to_type is None)
                    )

                    if not has_valid_transition:
                        always_value = 'False' if isinstance(binary_comparison.comparator, grammar.In) else 'True'
                        left_type_str = to_display_str(left_type)
                        item_type_str = to_display_str(item_type)
                        right_type_str = to_display_str(right_type)
                        self.context.add_error(
                            message='item has incompatible type with list elements',
                            span=binary_comparison.span,
                            hint=f'comparison will always result in `{always_value}`',
                            additional_spans=[
                                SpanWithHint(binary_comparison.left.span, hint=f'has type {left_type_str}'),
                                *self._maybe_get_additional_span_for_identifier_definition(
                                    binary_comparison.left, left_type_str
                                ),
                                SpanWithHint(
                                    binary_comparison.right.span, hint=f'list items have type {item_type_str}'
                                ),
                                *self._maybe_get_additional_span_for_identifier_definition(
                                    binary_comparison.right, right_type_str
                                ),
                            ],
                        )
                    elif not has_valid_transition_without_unwrap_left:
                        self._nodes_to_unwrap.add(id(binary_comparison.left))

        return self._validate_two_arg_type_transitions(
            binary_comparison.span,
            binary_comparison.comparator.original_comparator,
            binary_comparison.left,
            binary_comparison.right,
            _get_binary_comparison_transitions(),
            allow_any=True,
            valid_transition_hook=valid_transition_hook,
        )

    def _validate_boolean_operation(self, boolean_operation: grammar.BooleanOperation) -> type:
        # Type check left and right sides, but return bool regardless because the underlying `any` and `all` can
        # handle arbitrary types and will always return bools.
        for value in boolean_operation.values:
            value_type = self._validate_expression(value)
            value_type_str = to_display_str(value_type)
            self._check_compatible_type(
                type_t=value_type,
                accepted_by_t=bool,
                message=f'unsupported operand type for `{boolean_operation.operand.original_operand}`',
                node=value,
                hint=f'has type {value_type_str}, expected `bool`',
                additional_spans=self._maybe_get_additional_span_for_identifier_definition(value, value_type_str),
            )

        return bool

    def _validate_attribute(self, attribute: grammar.Attribute) -> type:
        self.context.add_error("attributes aren't supported yet", attribute.span)
        return AnyType

    def _validate_format_string(self, format_string: grammar.FormatString) -> type:
        for name in format_string.names:
            name_type = self._validate_expression(name)
            accepted_types = (int, float, bool, str)
            name_type_str = to_display_str(name_type)
            accepted_types_str = ', '.join(to_display_str(arg) for arg in accepted_types)
            self._check_compatible_type(
                type_t=name_type,
                accepted_by_t=cast(type, Union[accepted_types]),
                message='unsupported type for f-string substitution',
                node=name,
                hint=f'has type {name_type_str}, expected one of {accepted_types_str}',
                additional_spans=self._maybe_get_additional_span_for_identifier_definition(name, name_type_str),
            )
            if not self._name_type_and_span_cache[name.identifier_key].can_extract:
                self.context.add_error(
                    message='format strings can not include non-extractable features',
                    span=name.span,
                    hint=f"name '{name.identifier}' is marked as non-extractable",
                )
        return str

    def _validate_annotation(self, annotation: grammar.Annotation) -> type:
        raise AssertionError("Should not encounter Annotation, we're handling Assignments specially.")

    def _validate_annotation_with_variants(self, annotation_with_variants: grammar.AnnotationWithVariants) -> type:
        raise AssertionError("Should not encounter AnnotationWithVariants, we're handling Assignments specially.")

    def _validate_two_arg_type_transitions(
        self,
        span: grammar.Span,
        transitioner: str,
        left: grammar.Expression,
        right: grammar.Expression,
        valid_type_transitions_by_transitioner: Dict[str, Sequence[_ValidTwoArgTypeTransition]],
        allow_any: bool,
        valid_transition_hook: Optional[Callable[[type, type], None]] = None,
    ) -> type:
        """Utility to help handle something that takes two args.

        :param transitioner: The string representation of the object that's going to take the two arguments and
            convert them to a return value. Used for error messaging and looking up the set of type transitions.
        :param left: The left arg expression.
        :param right: The right arg expression.
        :param valid_type_transitions_by_transitioner: A mapping from transitioner string to valid type transitions
            for that transitioner.
        :param allow_any: Whether to allow `Any` for either of the types.
        :return: The resulting type if it's a valid transition, or Any if it's not. Invalid transitions will add an
            error to the context.
        """
        valid_type_transitions = valid_type_transitions_by_transitioner.get(transitioner)
        assert valid_type_transitions is not None, f'Unknown transitioner {transitioner}'

        left_type = self._validate_expression(left)
        right_type = self._validate_expression(right)

        if allow_any or (left_type != Any and right_type != Any):
            for transition in valid_type_transitions:
                left_result = is_compatible_type(left_type, transition.valid_left_type)
                right_result = is_compatible_type(right_type, transition.valid_right_type)

                had_errors = False
                if left_result.is_err():
                    left_result.unwrap_err().add_error(self.context, left.span)
                    had_errors = True
                if right_result.is_err():
                    right_result.unwrap_err().add_error(self.context, right.span)
                    had_errors = True
                if had_errors:
                    # Bail early, we've got types our type system doesn't support
                    return AnyType

                left_type_info = left_result.unwrap()
                right_type_info = right_result.unwrap()
                if left_type_info is not None and right_type_info is not None:
                    if left_type_info.unwrapped_to_type is not None:
                        left_type = left_type_info.unwrapped_to_type
                        self._nodes_to_unwrap.add(id(left))
                    if right_type_info.unwrapped_to_type is not None:
                        right_type = right_type_info.unwrapped_to_type
                        self._nodes_to_unwrap.add(id(right))
                    if valid_transition_hook is not None:
                        valid_transition_hook(left_type, right_type)
                    return transition.resulting_type

        left_type_str = to_display_str(left_type)
        right_type_str = to_display_str(right_type)
        left_type_str_no_quotes = to_display_str(left_type, include_quotes=False)
        right_type_str_no_quotes = to_display_str(right_type, include_quotes=False)
        self.context.add_error(
            f'unsupported operand types for `{transitioner}`',
            span,
            hint=f'no implementation for `{left_type_str_no_quotes} {transitioner} {right_type_str_no_quotes}`',
            additional_spans=[
                SpanWithHint(left.span, f'has type {left_type_str}'),
                *self._maybe_get_additional_span_for_identifier_definition(left, left_type_str),
                SpanWithHint(right.span, f'has type {right_type_str}'),
                *self._maybe_get_additional_span_for_identifier_definition(right, right_type_str),
            ],
        )
        return AnyType


@lru_cache()
def _get_binary_operation_transitions() -> Dict[str, Sequence[_ValidTwoArgTypeTransition]]:
    # Both ints yields an int
    int_transition = _ValidTwoArgTypeTransition(valid_left_type=int, valid_right_type=int, resulting_type=int)
    # At least one float yields a float
    float_transition = _ValidTwoArgTypeTransition(
        valid_left_type=_INT_OR_FLOAT_T,
        valid_right_type=_INT_OR_FLOAT_T,
        resulting_type=float,
    )
    # NOTE: Order here is important, we want to try the int-only version first.
    number_type_transitions = [int_transition, float_transition]

    operation_to_transitions = {
        grammar.Add: [
            _ValidTwoArgTypeTransition(valid_left_type=str, valid_right_type=str, resulting_type=str),
            *number_type_transitions,
        ],
        grammar.Subtract: number_type_transitions,
        grammar.Multiply: [
            *number_type_transitions,
            _ValidTwoArgTypeTransition(valid_left_type=str, valid_right_type=int, resulting_type=str),
            _ValidTwoArgTypeTransition(valid_left_type=int, valid_right_type=str, resulting_type=str),
        ],
        grammar.Divide: [float_transition],
        grammar.FloorDivide: number_type_transitions,
        grammar.Modulo: number_type_transitions,
        grammar.Pow: number_type_transitions,
        grammar.LeftShift: [int_transition],
        grammar.RightShift: [int_transition],
        grammar.BitwiseOr: [int_transition],
        grammar.BitwiseXor: [int_transition],
        grammar.BitwiseAnd: [int_transition],
    }
    # Key on the sting value of the original operator
    return {operation.original_operator: transitions for operation, transitions in operation_to_transitions.items()}


@lru_cache()
def _get_binary_comparison_transitions() -> Dict[str, Sequence[_ValidTwoArgTypeTransition]]:
    any_to_bool_transition = _ValidTwoArgTypeTransition(
        valid_left_type=AnyType, valid_right_type=AnyType, resulting_type=bool
    )
    number_to_bool_transition = _ValidTwoArgTypeTransition(
        valid_left_type=_INT_OR_FLOAT_T, valid_right_type=_INT_OR_FLOAT_T, resulting_type=bool
    )
    # For "in"/"not in"
    in_transitions = [
        _ValidTwoArgTypeTransition(valid_left_type=str, valid_right_type=str, resulting_type=bool),
        _ValidTwoArgTypeTransition(
            valid_left_type=AnyType, valid_right_type=cast(type, List[Any]), resulting_type=bool
        ),
    ]

    comparators_to_transitions = {
        grammar.Equals: [any_to_bool_transition],
        grammar.NotEquals: [any_to_bool_transition],
        grammar.LessThan: [number_to_bool_transition],
        grammar.LessThanEquals: [number_to_bool_transition],
        grammar.GreaterThan: [number_to_bool_transition],
        grammar.GreaterThanEquals: [number_to_bool_transition],
        grammar.In: in_transitions,
        grammar.NotIn: in_transitions,
    }
    # Key on the sting value of the original comparator
    return {
        comparator.original_comparator: transitions for comparator, transitions in comparators_to_transitions.items()
    }
