from typing import TYPE_CHECKING

from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.grammar import Assign, Call, Name, Source
from osprey.engine.ast_validator.base_validator import SourceValidator
from osprey.engine.ast_validator.validation_utils import add_must_assign_to_variable_error
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.udf.rvalue_type_checker import (
    AnnotationConversionError,
    convert_ast_annotation_to_type_checker,
)
from osprey.engine.udf.type_evaluator import is_compatible_type
from osprey.engine.udf.type_helpers import AnyType, to_display_str

if TYPE_CHECKING:
    from osprey.engine.ast_validator.validation_context import ValidationContext


# NEED TO EXCLUDE, but validate_static_types depends on this
class ValidateDynamicCallsHaveAnnotatedRValue(SourceValidator):
    """
    Validates that if a dynamic function is called, it's got an RValue Type
    """

    def __init__(self, context: 'ValidationContext'):
        super().__init__(context)
        self._udf_node_mapping = context.get_validator_result(ValidateCallKwargs)

    def validate_source(self, source: Source) -> None:
        for call_node in filter_nodes(source.ast_root, Call):
            self.validate_call_node(call_node)

    def validate_call_node(self, call_node: Call) -> None:
        assert isinstance(call_node.func, Name), 'call nodes with attribute not yet supported.'
        function_name = call_node.func.identifier

        # Lookup function in UDF. Validation of function existence takes place in
        # ValidateCallKwargs.
        udf_class = self.context.udf_registry.get(function_name)
        if not udf_class:
            return

        if not udf_class.has_dynamic_result():
            return

        assign = call_node.parent
        if not isinstance(assign, Assign):
            # If we have a generic return type, let's try to make the example look realistic based on that return type.
            # Otherwise just some scalar type ("str" in this case).
            if udf_class.is_generic():
                type_annotation_type = udf_class.get_substituted_generic_rvalue_type(str)
            else:
                type_annotation_type = str

            add_must_assign_to_variable_error(
                self.context,
                message=(
                    f'`{function_name}(...)` returns a dynamic result, and thus must be assigned to a variable '
                    f'with a type annotation'
                ),
                node=call_node,
                type_annotation=to_display_str(type_annotation_type, include_quotes=False),
            )
            return

        if not assign.annotation:
            self.context.add_error(
                message=(
                    f'`{function_name}(...)` returns a dynamic result, and the assignment must have a type annotation'
                ),
                span=assign.span,
                hint=f'add a type annotation, like: `{assign.target.identifier}: str = {function_name}(...)`',
            )
            return

        try:
            rvalue_type_checker = convert_ast_annotation_to_type_checker(node=assign.annotation)
        except AnnotationConversionError as e:
            self.context.add_error(
                message=e.message,
                span=e.span,
                hint=e.hint,
                additional_spans=e.additional_spans,
                additional_spans_message=e.additional_spans_message,
            )
            return

        if udf_class.is_generic():
            # If our UDF is generic, we need to ensure that the chosen type is compatible with the return type. If our
            # return type is just `T`, that's trivially always going to be true, but if our return type is something
            # like `Entity[T]` and we have the annotation be `List[int]` that cannot be true. So, in these cases, we
            # need to make sure to raise an error.
            compatible_type_result = is_compatible_type(
                rvalue_type_checker.to_typing_type(), udf_class.get_substituted_generic_rvalue_type(AnyType)
            )
            if compatible_type_result.is_err():
                compatible_type_result.unwrap_err().add_error(self.context, assign.annotation.span)

            if compatible_type_result.unwrap() is None:
                self.context.add_error(
                    message='annotation is incompatible with generic return type',
                    span=assign.annotation.span,
                    hint=(
                        f'annotation is incompatible with {to_display_str(udf_class)}'
                        f' return type {to_display_str(udf_class.get_rvalue_type())}'
                    ),
                )

        udf, _ = self._udf_node_mapping[id(call_node)]
        udf.set_rvalue_type_checker(rvalue_type_checker)
