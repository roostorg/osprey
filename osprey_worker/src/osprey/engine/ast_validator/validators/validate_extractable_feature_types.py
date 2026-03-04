"""
Validator to ensure that only serializable types are used as extracted features.

Extracted features are exposed in the UI and stored in BigQuery, so they must be
types that can be serialized to JSON. This validator prevents custom class types
(like custom dataclasses, EffectBase, etc.) from being assigned to non-local
variables that would be extracted.

To use a custom type without extraction, prefix the variable name with `_` to make
it a local variable:
    _VC1 = CustomUdf(entity=TestUser)  # OK - not extracted
    VC1 = CustomUdf(entity=TestUser)   # ERROR - would be extracted
"""

from typing import TYPE_CHECKING

import typing_inspect
from osprey.engine.ast import grammar
from osprey.engine.language_types.entities import EntityT
from osprey.engine.udf.type_helpers import to_display_str

from ..base_validator import SourceValidator
from .validate_static_types import ValidateStaticTypes, ValidateStaticTypesResult

if TYPE_CHECKING:
    from ..validation_context import ValidationContext


# Types that are allowed as extracted features
_PRIMITIVE_TYPES = {str, int, bool, float, type(None)}


def _is_extractable_type(t: type) -> bool:
    """
    Check if a type can be serialized for extraction.

    Allowed types:
    - Primitives: str, int, float, bool, NoneType
    - Optional[T] where T is extractable
    - List[T] where T is extractable
    - EntityT[...] (converts to int post-execution)
    """
    # Check primitives
    if t in _PRIMITIVE_TYPES:
        return True

    # Check EntityT (these convert to int post-execution)
    origin = typing_inspect.get_origin(t)
    if origin is EntityT:
        return True

    # Check Optional[T] (which is Union[T, None] at runtime)
    if typing_inspect.is_union_type(t):
        args = typing_inspect.get_args(t)
        non_none_args = [a for a in args if a is not type(None)]  # noqa: E721
        # Optional is a Union with exactly one non-None type
        if len(non_none_args) == 1:
            return _is_extractable_type(non_none_args[0])
        # Other Union types are not supported
        return False

    # Check List[T]
    if origin is list:
        args = typing_inspect.get_args(t)
        if len(args) == 1:
            return _is_extractable_type(args[0])
        return False

    # Any other type is not extractable
    return False


class ValidateExtractableFeatureTypes(SourceValidator):
    """
    Validates that extracted features have types that can be serialized.

    This prevents custom class types (like custom dataclasses, EffectBase, LabelEffect, etc.)
    from being assigned to non-local variables, which would cause serialization errors in the UI.
    """

    def __init__(self, context: 'ValidationContext'):
        super().__init__(context)
        # Depend on ValidateStaticTypes to get type information
        context.validator_depends_on([ValidateStaticTypes])
        self._static_types_result: ValidateStaticTypesResult = context.get_validator_result(ValidateStaticTypes)

    def validate_source(self, source: grammar.Source) -> None:
        # Check each feature that will be extracted
        for name, type_and_span in self._static_types_result.name_type_and_span_cache.items():
            # Skip if this feature won't be extracted (local variables starting with _)
            if not type_and_span.should_extract:
                continue

            # Skip if this feature is from a different source file
            if type_and_span.span.source != source:
                continue

            # Check if the type is extractable
            if not _is_extractable_type(type_and_span.type):
                type_str = to_display_str(type_and_span.type)
                # Get a simple name for the type
                simple_type_name = getattr(type_and_span.type, '__name__', type_str)

                self.context.add_error(
                    message=f'cannot extract feature with type `{simple_type_name}`',
                    span=type_and_span.span,
                    hint=(
                        f'type `{type_str}` cannot be serialized for extraction.\n'
                        f'Extracted features must be primitive types (str, int, float, bool), '
                        f'Optional[T], List[T], or EntityT.\n\n'
                        f'To use this value without extraction, prefix the variable name with `_`:\n'
                        f'    _{name} = ...  # local variable, not extracted'
                    ),
                )
