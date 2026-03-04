from dataclasses import dataclass
from typing import Any, Callable, List, Optional

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_extractable_feature_types import (
    ValidateExtractableFeatureTypes,
    _is_extractable_type,
)
from osprey.engine.conftest import CheckFailureFunction, RunValidationFunction
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.language_types.entities import EntityT
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.engine.udf.registry import UDFRegistry


# Custom class type that should not be extractable
@dataclass
class CustomResultT:
    value: str


class EmptyArguments(ArgumentsBase):
    pass


class ReturnsCustomType(UDFBase[EmptyArguments, CustomResultT]):
    """UDF that returns a custom class type."""

    def execute(self, execution_context: ExecutionContext, arguments: EmptyArguments) -> CustomResultT:
        return CustomResultT(value='test')


class ReturnsPrimitive(UDFBase[EmptyArguments, str]):
    """UDF that returns a primitive type."""

    def execute(self, execution_context: ExecutionContext, arguments: EmptyArguments) -> str:
        return 'test'


class TestIsExtractableType:
    """Unit tests for the _is_extractable_type function."""

    def test_primitives_are_extractable(self) -> None:
        assert _is_extractable_type(str) is True
        assert _is_extractable_type(int) is True
        assert _is_extractable_type(float) is True
        assert _is_extractable_type(bool) is True
        assert _is_extractable_type(type(None)) is True

    def test_optional_primitives_are_extractable(self) -> None:
        assert _is_extractable_type(Optional[str]) is True
        assert _is_extractable_type(Optional[int]) is True
        assert _is_extractable_type(Optional[float]) is True
        assert _is_extractable_type(Optional[bool]) is True

    def test_list_of_primitives_is_extractable(self) -> None:
        assert _is_extractable_type(List[str]) is True
        assert _is_extractable_type(List[int]) is True
        assert _is_extractable_type(List[float]) is True
        assert _is_extractable_type(List[bool]) is True

    def test_nested_optionals_and_lists_are_extractable(self) -> None:
        assert _is_extractable_type(Optional[List[str]]) is True
        assert _is_extractable_type(List[Optional[int]]) is True

    def test_entity_is_extractable(self) -> None:
        # EntityT converts to int post-execution
        assert _is_extractable_type(EntityT[str]) is True

    def test_custom_class_is_not_extractable(self) -> None:
        assert _is_extractable_type(CustomResultT) is False

    def test_optional_custom_class_is_not_extractable(self) -> None:
        assert _is_extractable_type(Optional[CustomResultT]) is False

    def test_list_of_custom_class_is_not_extractable(self) -> None:
        assert _is_extractable_type(List[CustomResultT]) is False


# Integration tests using the validator
pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateExtractableFeatureTypes, UniqueStoredNames]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(ReturnsCustomType, ReturnsPrimitive)),
]


def test_primitive_features_are_allowed(run_validation: RunValidationFunction) -> None:
    """Primitive types should be allowed as extracted features."""
    run_validation(
        """
        MyString = "hello"
        MyInt = 123
        MyFloat = 1.5
        MyBool = True
        """
    )


def test_primitive_udf_result_is_allowed(run_validation: RunValidationFunction) -> None:
    """UDFs returning primitive types should be allowed as extracted features."""
    run_validation('MyResult = ReturnsPrimitive()')


def test_local_variables_with_custom_type_are_allowed(run_validation: RunValidationFunction) -> None:
    """Local variables (starting with _) are not extracted, so custom types are OK."""
    run_validation('_LocalResult = ReturnsCustomType()')


def test_custom_type_as_extracted_feature_fails(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    """UDFs returning custom types should fail when assigned to extracted features."""
    with check_failure():
        run_validation('MyResult = ReturnsCustomType()')
