from __future__ import annotations

from typing import Any, Callable, Dict, List, Type

import pytest
from osprey.engine.ast_validator.validation_context import ValidationContext
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import CheckFailureFunction, ExecuteFunction, RunValidationFunction
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase, ConstExpr
from osprey.engine.udf.base import UDFBase
from osprey.engine.udf.registry import UDFRegistry

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs, UniqueStoredNames]),
    pytest.mark.use_osprey_stdlib,
]


def test_validate_call_kwargs_no_errors(run_validation: RunValidationFunction) -> None:
    run_validation(
        """
        UserId: str = JsonData(path='$.foo')
        Foo = Entity(type='User', id=UserId)
        """
    )


@pytest.fixture
def udf_with_usub() -> Type[UDFBase[Any, Any]]:
    class DocArguments(ArgumentsBase):
        foo: ConstExpr[int] = ConstExpr.for_default('bar', 6)

    class UDFWithUSub(UDFBase[DocArguments, int]):
        def execute(  # type: ignore[empty-body]
            self, execution_context: ExecutionContext, arguments: DocArguments
        ) -> bool:
            pass

    return UDFWithUSub


def test_validate_call_kwargs_with_unary_ops(
    run_validation: RunValidationFunction, udf_registry: UDFRegistry, udf_with_usub: Type[UDFBase[Any, Any]]
) -> None:
    udf_registry.register(udf_with_usub)

    run_validation(
        """
        UserId: str = UDFWithUSub(foo=-2)
        """
    )

    run_validation(
        """
        S = -2
        UserId: str = UDFWithUSub(foo=S)
        """
    )


def test_validate_call_kwargs_with_unary_ops_error(
    run_validation: RunValidationFunction,
    udf_registry: UDFRegistry,
    check_failure: CheckFailureFunction,
    udf_with_usub: Type[UDFBase[Any, Any]],
) -> None:
    udf_registry.register(udf_with_usub)

    with check_failure():
        run_validation(
            """
            UserId: str = UDFWithUSub(foo=-'s')
            """
        )


def test_validate_unknown_function(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation('Foo = Bar()')


def test_allow_optional_args(run_validation: RunValidationFunction, udf_registry: UDFRegistry) -> None:
    class OptionalArgsArguments(ArgumentsBase):
        required: str
        optional: str = 'hello'

    class OptionalArgsUdf(UDFBase[OptionalArgsArguments, None]):
        def execute(self, execution_context: ExecutionContext, arguments: OptionalArgsArguments) -> None:
            pass

    udf_registry.register(OptionalArgsUdf)

    run_validation('OptionalArgsUdf(required="hi")')


def test_missing_kwarg(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation('Foo: str = JsonData()')


def test_unknown_keyword_argument(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation('Foo: str = JsonData(path=1, bad=1)')


def test_missing_keyword_argument(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation('Foo: str = JsonData()')


class UnexpectedArgsArguments(ArgumentsBase):
    required: str
    optional: str = 'hello'
    extra_arguments: Dict[str, str]


class UnexpectedArgsUdfBase:
    def execute(self, execution_context: ExecutionContext, arguments: UnexpectedArgsArguments) -> List[str]:
        return [
            i
            for kv in {
                'required': arguments.required,
                'optional': arguments.optional,
                **arguments.extra_arguments,
            }.items()
            for i in kv
        ]


@pytest.mark.parametrize(
    'extra_arguments',
    [{'required': 'hi'}, {'required': 'hi', 'optional': 'hi'}, {'required': 'hi', 'unexpected': 'world'}],
)
def test_allow_unexpected_args(
    extra_arguments: Dict[str, str], execute: ExecuteFunction, udf_registry: UDFRegistry
) -> None:
    @udf_registry.register
    class UnexpectedArgsUdf(UnexpectedArgsUdfBase, UDFBase[UnexpectedArgsArguments, List[str]]):
        pass

    extra_arguments_str = ', '.join(f'{k}={v!r}' for k, v in extra_arguments.items())
    ret = execute(f'Ret = UnexpectedArgsUdf({extra_arguments_str})')['Ret']
    assert isinstance(ret, list), 'mypy'

    ret_data = dict(zip(ret[0::2], ret[1::2]))
    expected_data = {'optional': UnexpectedArgsArguments.optional, **extra_arguments}

    assert ret_data == expected_data


@pytest.mark.parametrize(
    'extra_arguments',
    [
        {'required': 'hi', 'unexpected': 7},
        {'required': 'hi', 'unexpected': None},
        {'required': 'hi', 'unexpected': 3.7},
        {'required': 'hi', 'unexpected': ['hello']},
    ],
)
def test_allow_unexpected_respects_type(
    extra_arguments: Dict[str, str],
    execute: ExecuteFunction,
    check_failure: CheckFailureFunction,
    udf_registry: UDFRegistry,
) -> None:
    @udf_registry.register
    class ValidatingUnexpectedArgsUdf(UnexpectedArgsUdfBase, UDFBase[UnexpectedArgsArguments, List[str]]):
        def __init__(self, validation_context: ValidationContext, arguments: UnexpectedArgsArguments):
            assert set(arguments.get_extra_arguments_ast().keys()) == {'unexpected'}

    with check_failure():
        extra_arguments_str = ', '.join(f'{k}={v!r}' for k, v in extra_arguments.items())
        execute(f'Ret = ValidatingUnexpectedArgsUdf({extra_arguments_str})')
