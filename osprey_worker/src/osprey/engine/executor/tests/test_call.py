from typing import Any

import pytest
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import InvalidDynamicReturnType, UDFBase
from osprey.engine.udf.registry import UDFRegistry

from ...conftest import ExecuteFunction, ExecuteWithResultFunction


class Arguments(ArgumentsBase):
    a: int
    b: int


class Add(UDFBase[Arguments, int]):
    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> int:
        return arguments.a + arguments.b


pytestmark = pytest.mark.use_udf_registry(UDFRegistry.with_udfs(Add))


def test_call(execute: ExecuteFunction) -> None:
    data = execute(
        """
        A = 1
        B = 2
        Ret = Add(a=A, b=B)
        Ret2 = Add(a=10, b=5)
        Ret3 = Add(a=10, b=Ret)
        """
    )
    assert data == {'Ret': 3, 'Ret2': 15, 'Ret3': 13}


def test_dynamic_result_coerces_type(udf_registry: UDFRegistry, execute_with_result: ExecuteWithResultFunction) -> None:
    class Test(UDFBase[ArgumentsBase, Any]):
        def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> Any:
            return 1

    udf_registry.register(Test)

    result = execute_with_result(
        """
        A: int = Test()
        B: List[str] = Test()
        """
    )

    assert result.extracted_features['A'] == 1
    assert result.extracted_features['B'] is None
    assert len(result.error_infos) == 1
    e_info = result.error_infos[0]
    assert isinstance(e_info.error, InvalidDynamicReturnType)
    assert str(e_info.error) == (
        'Function `Test` with dynamic return type returned `int` but was expected to return `List[str]`.'
    )


def test_uses_default_value_if_no_other_provided(udf_registry: UDFRegistry, execute: ExecuteFunction) -> None:
    class OptionalArgsArguments(ArgumentsBase):
        required: str
        optional: str = 'hello'

    class OptionalArgsUdf(UDFBase[OptionalArgsArguments, str]):
        def execute(self, execution_context: ExecutionContext, arguments: OptionalArgsArguments) -> str:
            return arguments.required + ' ' + arguments.optional

    udf_registry.register(OptionalArgsUdf)

    data = execute(
        """
        A = OptionalArgsUdf(required="hi")
        B = OptionalArgsUdf(required="hey", optional="there")
        """
    )

    assert data == {'A': 'hi hello', 'B': 'hey there'}
