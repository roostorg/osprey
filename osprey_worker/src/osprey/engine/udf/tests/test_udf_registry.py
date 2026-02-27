from collections.abc import Sequence
from typing import Any, Generic, Type, TypeVar, cast

import pytest
from osprey.engine.conftest import CheckOutputFunction
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.language_types.osprey_invariant_generic import OspreyInvariantGeneric
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.engine.udf.registry import UDFRegistry
from osprey.engine.udf.type_helpers import UnsupportedTypeError


class Arguments(ArgumentsBase):
    foo: str


def make_test_udf() -> Type[UDFBase[Arguments, str]]:
    class Test(UDFBase[Arguments, str]):
        def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:  # type: ignore[empty-body]
            pass

    return Test


def test_udf_registry() -> None:
    registry = UDFRegistry()
    Test = make_test_udf()
    registry.register(Test)

    assert registry.get('Test') is Test


def test_udf_registry_with_udfs() -> None:
    Test = make_test_udf()
    registry = UDFRegistry.with_udfs(Test)
    assert registry.get('Test') is Test


def test_merge() -> None:
    class Arguments2(ArgumentsBase):
        foo: str

    class Test2(UDFBase[Arguments2, str]):
        def execute(  # type: ignore[empty-body]
            self, execution_context: ExecutionContext, arguments: Arguments2
        ) -> str:
            pass

    Test = make_test_udf()
    registry = UDFRegistry.with_udfs(Test)
    registry_2 = UDFRegistry.with_udfs(Test2)
    registry_2.merge(registry)

    assert registry.get('Test2') is None
    assert registry.get('Test') is Test
    assert registry_2.get('Test') is Test
    assert registry_2.get('Test2') is Test2


def test_merge_conflict() -> None:
    Test = make_test_udf()
    Test2 = make_test_udf()

    registry = UDFRegistry.with_udfs(Test)
    registry_2 = UDFRegistry.with_udfs(Test2)

    with pytest.raises(Exception) as e:
        registry_2.merge(registry)

    assert e.match('Could not merge registry, the following functions exist in both registries: Test')


def test_no_merge_conflict() -> None:
    Test = make_test_udf()

    registry = UDFRegistry.with_udfs(Test)
    registry_2 = UDFRegistry.with_udfs(Test)
    registry_2.merge(registry)

    assert registry_2.get('Test') is Test


def _get_invalid_types() -> Sequence[Type[UDFBase[Any, Any]]]:
    # Complex types
    class ComplexParamArguments(ArgumentsBase):
        # This is a type you can't write in the rules language
        p: tuple[str, ...]

    class ComplexParamUdf(UDFBase[ComplexParamArguments, str]):
        def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> str:
            return ''

    class ComplexReturnUdf(UDFBase[ArgumentsBase, tuple[str, ...]]):
        def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> tuple[str, ...]:
            return tuple()

    # Generics
    A = TypeVar('A')
    B = TypeVar('B')

    class InheritsWrongGenericsUdf(Generic[A], UDFBase[ArgumentsBase, A]):
        def execute(  # type: ignore[empty-body]
            self, execution_context: ExecutionContext, arguments: ArgumentsBase
        ) -> A:
            pass

    class DoesNotReturnGenericUdf(OspreyInvariantGeneric[A], UDFBase[ArgumentsBase, None]):
        def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> None:
            pass

    class ReturnsDifferentTypeVarDirectUdf(
        OspreyInvariantGeneric[A],
        UDFBase[ArgumentsBase, B],
    ):
        def execute(  # type: ignore[empty-body]
            self, execution_context: ExecutionContext, arguments: ArgumentsBase
        ) -> B:
            pass

    class ArgumentInheritsWrongGenericArgs(Generic[A], ArgumentsBase):
        foo: A

    class ArgumentInheritsWrongGenericUdf(OspreyInvariantGeneric[A], UDFBase[ArgumentInheritsWrongGenericArgs[A], A]):
        def execute(  # type: ignore[empty-body]
            self, execution_context: ExecutionContext, arguments: ArgumentsBase
        ) -> A:
            pass

    class ArgumentGenericWhenUdfIsNotArgs(OspreyInvariantGeneric[A], ArgumentsBase):
        foo: A

    class ArgumentGenericWhenUdfIsNotUdf(UDFBase[ArgumentGenericWhenUdfIsNotArgs[B], None]):
        def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> None:
            pass

    class ArgumentHasDifferentTypeVarArgs(OspreyInvariantGeneric[A], ArgumentsBase):
        foo: A

    class ArgumentHasDifferentTypeVarUdf(OspreyInvariantGeneric[B], UDFBase[ArgumentHasDifferentTypeVarArgs[A], B]):
        def execute(  # type: ignore[empty-body]
            self, execution_context: ExecutionContext, arguments: ArgumentsBase
        ) -> B:
            pass

    class ArgumentDoesNotUseTypeArgs(OspreyInvariantGeneric[A], ArgumentsBase):
        pass

    class ArgumentDoesNotUseTypeUdf(OspreyInvariantGeneric[A], UDFBase[ArgumentDoesNotUseTypeArgs[A], A]):
        def execute(  # type: ignore[empty-body]
            self, execution_context: ExecutionContext, arguments: ArgumentsBase
        ) -> A:
            pass

    return (
        ComplexParamUdf,
        ComplexReturnUdf,
        InheritsWrongGenericsUdf,
        DoesNotReturnGenericUdf,
        ReturnsDifferentTypeVarDirectUdf,
        ArgumentInheritsWrongGenericUdf,
        ArgumentGenericWhenUdfIsNotUdf,
        ArgumentHasDifferentTypeVarUdf,
        ArgumentDoesNotUseTypeUdf,
    )


@pytest.mark.parametrize('t', _get_invalid_types(), ids=lambda x: cast(type, x).__name__)
def test_cannot_register_invalid_types(t: Type[UDFBase[Any, Any]], check_output: CheckOutputFunction) -> None:
    with pytest.raises(UnsupportedTypeError) as e:
        UDFRegistry.with_udfs(t)

    assert check_output(str(e.value))


def test_valid_generic_functions() -> None:
    A = TypeVar('A')

    class ReturnsTypeVarDirectlyUdf(OspreyInvariantGeneric[A], UDFBase[ArgumentsBase, A]):
        def execute(  # type: ignore[empty-body]
            self, execution_context: ExecutionContext, arguments: ArgumentsBase
        ) -> A:
            pass

    class ReturnsTypeVarIndirectlyUdf(OspreyInvariantGeneric[A], UDFBase[ArgumentsBase, A | None]):
        def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> A | None:
            pass

    class ArgumentsUseTypeVarsDirectlyArgs(OspreyInvariantGeneric[A], ArgumentsBase):
        foo: A

    class ArgumentsUseTypeVarsDirectlyUdf(OspreyInvariantGeneric[A], UDFBase[ArgumentsUseTypeVarsDirectlyArgs[A], A]):
        def execute(  # type: ignore[empty-body]
            self, execution_context: ExecutionContext, arguments: ArgumentsBase
        ) -> A:
            pass

    class ArgumentsUseTypeVarsOptionalArgs(OspreyInvariantGeneric[A], ArgumentsBase):
        foo: A | None

    class ArgumentsUseTypeVarsOptionalUdf(OspreyInvariantGeneric[A], UDFBase[ArgumentsUseTypeVarsOptionalArgs[A], A]):
        def execute(  # type: ignore[empty-body]
            self, execution_context: ExecutionContext, arguments: ArgumentsBase
        ) -> A:
            pass

    class ArgumentsUseTypeVarsListArgs(OspreyInvariantGeneric[A], ArgumentsBase):
        foo: list[A]

    class ArgumentsUseTypeVarsListUdf(OspreyInvariantGeneric[A], UDFBase[ArgumentsUseTypeVarsListArgs[A], A]):
        def execute(  # type: ignore[empty-body]
            self, execution_context: ExecutionContext, arguments: ArgumentsBase
        ) -> A:
            pass

    UDFRegistry.with_udfs(ReturnsTypeVarDirectlyUdf)
    UDFRegistry.with_udfs(ReturnsTypeVarIndirectlyUdf)
    UDFRegistry.with_udfs(ArgumentsUseTypeVarsDirectlyUdf)
    UDFRegistry.with_udfs(ArgumentsUseTypeVarsOptionalUdf)
    UDFRegistry.with_udfs(ArgumentsUseTypeVarsListUdf)
