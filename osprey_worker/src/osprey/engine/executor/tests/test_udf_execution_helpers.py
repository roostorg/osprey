import pytest
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.executor.external_service_utils import ExternalService
from osprey.engine.executor.udf_execution_helpers import HasHelper, UDFHelpers
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.engine.udf.registry import UDFRegistry


class Arguments(ArgumentsBase):
    # this is checkign that the arguments are hashable
    message: str = ''
    some_list: list[str] = []


class CountingService(ExternalService[Arguments, int]):
    def __init__(self) -> None:
        self.calls: list[Arguments] = []

    def get_from_service(self, key: Arguments) -> int:
        self.calls.append(key)
        return len(self.calls)


class CountingCallsUDF1(HasHelper[CountingService], UDFBase[Arguments, int]):
    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> int:
        return self.accessor_get(execution_context, arguments)

    @classmethod
    def create_provider(cls) -> CountingService:
        return CountingService()


class CountingCallsUDF2(HasHelper[CountingService], UDFBase[Arguments, int]):
    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> int:
        return self.accessor_get(execution_context, arguments)

    @classmethod
    def create_provider(cls) -> CountingService:
        return CountingService()


@pytest.fixture
def udf_registry() -> UDFRegistry:
    return UDFRegistry.with_udfs(CountingCallsUDF1, CountingCallsUDF2)


def test_helper_reuses_service_with_complex_key(execute: ExecuteFunction) -> None:
    helpers = (
        UDFHelpers()
        .set_udf_helper(CountingCallsUDF1, CountingCallsUDF1.create_provider())
        .set_udf_helper(CountingCallsUDF2, CountingCallsUDF2.create_provider())
    )
    data = execute(
        """
        _MessageContent = 'this is weird'
        r1 = CountingCallsUDF1(message=_MessageContent)
        r2 = CountingCallsUDF2(message=_MessageContent)
        """,
        udf_helpers=helpers,
    )

    # the service should only be called once even tho two different udfs access it
    assert 1 == data['r1'] == data['r2']


def test_helper_raises_unhashable_type_error(execute: ExecuteFunction) -> None:
    helper = CountingService()
    helpers = UDFHelpers().set_udf_helper(CountingCallsUDF1, helper).set_udf_helper(CountingCallsUDF2, helper)
    with pytest.raises(TypeError, match='unhashable type'):
        execute(
            """
            _MessageContent = 'this is weird'
            r1 = CountingCallsUDF1(some_list=[_MessageContent])
            r2 = CountingCallsUDF2(some_list=[_MessageContent])
            """,
            udf_helpers=helpers,
        )
