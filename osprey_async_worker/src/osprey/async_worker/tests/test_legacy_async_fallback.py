import pytest
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.engine.udf.registry import UDFRegistry


class LegacyAsyncUDFArguments(ArgumentsBase):
    pass


class LegacyAsyncUDF(UDFBase[LegacyAsyncUDFArguments, str]):
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: LegacyAsyncUDFArguments) -> str:
        return 'legacy_result'


@pytest.mark.asyncio
async def test_legacy_async_udf_runs_through_executor_fallback(async_execute_with_result):
    """run a legacy async udf through the executor fallback"""
    registry = UDFRegistry.with_udfs(LegacyAsyncUDF)
    result = await async_execute_with_result(
        'Result: str = LegacyAsyncUDF()',
        udf_registry=registry,
    )

    assert result.error_infos == []
    assert result.extracted_features['Result'] == 'legacy_result'
