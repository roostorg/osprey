"""Tests for the async executor.

Validates that the async executor produces the same results as the gevent
executor for stdlib UDFs (pure computation, no I/O).
"""

import asyncio
from typing import Sequence

import pytest
from osprey.async_worker.adaptor.interfaces import AsyncBatchableUDFBase, AsyncUDFBase
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.registry import UDFRegistry
from result import Ok, Result


class CancellationArguments(ArgumentsBase):
    value: str


class BatchCancellationArguments(ArgumentsBase):
    id: str
    routing_key: str


@pytest.mark.asyncio
async def test_execute_sync_udfs(async_execute_fn):
    """Sync stdlib UDFs run inline and produce correct results."""
    result = await async_execute_fn(
        """
        Msg: str = JsonData(path="$.message", coerce_type=True)
        MessageLength = StringLength(s=Msg)
        """,
        data={'message': 'hello world'},
    )
    assert result['MessageLength'] == 11


@pytest.mark.asyncio
async def test_execute_json_data(async_execute_fn):
    """JsonData UDF extracts values from action data."""
    result = await async_execute_fn(
        'Username: str = JsonData(path="$.user.name", coerce_type=True)',
        data={'user': {'name': 'alice'}},
    )
    assert result['Username'] == 'alice'


@pytest.mark.asyncio
async def test_execute_multiple_udfs(async_execute_fn):
    """Multiple UDFs in a single execution graph resolve correctly."""
    result = await async_execute_fn(
        """
        Name: str = JsonData(path="$.name", coerce_type=True)
        NameLength = StringLength(s=Name)
        NameLower = StringToLower(s=Name)
        """,
        data={'name': 'HELLO'},
    )
    assert result['Name'] == 'HELLO'
    assert result['NameLength'] == 5
    assert result['NameLower'] == 'hello'


@pytest.mark.asyncio
async def test_execute_dependent_chain(async_execute_fn):
    """UDFs with dependencies resolve in correct order."""
    result = await async_execute_fn(
        """
        Raw: str = JsonData(path="$.text", coerce_type=True)
        Stripped = StringStrip(s=Raw)
        Lower = StringToLower(s=Stripped)
        Length = StringLength(s=Lower)
        """,
        data={'text': '  Hello World  '},
    )
    assert result['Raw'] == '  Hello World  '
    assert result['Stripped'] == 'Hello World'
    assert result['Lower'] == 'hello world'
    assert result['Length'] == 11


@pytest.mark.asyncio
async def test_execute_with_rules(async_execute_fn):
    """Rule evaluation works correctly."""
    result = await async_execute_fn(
        """
        Txt: str = JsonData(path="$.text", coerce_type=True)
        Length = StringLength(s=Txt)
        IsLong = Rule(
            when_all=[Length > 10],
            description="Text is long",
        )
        """,
        data={'text': 'short'},
    )
    assert result['Length'] == 5
    assert result['IsLong'] is False

    result = await async_execute_fn(
        """
        Txt: str = JsonData(path="$.text", coerce_type=True)
        Length = StringLength(s=Txt)
        IsLong = Rule(
            when_all=[Length > 10],
            description="Text is long",
        )
        """,
        data={'text': 'this is a longer text'},
    )
    assert result['IsLong'] is True


@pytest.mark.asyncio
async def test_execute_empty_rules(async_execute_with_result):
    """Empty rules produce a valid ExecutionResult with no errors."""
    result = await async_execute_with_result(
        '# empty rules file',
        data={},
    )
    assert result is not None
    assert len(result.error_infos) == 0


@pytest.mark.asyncio
async def test_execute_missing_json_path(async_execute_fn):
    """Missing JSON path returns None, not an error."""
    result = await async_execute_fn(
        'Value: str = JsonData(path="$.nonexistent", coerce_type=True)',
        data={'something': 'else'},
        allow_errors=True,
    )
    assert result['Value'] is None


@pytest.mark.asyncio
async def test_execute_sync_only_mode(async_execute_with_result):
    """With max_concurrent=0, everything runs synchronously."""
    result = await async_execute_with_result(
        """
        Txt: str = JsonData(path="$.text", coerce_type=True)
        Value = StringLength(s=Txt)
        """,
        data={'text': 'test'},
        max_concurrent=0,
    )
    assert result.extracted_features['Value'] == 4
    assert len(result.error_infos) == 0


@pytest.mark.asyncio
async def test_execution_result_has_expected_fields(async_execute_with_result):
    """ExecutionResult contains all expected fields."""
    result = await async_execute_with_result(
        'Name: str = JsonData(path="$.name", coerce_type=True)',
        data={'name': 'test'},
        action_name='test_action',
        action_id=42,
    )
    assert result.action.action_name == 'test_action'
    assert result.action.action_id == 42
    assert '__action_id' in result.extracted_features
    assert '__timestamp' in result.extracted_features
    assert '__error_count' in result.extracted_features
    assert result.extracted_features['__error_count'] == 0


@pytest.mark.asyncio
async def test_string_operations(async_execute_fn):
    """Various string UDFs work correctly."""
    result = await async_execute_fn(
        """
        Text: str = JsonData(path="$.text", coerce_type=True)
        Upper = StringToUpper(s=Text)
        StartsWith = StringStartsWith(s=Text, start="hello")
        EndsWith = StringEndsWith(s=Text, end="world")
        """,
        data={'text': 'hello world'},
    )
    assert result['Upper'] == 'HELLO WORLD'
    assert result['StartsWith'] is True
    assert result['EndsWith'] is True


@pytest.mark.asyncio
async def test_parity_complex_graph(async_execute_fn):
    """Complex dependency graph produces correct results."""
    result = await async_execute_fn(
        """
        A: str = JsonData(path="$.a", coerce_type=True)
        B: str = JsonData(path="$.b", coerce_type=True)
        LenA = StringLength(s=A)
        LenB = StringLength(s=B)
        ALower = StringToLower(s=A)
        BUpper = StringToUpper(s=B)
        RuleA = Rule(when_all=[LenA > 3], description="A is long")
        RuleB = Rule(when_all=[LenB > 3], description="B is long")
        """,
        data={'a': 'Hello', 'b': 'Hi'},
    )
    assert result['LenA'] == 5
    assert result['LenB'] == 2
    assert result['ALower'] == 'hello'
    assert result['BUpper'] == 'HI'
    assert result['RuleA'] is True
    assert result['RuleB'] is False


@pytest.mark.asyncio
async def test_cancelled_udf_task_cancels_execution(async_execute_with_result):
    started = asyncio.Event()
    cancelled = asyncio.Event()

    class SlowUDF(AsyncUDFBase[CancellationArguments, str]):
        @classmethod
        def _get_udf_base_args(cls):
            return (CancellationArguments, str)

        async def async_execute(self, execution_context: ExecutionContext, arguments: CancellationArguments) -> str:
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

    execution = asyncio.create_task(
        async_execute_with_result(
            'Result = SlowUDF(value="test")',
            udf_registry=UDFRegistry.with_udfs(SlowUDF),
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1)

    udf_tasks = [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and getattr(task.get_coro(), '__name__', '') == '_execute_async_udf'
    ]
    assert len(udf_tasks) == 1
    udf_tasks[0].cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(execution, timeout=1)
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_cancelled_execution_cancels_owned_udf_tasks(async_execute_with_result):
    singlet_started = asyncio.Event()
    singlet_cancelled = asyncio.Event()
    singlet_release = asyncio.Event()
    batch_started = asyncio.Event()
    batch_cancelled = asyncio.Event()
    batch_release = asyncio.Event()

    class SlowSingletUDF(AsyncUDFBase[CancellationArguments, str]):
        @classmethod
        def _get_udf_base_args(cls):
            return (CancellationArguments, str)

        async def async_execute(self, execution_context: ExecutionContext, arguments: CancellationArguments) -> str:
            singlet_started.set()
            try:
                await singlet_release.wait()
            except asyncio.CancelledError:
                singlet_cancelled.set()
                raise
            return arguments.value

    class SlowBatchUDF(AsyncBatchableUDFBase[BatchCancellationArguments, str, BatchCancellationArguments]):
        @classmethod
        def _get_udf_base_args(cls):
            return (BatchCancellationArguments, str, BatchCancellationArguments)

        def get_batchable_arguments(self, arguments: BatchCancellationArguments) -> BatchCancellationArguments:
            return arguments

        async def async_execute(
            self, execution_context: ExecutionContext, arguments: BatchCancellationArguments
        ) -> str:
            return arguments.id

        async def async_execute_batch(
            self,
            execution_context: ExecutionContext,
            udfs: Sequence,
            arguments: Sequence[BatchCancellationArguments],
        ) -> Sequence[Result[str, Exception]]:
            batch_started.set()
            try:
                await batch_release.wait()
            except asyncio.CancelledError:
                batch_cancelled.set()
                raise
            return [Ok(argument.id) for argument in arguments]

    execution = asyncio.create_task(
        async_execute_with_result(
            """
            Single = SlowSingletUDF(value="single")
            Batch1 = SlowBatchUDF(id="one", routing_key="shared")
            Batch2 = SlowBatchUDF(id="two", routing_key="shared")
            """,
            udf_registry=UDFRegistry.with_udfs(SlowSingletUDF, SlowBatchUDF),
        )
    )

    await asyncio.wait_for(asyncio.gather(singlet_started.wait(), batch_started.wait()), timeout=1)
    execution.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(execution, timeout=1)
        assert singlet_cancelled.is_set()
        assert batch_cancelled.is_set()
    finally:
        singlet_release.set()
        batch_release.set()
        owned_tasks = [
            task
            for task in asyncio.all_tasks()
            if (
                task is not asyncio.current_task()
                and (
                    getattr(task.get_coro(), '__name__', '') == '_execute_async_udf'
                    or getattr(task.get_coro(), '__name__', '') == '_execute_async_batch'
                )
            )
        ]
        for task in owned_tasks:
            task.cancel()
        if owned_tasks:
            await asyncio.gather(*owned_tasks, return_exceptions=True)
