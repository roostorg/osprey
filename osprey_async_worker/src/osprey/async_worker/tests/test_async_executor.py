"""Tests for the async executor.

Validates that the async executor produces the same results as the gevent
executor for stdlib UDFs (pure computation, no I/O).
"""

import asyncio
from typing import ClassVar, Sequence

import pytest
from osprey.async_worker.adaptor.interfaces import AsyncBatchableUDFBase, AsyncUDFBase
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import BatchableUDFBase, UDFBase
from osprey.engine.udf.registry import UDFRegistry
from result import Ok, Result


@pytest.fixture(autouse=True)
def reset_udf_timeouts():
    """reset both async udf base timeouts to 2.0 before and after each test."""
    AsyncUDFBase.timeout = 2.0
    AsyncBatchableUDFBase.timeout = 2.0
    yield
    AsyncUDFBase.timeout = 2.0
    AsyncBatchableUDFBase.timeout = 2.0


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


# Test argument type for timeout testing
class TimeoutTestArguments(ArgumentsBase):
    """Simple test argument type."""

    value: str


# Timeout behavior


@pytest.mark.asyncio
async def test_native_udf_base_exposes_timeout():
    """Native async UDF bases expose a two-second timeout."""
    assert hasattr(AsyncUDFBase, 'timeout')
    assert AsyncUDFBase.timeout == 2.0
    assert hasattr(AsyncBatchableUDFBase, 'timeout')
    assert AsyncBatchableUDFBase.timeout == 2.0

    # Legacy bases should NOT have timeout
    assert not hasattr(UDFBase, 'timeout')
    assert not hasattr(BatchableUDFBase, 'timeout')


@pytest.mark.asyncio
async def test_subclass_timeout_override_used(async_execute_with_result):
    """The executor enforces a positive subclass timeout override."""

    class CustomTimeoutUDF(AsyncUDFBase[TimeoutTestArguments, str]):
        timeout: ClassVar[float] = 0.1  # Short override

        @classmethod
        def _get_udf_base_args(cls):
            return (TimeoutTestArguments, str)

        async def async_execute(self, execution_context: ExecutionContext, arguments: TimeoutTestArguments) -> str:
            # This will timeout because 0.1s is very short
            await asyncio.sleep(0.5)
            return arguments.value

    registry = UDFRegistry.with_udfs(CustomTimeoutUDF)
    result = await async_execute_with_result(
        'Result = CustomTimeoutUDF(value="test")',
        data={},
        udf_registry=registry,
    )

    # Should have failed with timeout
    assert len(result.error_infos) > 0
    assert isinstance(result.error_infos[0].error, TimeoutError)


@pytest.mark.asyncio
async def test_udf_completes_within_deadline(async_execute_with_result):
    """A UDF completing before its deadline retains its result without errors."""

    class ControlledAsyncUDF(AsyncUDFBase[TimeoutTestArguments, str]):
        """Async UDF controlled by an event for testing."""

        _release_event: asyncio.Event | None = None

        @classmethod
        def _get_udf_base_args(cls):
            return (TimeoutTestArguments, str)

        async def async_execute(self, execution_context: ExecutionContext, arguments: TimeoutTestArguments) -> str:
            """Wait for release event before completing."""
            if self._release_event:
                await self._release_event.wait()
            return arguments.value

    release_event = asyncio.Event()
    ControlledAsyncUDF._release_event = release_event

    registry = UDFRegistry.with_udfs(ControlledAsyncUDF)

    # Start execution and immediately release
    task = asyncio.create_task(
        async_execute_with_result(
            'Result = ControlledAsyncUDF(value="success")',
            data={},
            udf_registry=registry,
        )
    )

    # Give it a moment to start
    await asyncio.sleep(0.01)

    # Release the UDF
    release_event.set()

    result = await task

    # Should have completed successfully
    assert result.extracted_features['Result'] == 'success'
    assert len(result.error_infos) == 0


@pytest.mark.asyncio
async def test_udf_timeout_recorded_as_error(async_execute_with_result):
    """A UDF exceeding its deadline records a TimeoutError."""
    release_event = asyncio.Event()

    class ShortTimeoutUDF(AsyncUDFBase[TimeoutTestArguments, str]):
        timeout: ClassVar[float] = 0.1  # 100ms timeout
        _release_event: asyncio.Event | None = None

        @classmethod
        def _get_udf_base_args(cls):
            return (TimeoutTestArguments, str)

        async def async_execute(self, execution_context: ExecutionContext, arguments: TimeoutTestArguments) -> str:
            # Wait for release event; timeout will trigger before it fires
            if ShortTimeoutUDF._release_event:
                await ShortTimeoutUDF._release_event.wait()
            return arguments.value

    ShortTimeoutUDF._release_event = release_event

    registry = UDFRegistry.with_udfs(ShortTimeoutUDF)

    result = await async_execute_with_result(
        'Result = ShortTimeoutUDF(value="timeout")',
        data={},
        udf_registry=registry,
    )

    # Should have a timeout error
    assert len(result.error_infos) > 0
    assert isinstance(result.error_infos[0].error, TimeoutError)


@pytest.mark.asyncio
async def test_semaphore_wait_not_counted_against_deadline(async_execute_with_result):
    """Semaphore wait time does not consume the UDF execution deadline.

    Coordination:
    1. First (blocking) acquires semaphore and signals block_event
    2. Main test waits for block_event to confirm First owns semaphore
    3. Main test holds First in semaphore for 0.6s (longer than Second's 0.5s timeout)
    4. Main test releases First; Second can now acquire semaphore
    5. Second completes within its 0.5s timeout (because semaphore wait doesn't count)
    """

    class QuickAsyncUDF(AsyncUDFBase[TimeoutTestArguments, str]):
        """Async UDF with a short timeout for testing semaphore isolation."""

        timeout: ClassVar[float] = 0.5  # Short timeout to test semaphore isolation

        @classmethod
        def _get_udf_base_args(cls):
            return (TimeoutTestArguments, str)

        async def async_execute(self, execution_context: ExecutionContext, arguments: TimeoutTestArguments) -> str:
            """Complete immediately."""
            # Return immediately to avoid blocking semaphore
            await asyncio.sleep(0)
            return arguments.value

    class BlockingAsyncUDF(AsyncUDFBase[TimeoutTestArguments, str]):
        """Async UDF that blocks the semaphore for testing."""

        _block_event: asyncio.Event | None = None
        _release_event: asyncio.Event | None = None

        @classmethod
        def _get_udf_base_args(cls):
            return (TimeoutTestArguments, str)

        async def async_execute(self, execution_context: ExecutionContext, arguments: TimeoutTestArguments) -> str:
            """Signal that we own the semaphore, then wait for release."""
            if BlockingAsyncUDF._block_event:
                BlockingAsyncUDF._block_event.set()
            if BlockingAsyncUDF._release_event:
                await BlockingAsyncUDF._release_event.wait()
            return arguments.value

    block_event = asyncio.Event()
    release_event = asyncio.Event()

    # Set test coordination events
    BlockingAsyncUDF._block_event = block_event
    BlockingAsyncUDF._release_event = release_event

    registry = UDFRegistry.with_udfs(BlockingAsyncUDF, QuickAsyncUDF)

    async def run_execution():
        return await async_execute_with_result(
            """
            First = BlockingAsyncUDF(value="first")
            Second = QuickAsyncUDF(value="second")
            """,
            data={},
            udf_registry=registry,
            max_concurrent=1,  # Only one concurrent execution
        )

    # Start execution
    result_task = asyncio.create_task(run_execution())

    # Wait for First (blocking) to own the semaphore and signal block_event
    try:
        await asyncio.wait_for(block_event.wait(), timeout=1.0)
    except asyncio.TimeoutError:
        raise AssertionError('BlockingAsyncUDF did not acquire semaphore')

    # Hold First in semaphore for 0.6s (longer than Second's 0.5s execution timeout)
    # Second is waiting for semaphore; the wait must not count against its deadline
    await asyncio.sleep(0.6)

    # Release First; Second acquires semaphore and must complete in its 0.5s timeout
    release_event.set()

    # Get the result
    result = await result_task

    # Both should succeed - QuickAsyncUDF should not have timed out
    # even though it waited 0.6s for the semaphore
    assert result.extracted_features['First'] == 'first'
    assert result.extracted_features['Second'] == 'second'
    assert len(result.error_infos) == 0


# Test argument type for batch testing
class BatchTestArgs(ArgumentsBase):
    """Test argument type for batch UDFs."""

    id: str
    routing_key: str


@pytest.mark.asyncio
async def test_batch_mixed_timeout_uses_highest(async_execute_with_result):
    """A mixed-timeout batch uses the highest timeout without changing grouping.

    Tests that a batch with mixed timeouts uses the MAX timeout:
    - Both UDFs batch together (grouping unchanged)
    - Batch sleeps 0.3s which would timeout at 0.2s but succeeds at 1.0s
    - Verifies executor uses max(0.2, 1.0) = 1.0s timeout for the batch
    """
    # Isolated capture state for this test execution
    captured_timeouts: list[float] = []

    class MixedTimeoutBatchUDF(AsyncBatchableUDFBase[BatchTestArgs, str, BatchTestArgs]):
        timeout: ClassVar[float] = 2.0

        @classmethod
        def _get_udf_base_args(cls):
            return (BatchTestArgs, str, BatchTestArgs)

        def get_batchable_arguments(self, arguments: BatchTestArgs) -> BatchTestArgs:
            return arguments

        async def async_execute(self, execution_context: ExecutionContext, arguments: BatchTestArgs) -> str:
            return f'result-{arguments.id}'

        async def async_execute_batch(
            self,
            execution_context: ExecutionContext,
            udfs: Sequence,
            arguments: Sequence[BatchTestArgs],
        ) -> Sequence[Result[str, Exception]]:
            # Record the actual timeouts in local capture state
            nonlocal captured_timeouts
            captured_timeouts = [u.timeout for u in udfs]
            # Sleep 0.3s: would fail with 0.2s timeout but pass with 1.0s
            await asyncio.sleep(0.3)
            return [Ok(f'result-{arg.id}') for arg in arguments]

    class ShortTimeoutBatchUDF(MixedTimeoutBatchUDF):
        timeout: ClassVar[float] = 0.2

    class LongTimeoutBatchUDF(MixedTimeoutBatchUDF):
        timeout: ClassVar[float] = 1.0

    registry = UDFRegistry.with_udfs(ShortTimeoutBatchUDF, LongTimeoutBatchUDF)

    result = await async_execute_with_result(
        """
        Short = ShortTimeoutBatchUDF(id="short", routing_key="shared")
        Long = LongTimeoutBatchUDF(id="long", routing_key="shared")
        """,
        data={},
        udf_registry=registry,
    )

    # Verify both UDFs were in the same batch
    timeouts = sorted(captured_timeouts)
    assert timeouts == [0.2, 1.0]

    # Both should have succeeded (batch used max timeout of 1.0s, not min of 0.2s)
    assert result.extracted_features['Short'] == 'result-short'
    assert result.extracted_features['Long'] == 'result-long'
    assert len(result.error_infos) == 0


@pytest.mark.asyncio
async def test_batch_within_deadline_retains_results(async_execute_with_result):
    """A batch within its shared deadline retains every result without errors."""

    class FastBatchUDF(AsyncBatchableUDFBase[BatchTestArgs, str, BatchTestArgs]):
        timeout: ClassVar[float] = 0.5

        @classmethod
        def _get_udf_base_args(cls):
            return (BatchTestArgs, str, BatchTestArgs)

        def get_batchable_arguments(self, arguments: BatchTestArgs) -> BatchTestArgs:
            return arguments

        async def async_execute(self, execution_context: ExecutionContext, arguments: BatchTestArgs) -> str:
            return f'fast-{arguments.id}'

        async def async_execute_batch(
            self,
            execution_context: ExecutionContext,
            udfs: Sequence,
            arguments: Sequence[BatchTestArgs],
        ) -> Sequence[Result[str, Exception]]:
            # Quick execution within timeout
            await asyncio.sleep(0.1)
            return [Ok(f'fast-{arg.id}') for arg in arguments]

    registry = UDFRegistry.with_udfs(FastBatchUDF)

    result = await async_execute_with_result(
        """
        Result1 = FastBatchUDF(id="first", routing_key="shared")
        Result2 = FastBatchUDF(id="second", routing_key="shared")
        """,
        data={},
        udf_registry=registry,
    )

    # Both should succeed
    assert result.extracted_features['Result1'] == 'fast-first'
    assert result.extracted_features['Result2'] == 'fast-second'
    assert len(result.error_infos) == 0


@pytest.mark.asyncio
async def test_batch_over_deadline_fails_all_nodes(async_execute_with_result):
    """A batch exceeding its shared deadline records timeout and fails every node.

    Direct assertion: async_execute_batch() receives asyncio.CancelledError
    when the batch timeout fires, confirming the coroutine is cancelled.
    """
    # Isolated flag to capture cancellation signal
    batch_received_cancellation = False

    class SlowBatchUDF(AsyncBatchableUDFBase[BatchTestArgs, str, BatchTestArgs]):
        timeout: ClassVar[float] = 0.1  # 100ms timeout

        @classmethod
        def _get_udf_base_args(cls):
            return (BatchTestArgs, str, BatchTestArgs)

        def get_batchable_arguments(self, arguments: BatchTestArgs) -> BatchTestArgs:
            return arguments

        async def async_execute(self, execution_context: ExecutionContext, arguments: BatchTestArgs) -> str:
            return f'slow-{arguments.id}'

        async def async_execute_batch(
            self,
            execution_context: ExecutionContext,
            udfs: Sequence,
            arguments: Sequence[BatchTestArgs],
        ) -> Sequence[Result[str, Exception]]:
            # Exceed the 0.1s timeout
            nonlocal batch_received_cancellation
            try:
                await asyncio.sleep(0.5)
                return [Ok(f'slow-{arg.id}') for arg in arguments]
            except asyncio.CancelledError:
                # Direct assertion: batch coroutine receives CancelledError
                batch_received_cancellation = True
                raise

    registry = UDFRegistry.with_udfs(SlowBatchUDF)

    result = await async_execute_with_result(
        """
        Result1 = SlowBatchUDF(id="first", routing_key="shared")
        Result2 = SlowBatchUDF(id="second", routing_key="shared")
        """,
        data={},
        udf_registry=registry,
    )

    # The batch coroutine receives cancellation before node errors are recorded
    assert batch_received_cancellation, 'async_execute_batch() did not receive CancelledError'

    # Both results should have failed with TimeoutError in the batch
    assert len(result.error_infos) >= 2
    timeout_errors = [ei for ei in result.error_infos if isinstance(ei.error, TimeoutError)]
    assert len(timeout_errors) >= 2, f'Expected 2+ TimeoutErrors, got {len(timeout_errors)} from {result.error_infos}'
