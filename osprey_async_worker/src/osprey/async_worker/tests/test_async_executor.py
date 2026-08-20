"""Tests for the async executor.

Validates that the async executor produces the same results as the gevent
executor for stdlib UDFs (pure computation, no I/O).
"""

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from textwrap import dedent
from typing import Any, ClassVar, Sequence

import pytest
from osprey.async_worker import executor as async_executor
from osprey.async_worker.adaptor.interfaces import AsyncBatchableUDFBase, AsyncUDFBase
from osprey.async_worker.executor import execute
from osprey.engine.ast.grammar import Source
from osprey.engine.ast.sources import Sources
from osprey.engine.ast_validator import validate_sources
from osprey.engine.ast_validator.validator_registry import ValidatorRegistry
from osprey.engine.executor.execution_context import Action, ExecutionContext
from osprey.engine.executor.execution_graph import ExecutionGraph, compile_execution_graph
from osprey.engine.executor.execution_plan import ExecutionPlanState
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.stdlib import get_config_registry
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.engine.udf.registry import UDFRegistry
from result import Ok, Result


class CountingBatchableArguments(ArgumentsBase):
    key: str
    value: str


@dataclass
class CountingBatchableArgs:
    key: str
    value: str


class CountingBatchableUdf(AsyncBatchableUDFBase[CountingBatchableArguments, str, CountingBatchableArgs]):
    """A batchable async UDF that records argument resolution calls.

    The explicit type getters avoid unsubstituted TypeVars for this test class.
    """

    resolve_call_count: ClassVar[int] = 0
    raise_on_resolve: ClassVar[bool] = False

    @classmethod
    def get_arguments_type(cls):
        return CountingBatchableArguments

    @classmethod
    def get_rvalue_type(cls):
        return str

    @classmethod
    def get_batchable_arguments_type(cls):
        return CountingBatchableArgs

    def resolve_arguments(self, execution_context, call_executor) -> CountingBatchableArguments:
        type(self).resolve_call_count += 1
        if type(self).raise_on_resolve:
            raise RuntimeError('resolve boom')
        return super().resolve_arguments(execution_context, call_executor)

    def get_batchable_arguments(self, arguments: CountingBatchableArguments) -> CountingBatchableArgs:
        return CountingBatchableArgs(key=arguments.key, value=arguments.value)

    def get_batch_routing_key(self, arguments: CountingBatchableArgs) -> str:
        return arguments.key

    async def async_execute(self, execution_context: ExecutionContext, arguments: CountingBatchableArguments) -> str:
        return arguments.value

    async def async_execute_batch(self, execution_context, udfs, arguments: Sequence[CountingBatchableArgs]):
        return [Ok(a.value) for a in arguments]


class GatedArguments(ArgumentsBase):
    value: str


class CancellationArguments(ArgumentsBase):
    value: str


class BatchCancellationArguments(ArgumentsBase):
    id: str
    routing_key: str


class GatedAsyncUdf(AsyncUDFBase[GatedArguments, str]):
    entered = 0
    both_entered: asyncio.Event
    release: asyncio.Event

    async def async_execute(self, execution_context: ExecutionContext, arguments: GatedArguments) -> str:
        type(self).entered += 1
        if type(self).entered == 2:
            type(self).both_entered.set()
        await type(self).release.wait()
        return arguments.value


@pytest.mark.asyncio
async def test_planned_execute_activates_each_imported_or_required_source_once(
    async_execute_fn, monkeypatch: pytest.MonkeyPatch
) -> None:
    activated_sources: list[str] = []
    activate_source = ExecutionPlanState.activate_source

    def record_activation(self: ExecutionPlanState, source: Source) -> None:
        activated_sources.append(source.path)
        activate_source(self, source)

    monkeypatch.setattr(ExecutionPlanState, 'activate_source', record_activation)

    result = await async_execute_fn(
        {
            'main.sml': "Import(rules=['branch.sml', 'shared.sml'])",
            'branch.sml': "Require(rule='shared.sml')",
            'shared.sml': 'Shared = 1 + 0',
        }
    )

    assert result == {'Shared': 1}
    assert activated_sources == ['main.sml', 'branch.sml', 'shared.sml']


@pytest.mark.asyncio
async def test_concurrent_actions_isolate_dynamic_source_activation(
    stdlib_udf_registry: UDFRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = stdlib_udf_registry
    registry.register(GatedAsyncUdf)
    sources = Sources.from_dict(
        {
            'main.sml': dedent(
                """
                ActionName: str = JsonData(path="$.action_name", coerce_type=True)
                Require(rule=f"actions/{ActionName}.sml")
                """
            ),
            'actions/a.sml': 'A = GatedAsyncUdf(value="a")',
            'actions/b.sml': 'B = GatedAsyncUdf(value="b")',
        }
    )
    validator_registry = ValidatorRegistry.get_instance().instance_with_additional_validators(
        get_config_registry().get_validator()
    )
    graph = compile_execution_graph(validate_sources(sources, registry, validator_registry))
    sources_by_action: dict[str, list[str]] = defaultdict(list)
    original_enqueue_source = ExecutionContext.enqueue_source

    def record_enqueue_source(context: ExecutionContext, source: Source) -> None:
        sources_by_action[context.get_action_name()].append(source.path)
        original_enqueue_source(context, source)

    monkeypatch.setattr(ExecutionContext, 'enqueue_source', record_enqueue_source)
    GatedAsyncUdf.entered = 0
    GatedAsyncUdf.both_entered = asyncio.Event()
    GatedAsyncUdf.release = asyncio.Event()
    timestamp = datetime(2026, 8, 4, tzinfo=timezone.utc)
    tasks = [
        asyncio.create_task(
            execute(
                graph,
                UDFHelpers(),
                Action(action_id=index, action_name=name, data={'action_name': name}, timestamp=timestamp),
            )
        )
        for index, name in enumerate(('a', 'b'), start=1)
    ]

    try:
        await asyncio.wait_for(GatedAsyncUdf.both_entered.wait(), timeout=5)
        assert not any(task.done() for task in tasks)
        GatedAsyncUdf.release.set()
        result_a, result_b = await asyncio.gather(*tasks)
    finally:
        GatedAsyncUdf.release.set()
        await asyncio.gather(*tasks, return_exceptions=True)

    assert result_a.extracted_features['A'] == 'a'
    assert 'B' not in result_a.extracted_features
    assert result_b.extracted_features['B'] == 'b'
    assert 'A' not in result_b.extracted_features
    assert not result_a.error_infos
    assert not result_b.error_infos
    assert sources_by_action == {
        'a': ['main.sml', 'actions/a.sml'],
        'b': ['main.sml', 'actions/b.sml'],
    }


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


@pytest.fixture()
def counting_batchable_udf():
    """Registers CountingBatchableUdf and resets its call-count/raise state around the test."""
    CountingBatchableUdf.resolve_call_count = 0
    CountingBatchableUdf.raise_on_resolve = False
    yield CountingBatchableUdf
    CountingBatchableUdf.resolve_call_count = 0
    CountingBatchableUdf.raise_on_resolve = False


@pytest.mark.asyncio
async def test_batch_of_one_reuses_resolved_arguments(
    async_execute_with_result, stdlib_udf_registry: UDFRegistry, counting_batchable_udf
):
    """A batchable UDF alone (batch group size 1) falls through to the singleton async path.

    Before the fix, this chain's arguments were resolved once to compute the routing key in
    `_enqueue_batches`, then resolved again in `_execute_async_udf` -- a duplicate Arguments
    construction. resolve_arguments should now only be called once.
    """
    stdlib_udf_registry.register(counting_batchable_udf)
    result = await async_execute_with_result(
        'A = CountingBatchableUdf(key="solo", value="a")', udf_registry=stdlib_udf_registry
    )
    assert result.extracted_features['A'] == 'a'
    assert not result.error_infos
    assert counting_batchable_udf.resolve_call_count == 1


@pytest.mark.asyncio
async def test_batch_of_two_resolves_once_per_chain(
    async_execute_with_result, stdlib_udf_registry: UDFRegistry, counting_batchable_udf
):
    """When a batch group forms (>=2 chains sharing a routing key), each chain's arguments are
    resolved exactly once -- the batch execution path never re-resolves, so this is unchanged
    by the fallthrough fix."""
    stdlib_udf_registry.register(counting_batchable_udf)
    result = await async_execute_with_result(
        """
        A = CountingBatchableUdf(key="shared", value="a")
        B = CountingBatchableUdf(key="shared", value="b")
        """,
        udf_registry=stdlib_udf_registry,
    )
    assert result.extracted_features['A'] == 'a'
    assert result.extracted_features['B'] == 'b'
    assert not result.error_infos
    assert counting_batchable_udf.resolve_call_count == 2


@pytest.mark.asyncio
async def test_plan_matches_legacy_native_async_batch(
    async_execute_with_result,
    stdlib_udf_registry: UDFRegistry,
    counting_batchable_udf,
    monkeypatch: pytest.MonkeyPatch,
):
    stdlib_udf_registry.register(counting_batchable_udf)
    sources = """
        A = CountingBatchableUdf(key="shared", value="a")
        B = CountingBatchableUdf(key="shared", value="b")
    """
    action_time = datetime(2026, 8, 4, tzinfo=timezone.utc)

    with monkeypatch.context() as legacy:
        legacy.setattr(ExecutionGraph, 'get_execution_plan', lambda _graph: None)
        legacy_result = await async_execute_with_result(
            sources,
            udf_registry=stdlib_udf_registry,
            action_time=action_time,
        )
    legacy_resolve_count = counting_batchable_udf.resolve_call_count
    counting_batchable_udf.resolve_call_count = 0

    planned_result = await async_execute_with_result(
        sources,
        udf_registry=stdlib_udf_registry,
        action_time=action_time,
    )

    assert planned_result.extracted_features == legacy_result.extracted_features
    assert planned_result.error_infos == legacy_result.error_infos == []
    assert counting_batchable_udf.resolve_call_count == legacy_resolve_count == 2


@pytest.mark.asyncio
async def test_batch_of_one_resolve_failure_surfaces_once(
    async_execute_with_result, stdlib_udf_registry: UDFRegistry, counting_batchable_udf
):
    """If resolve_arguments raises while computing the routing key (batch group size 1), the
    failure is fully handled inside _enqueue_batches: the chain is resolved to Err(None) there
    and never falls through to _execute_async_udf. resolve_arguments is therefore still only
    attempted once, and the same exception surfaces as before the fallthrough fix."""
    counting_batchable_udf.raise_on_resolve = True
    stdlib_udf_registry.register(counting_batchable_udf)
    result = await async_execute_with_result(
        'A = CountingBatchableUdf(key="solo", value="a")', udf_registry=stdlib_udf_registry
    )
    assert result.extracted_features.get('A') is None
    assert len(result.error_infos) == 1
    assert isinstance(result.error_infos[0].error, RuntimeError)
    assert str(result.error_infos[0].error) == 'resolve boom'
    assert counting_batchable_udf.resolve_call_count == 1


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
            raise AssertionError('unreachable')

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
            udfs: Sequence[UDFBase[Any, Any]],
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


@pytest.mark.asyncio
async def test_repeated_cancellation_waits_for_owned_task_cleanup(async_execute_with_result):
    started = asyncio.Event()
    work_release = asyncio.Event()
    cleanup_started = asyncio.Event()
    cleanup_release = asyncio.Event()

    class SlowUDF(AsyncUDFBase[CancellationArguments, str]):
        @classmethod
        def _get_udf_base_args(cls):
            return (CancellationArguments, str)

        async def async_execute(self, execution_context: ExecutionContext, arguments: CancellationArguments) -> str:
            started.set()
            try:
                await work_release.wait()
            except asyncio.CancelledError:
                cleanup_started.set()
                await cleanup_release.wait()
                raise
            raise AssertionError('unreachable')

    execution = asyncio.create_task(
        async_execute_with_result(
            'Result = SlowUDF(value="test")',
            udf_registry=UDFRegistry.with_udfs(SlowUDF),
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1)

    try:
        execution.cancel()
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        for _ in range(3):
            execution.cancel()
        for _ in range(10):
            await asyncio.sleep(0)

        assert not execution.done()
        cleanup_release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(execution, timeout=1)
        assert execution.cancelling() == 1
    finally:
        work_release.set()
        cleanup_release.set()
        await asyncio.wait_for(asyncio.gather(execution, return_exceptions=True), timeout=1)


@pytest.mark.asyncio
@pytest.mark.parametrize(('stale_cancelling_count', 'expected_cancelling_count'), [(False, 1), (True, 2)])
async def test_cancellation_during_child_cancellation_cleanup_preserves_request(
    async_execute_with_result, stale_cancelling_count: bool, expected_cancelling_count: int
):
    cancelled_started = asyncio.Event()
    cancelled_release = asyncio.Event()
    slow_started = asyncio.Event()
    work_release = asyncio.Event()
    cleanup_started = asyncio.Event()
    cleanup_release = asyncio.Event()

    class CancelledUDF(AsyncUDFBase[CancellationArguments, str]):
        @classmethod
        def _get_udf_base_args(cls):
            return (CancellationArguments, str)

        async def async_execute(self, execution_context: ExecutionContext, arguments: CancellationArguments) -> str:
            cancelled_started.set()
            await cancelled_release.wait()
            raise asyncio.CancelledError

    class SlowUDF(AsyncUDFBase[CancellationArguments, str]):
        @classmethod
        def _get_udf_base_args(cls):
            return (CancellationArguments, str)

        async def async_execute(self, execution_context: ExecutionContext, arguments: CancellationArguments) -> str:
            slow_started.set()
            try:
                await work_release.wait()
            except asyncio.CancelledError:
                cleanup_started.set()
                await cleanup_release.wait()
                raise
            raise AssertionError('unreachable')

    async def run_execution():
        if stale_cancelling_count:
            current_task = asyncio.current_task()
            assert current_task is not None
            current_task.cancel()
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                # Preserve the stale cancellation count for this test
                pass
        return await async_execute_with_result(
            'Cancelled = CancelledUDF(value="cancelled")\nSlow = SlowUDF(value="slow")',
            udf_registry=UDFRegistry.with_udfs(CancelledUDF, SlowUDF),
        )

    execution = asyncio.create_task(run_execution())
    await asyncio.wait_for(asyncio.gather(cancelled_started.wait(), slow_started.wait()), timeout=1)

    try:
        cancelled_release.set()
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        execution.cancel()
        cleanup_release.set()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(execution, timeout=1)
        assert execution.cancelling() == expected_cancelling_count
    finally:
        cancelled_release.set()
        work_release.set()
        cleanup_release.set()
        await asyncio.wait_for(asyncio.gather(execution, return_exceptions=True), timeout=1)


@pytest.mark.asyncio
async def test_owned_task_cleanup_timeout_does_not_block_cancellation(
    async_execute_with_result, monkeypatch: pytest.MonkeyPatch
):
    started = asyncio.Event()
    work_release = asyncio.Event()
    cleanup_started = asyncio.Event()
    cleanup_release = asyncio.Event()
    cleanup_finished = asyncio.Event()
    monkeypatch.setattr(async_executor, '_OWNED_TASK_CLEANUP_SECONDS', 0.01)

    class SlowUDF(AsyncUDFBase[CancellationArguments, str]):
        @classmethod
        def _get_udf_base_args(cls):
            return (CancellationArguments, str)

        async def async_execute(self, execution_context: ExecutionContext, arguments: CancellationArguments) -> str:
            started.set()
            try:
                await work_release.wait()
            except asyncio.CancelledError:
                cleanup_started.set()
                try:
                    await cleanup_release.wait()
                finally:
                    cleanup_finished.set()
                raise
            raise AssertionError('unreachable')

    execution = asyncio.create_task(
        async_execute_with_result(
            'Result = SlowUDF(value="test")',
            udf_registry=UDFRegistry.with_udfs(SlowUDF),
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1)

    execution.cancel()
    try:
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        done, _ = await asyncio.wait({execution}, timeout=1)

        assert execution in done
        with pytest.raises(asyncio.CancelledError):
            execution.result()
        await asyncio.wait_for(cleanup_finished.wait(), timeout=1)
    finally:
        work_release.set()
        cleanup_release.set()
        await asyncio.wait_for(cleanup_finished.wait(), timeout=1)
        await asyncio.wait_for(asyncio.gather(execution, return_exceptions=True), timeout=1)


@pytest.mark.asyncio
async def test_cancellation_during_error_cleanup_supersedes_original_error(async_execute_with_result):
    class FatalError(BaseException):
        pass

    fatal_started = asyncio.Event()
    fatal_release = asyncio.Event()
    slow_started = asyncio.Event()
    work_release = asyncio.Event()
    cleanup_started = asyncio.Event()
    cleanup_release = asyncio.Event()

    class FatalUDF(AsyncUDFBase[CancellationArguments, str]):
        @classmethod
        def _get_udf_base_args(cls):
            return (CancellationArguments, str)

        async def async_execute(self, execution_context: ExecutionContext, arguments: CancellationArguments) -> str:
            fatal_started.set()
            await fatal_release.wait()
            raise FatalError

    class SlowUDF(AsyncUDFBase[CancellationArguments, str]):
        @classmethod
        def _get_udf_base_args(cls):
            return (CancellationArguments, str)

        async def async_execute(self, execution_context: ExecutionContext, arguments: CancellationArguments) -> str:
            slow_started.set()
            try:
                await work_release.wait()
            except asyncio.CancelledError:
                cleanup_started.set()
                await cleanup_release.wait()
                raise
            raise AssertionError('unreachable')

    execution = asyncio.create_task(
        async_execute_with_result(
            'Fatal = FatalUDF(value="fatal")\nSlow = SlowUDF(value="slow")',
            udf_registry=UDFRegistry.with_udfs(FatalUDF, SlowUDF),
        )
    )
    await asyncio.wait_for(asyncio.gather(fatal_started.wait(), slow_started.wait()), timeout=1)

    try:
        fatal_release.set()
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        execution.cancel()
        cleanup_release.set()

        with pytest.raises(asyncio.CancelledError) as exc_info:
            await asyncio.wait_for(execution, timeout=1)
        assert execution.cancelling() == 1
        assert isinstance(exc_info.value.__cause__, FatalError)
    finally:
        fatal_release.set()
        work_release.set()
        cleanup_release.set()
        await asyncio.wait_for(asyncio.gather(execution, return_exceptions=True), timeout=1)
