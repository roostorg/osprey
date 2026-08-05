"""Tests for the async executor.

Validates that the async executor produces the same results as the gevent
executor for stdlib UDFs (pure computation, no I/O).
"""

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from textwrap import dedent
from typing import ClassVar, Sequence

import pytest
from osprey.async_worker.adaptor.interfaces import AsyncBatchableUDFBase, AsyncUDFBase
from osprey.async_worker.executor import execute
from osprey.engine.ast.grammar import Source
from osprey.engine.ast.sources import Sources
from osprey.engine.ast_validator import validate_sources
from osprey.engine.ast_validator.validator_registry import ValidatorRegistry
from osprey.engine.executor.dependency_chain import DependencyChain
from osprey.engine.executor.execution_context import Action, ExecutionContext
from osprey.engine.executor.execution_graph import ExecutionGraph, compile_execution_graph
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.stdlib import get_config_registry
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.registry import UDFRegistry
from result import Ok


class GatedArguments(ArgumentsBase):
    value: str


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


class CountingBatchableArguments(ArgumentsBase):
    key: str
    value: str


@dataclass
class CountingBatchableArgs:
    key: str
    value: str


class CountingBatchableUdf(AsyncBatchableUDFBase[CountingBatchableArguments, str, CountingBatchableArgs]):
    """Batchable async UDF that counts `resolve_arguments` calls and can raise on demand.

    Only used to exercise the batch-fallthrough path in `_enqueue_batches`/`_execute_async_udf`.
    `AsyncBatchableUDFBase` has no existing subclass in the codebase to model from, and the
    generic MRO walk in `UDFBase._get_udf_base_args` (unlike `AsyncUDFBase`, which overrides it)
    resolves to unsubstituted TypeVars for a class parameterized two levels up — so the type
    getters below are hardcoded rather than left to rely on that walk.
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


@pytest.mark.asyncio
async def test_execute_skips_duplicate_import_and_require_source_activation(
    async_execute_fn, monkeypatch: pytest.MonkeyPatch
) -> None:
    looked_up_sources: list[str] = []
    get_sorted_dependency_chain = ExecutionGraph.get_sorted_dependency_chain

    def record_lookup(self: ExecutionGraph, source: Source) -> Sequence[DependencyChain]:
        looked_up_sources.append(source.path)
        return get_sorted_dependency_chain(self, source)

    monkeypatch.setattr(ExecutionGraph, 'get_sorted_dependency_chain', record_lookup)

    result = await async_execute_fn(
        {
            'main.sml': "Import(rules=['branch.sml', 'shared.sml'])",
            'branch.sml': "Require(rule='shared.sml')",
            'shared.sml': 'Shared = 1 + 0',
        }
    )

    assert result == {'Shared': 1}
    assert looked_up_sources == ['main.sml', 'branch.sml', 'shared.sml']


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
                Action(action_id=i, action_name=name, data={'action_name': name}, timestamp=timestamp),
            )
        )
        for i, name in enumerate(('a', 'b'), start=1)
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
async def test_execution_plan_matches_legacy_dynamic_source_results(
    async_execute_with_result, monkeypatch: pytest.MonkeyPatch
) -> None:
    sources = {
        'main.sml': """
            ActionName: str = JsonData(path="$.action_name", coerce_type=True)
            Require(rule=f"actions/{ActionName}.sml")
        """,
        'actions/a.sml': """
            Import(rules=["shared.sml"])
            A = 40 + SharedBase
        """,
        'actions/b.sml': 'B = 99',
        'shared.sml': 'SharedBase = 2',
    }
    action_time = datetime(2026, 8, 4, tzinfo=timezone.utc)

    with monkeypatch.context() as legacy:
        legacy.setattr(ExecutionGraph, 'get_execution_plan', lambda _graph: None)
        legacy_result = await async_execute_with_result(sources, data={'action_name': 'a'}, action_time=action_time)

    planned_result = await async_execute_with_result(sources, data={'action_name': 'a'}, action_time=action_time)

    assert planned_result.extracted_features == legacy_result.extracted_features
    assert planned_result.effects == legacy_result.effects
    assert planned_result.error_infos == legacy_result.error_infos
    assert planned_result.rule_audit_entries == legacy_result.rule_audit_entries


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
