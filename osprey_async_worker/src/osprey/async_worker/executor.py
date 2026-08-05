"""Async executor for Osprey rules engine.

Two execution paths:
- Sync UDFs (execute_async=False): pure computation, run inline on the event loop
- Async UDFs (AsyncUDFBase): native async I/O, awaited as tasks with semaphore

No thread pool. No run_in_executor. Existing sync UDFs that use gevent
primitives will NOT work here — they must be ported to AsyncUDFBase.
"""

import asyncio
import os
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional, Sequence, Set, Tuple

import sentry_sdk
from ddtrace import tracer
from ddtrace.span import Span as TracerSpan
from osprey.async_worker.adaptor.interfaces import AsyncBatchableUDFBase, AsyncUDFBase
from osprey.async_worker.lib.pigeon.exceptions import RPCException
from osprey.engine.ast.grammar import ASTNode
from osprey.engine.executor.custom_extracted_features import (
    ActionIdExtractedFeature,
    ErrorCountExtractedFeature,
    SampleRateExtractedFeature,
    TimestampExtractedFeature,
)
from osprey.engine.executor.dependency_chain import DependencyChain
from osprey.engine.executor.execution_context import (
    Action,
    ExecutionContext,
    ExecutionResult,
    ExpectedUdfException,
    NodeErrorInfo,
    NodeFailurePropagationException,
    NodeResult,
)
from osprey.engine.executor.execution_graph import ExecutionGraph
from osprey.engine.executor.node_executor.call_executor import CallExecutor
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.stdlib.udfs.json_utils import MissingJsonPath
from osprey.engine.udf.base import BatchableUDFBase
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.logging import get_logger
from result import Err, Ok

logger = get_logger(__name__)

# Shared placeholder for the "make mypy happy" default below; always overwritten before use.
_UNSET_RESULT: NodeResult = Err(None)

_DEFAULT_MAX_ASYNC_PER_EXECUTION = 12
_OBSERVABILITY_SAMPLE_RATE = 0.01
_CARDINALITY_SUPPRESSION_TAGS = (
    'host:none',
    'kube_node:none',
    'instance-id:none',
    'internal-hostname:none',
    'name:none',
)


def _get_ready_sync_and_async(
    allow_async: bool, context: ExecutionContext
) -> Tuple[Sequence[DependencyChain], Sequence[DependencyChain]]:
    _ready_sync = []
    _ready_async = []
    for ready_chain in context.get_ready_to_execute():
        if ready_chain.executor.execute_async and allow_async:
            _ready_async.append(ready_chain)
        else:
            _ready_sync.append(ready_chain)
    return _ready_sync, _ready_async


_SUPPRESS_IN_PROD = (ExpectedUdfException, NodeFailurePropagationException, MissingJsonPath, TypeError)


def _is_spammy_exception(e: Optional[Exception]) -> bool:
    if e is None:
        return True
    if os.environ.get('ENVIRONMENT') in ('staging', 'development'):
        return False
    return isinstance(e, _SUPPRESS_IN_PROD)


def _get_metric_tags(
    context: ExecutionContext, batchable_udf: Optional[BatchableUDFBase[Any, Any, Any]] = None
) -> List[str]:
    return [
        f'action:{context.get_action_name()}',
        f'encoding:{context.get_data_encoding()}',
        f'batch_type:{batchable_udf.get_batchable_arguments_type().__name__}'
        if batchable_udf is not None
        else 'batch_type:none',
        *_CARDINALITY_SUPPRESSION_TAGS,
    ]


def _record_udf_metric(
    metric_tags: List[str],
    execution_result: NodeResult,
    caught_exception: Optional[Exception],
) -> None:
    """Emit UDF execution metrics. Shared by sync and async paths."""
    if execution_result.is_ok():
        metrics.increment('udf_execution', tags=metric_tags + ['exc_name:none', 'result:success'])
    elif not _is_spammy_exception(caught_exception):
        exc_name = caught_exception.__class__.__name__
        if isinstance(caught_exception, RPCException):
            exc_name = exc_name + f'.{caught_exception.code().name.lower()}'
        metrics.increment(
            'udf_execution',
            tags=metric_tags + [f'exc_name:{exc_name}', 'result:unexpected_failure'],
        )
        sentry_sdk.capture_exception(caught_exception)


def _get_permit_metric_tags(context: ExecutionContext, udf: Optional[object] = None) -> List[str]:
    udf_name = udf.__class__.__name__ if udf is not None else 'none'
    return [f'action:{context.get_action_name()}', f'udf:{udf_name}', *_CARDINALITY_SUPPRESSION_TAGS]


@asynccontextmanager
async def _acquire_udf_permit(semaphore: asyncio.Semaphore, metric_tags: List[str]) -> AsyncIterator[None]:
    """Acquire one execution permit while measuring only time spent waiting for it."""
    with metrics.timed('udf_permit_wait_duration', tags=metric_tags, sample_rate=_OBSERVABILITY_SAMPLE_RATE):
        await semaphore.acquire()
    try:
        yield
    finally:
        semaphore.release()


# --- Sync execution (inline, for pure-computation UDFs) ---


def _execute_sync(
    chain: DependencyChain,
    context: ExecutionContext,
    error_info_: List[NodeErrorInfo],
) -> NodeResult:
    """Execute a sync UDF inline. For pure computation only — no I/O."""
    execution_result: NodeResult = _UNSET_RESULT
    try:
        execution_result = Ok(chain.executor.execute(execution_context=context))
    except Exception as e:
        if not isinstance(e, NodeFailurePropagationException):
            error_info_.append(NodeErrorInfo(e, chain.executor.node))
        execution_result = Err(None)
    return execution_result


# --- Legacy sync UDF fallback (thread pool, for execute_async=True UDFs not yet ported) ---


def _execute_legacy_sync(
    chain: DependencyChain,
    context: ExecutionContext,
    error_info_: List[NodeErrorInfo],
) -> NodeResult:
    """Execute a legacy sync UDF that has execute_async=True but is not AsyncUDFBase.

    Runs in a thread pool via run_in_executor. May fail if the UDF uses gevent
    primitives (no monkey patching), but errors are captured gracefully.
    """
    caught_exception: Optional[Exception] = None
    metric_tags = _get_metric_tags(context)
    if isinstance(chain.executor, CallExecutor):
        call_node: CallExecutor = chain.executor
        metric_tags += [f'udf:{call_node._udf.__class__.__name__}']

    execution_result: NodeResult = _UNSET_RESULT
    try:
        with metrics.timed('udf_execution_duration', tags=metric_tags, sample_rate=0.01):
            execution_result = Ok(chain.executor.execute(execution_context=context))
    except Exception as e:
        if not isinstance(e, NodeFailurePropagationException):
            error_info_.append(NodeErrorInfo(e, chain.executor.node))
        execution_result = Err(None)
        caught_exception = e
    finally:
        if isinstance(chain.executor, CallExecutor):
            _record_udf_metric(metric_tags, execution_result, caught_exception)
        return execution_result


async def _execute_legacy_in_executor(
    loop: asyncio.AbstractEventLoop,
    semaphore: asyncio.Semaphore,
    chain: DependencyChain,
    context: ExecutionContext,
    error_info_: List[NodeErrorInfo],
) -> NodeResult:
    """Run a legacy sync UDF in the thread pool with semaphore."""
    udf = chain.executor._udf if isinstance(chain.executor, CallExecutor) else None
    async with _acquire_udf_permit(semaphore, _get_permit_metric_tags(context, udf)):
        return await loop.run_in_executor(None, _execute_legacy_sync, chain, context, error_info_)


def _execute_legacy_batch_sync(
    udfs: Sequence[BatchableUDFBase[Any, Any, Any]],
    nodes: Sequence[ASTNode],
    batchable_args: Sequence[Any],
    context: ExecutionContext,
    error_info_: List[NodeErrorInfo],
) -> Sequence[NodeResult]:
    """Execute a batch of legacy sync batchable UDFs in thread pool."""
    assert len(udfs) == len(nodes) == len(batchable_args)
    num_executions = len(udfs)
    metric_tags = _get_metric_tags(context, udfs[0])

    try:
        with metrics.timed('udf_execution_batch_duration', tags=metric_tags, sample_rate=0.01):
            results = udfs[0].execute_batch(context, udfs, batchable_args)
        assert len(results) == num_executions
    except Exception as e:
        if not isinstance(e, NodeFailurePropagationException):
            for n in nodes:
                error_info_.append(NodeErrorInfo(e, n))
        if not _is_spammy_exception(e):
            metrics.increment(
                'udf_execution_batch',
                tags=metric_tags + [f'exc_name:{e.__class__.__name__}', 'result:unexpected_failure'],
            )
        return [Err(None)] * num_executions

    type_checked_results = []
    for udf, node, result in zip(udfs, nodes, results):
        if result.is_err():
            if not isinstance(result.value, NodeFailurePropagationException):
                error_info_.append(NodeErrorInfo(result.value, node))
            type_checked_results.append(Err(None))
            continue
        try:
            type_checked_results.append(Ok(udf.check_result_type(result.value)))
        except Exception as e:
            if not isinstance(e, NodeFailurePropagationException):
                error_info_.append(NodeErrorInfo(e, node))
            type_checked_results.append(Err(None))

    return type_checked_results


# --- Async execution (native async UDFs, awaited on event loop) ---


async def _execute_async_udf(
    semaphore: asyncio.Semaphore,
    chain: DependencyChain,
    context: ExecutionContext,
    error_info_: List[NodeErrorInfo],
    pre_resolved_arguments: Optional[Any] = None,
) -> NodeResult:
    """Execute a native async UDF. Awaited directly on the event loop.

    `pre_resolved_arguments`, when given, is the Arguments already computed for this same
    chain (e.g. by `_enqueue_batches` while checking whether a batch would form), so this
    skips a redundant `resolve_arguments` call for the same message.
    """
    call_executor: CallExecutor = chain.executor  # type: ignore
    udf: AsyncUDFBase[Any, Any] = call_executor._udf  # type: ignore
    async with _acquire_udf_permit(semaphore, _get_permit_metric_tags(context, udf)):
        metric_tags = _get_metric_tags(context) + [f'udf:{udf.__class__.__name__}']

        caught_exception: Optional[Exception] = None
        execution_result: NodeResult = _UNSET_RESULT
        try:
            resolved_arguments = (
                pre_resolved_arguments
                if pre_resolved_arguments is not None
                else udf.resolve_arguments(context, call_executor)
            )
            with metrics.timed('udf_execution_duration', tags=metric_tags, sample_rate=0.01):
                result = await udf.async_execute(context, resolved_arguments)
            execution_result = Ok(udf.check_result_type(result))
        except Exception as e:
            if not isinstance(e, NodeFailurePropagationException):
                error_info_.append(NodeErrorInfo(e, call_executor.node))
            execution_result = Err(None)
            caught_exception = e
        finally:
            _record_udf_metric(metric_tags, execution_result, caught_exception)
            return execution_result


async def _execute_async_batch(
    semaphore: asyncio.Semaphore,
    udfs: Sequence[AsyncBatchableUDFBase[Any, Any, Any]],
    nodes: Sequence[ASTNode],
    batchable_args: Sequence[Any],
    context: ExecutionContext,
    error_info_: List[NodeErrorInfo],
) -> Sequence[NodeResult]:
    """Execute a batch of native async batchable UDFs."""
    async with _acquire_udf_permit(semaphore, _get_permit_metric_tags(context, udfs[0])):
        assert len(udfs) == len(nodes) == len(batchable_args)
        num_executions = len(udfs)
        metric_tags = _get_metric_tags(context, udfs[0])

        try:
            with metrics.timed('udf_execution_batch_duration', tags=metric_tags, sample_rate=0.01):
                results = await udfs[0].async_execute_batch(context, udfs, batchable_args)
            assert len(results) == num_executions
        except Exception as e:
            if not isinstance(e, NodeFailurePropagationException):
                for n in nodes:
                    error_info_.append(NodeErrorInfo(e, n))
            if not _is_spammy_exception(e):
                metrics.increment(
                    'udf_execution_batch',
                    tags=metric_tags + [f'exc_name:{e.__class__.__name__}', 'result:unexpected_failure'],
                )
            return [Err(None)] * num_executions

        type_checked_results = []
        for udf, node, result in zip(udfs, nodes, results):
            if result.is_err():
                if not isinstance(result.value, NodeFailurePropagationException):
                    error_info_.append(NodeErrorInfo(result.value, node))
                if not _is_spammy_exception(result.value):
                    exc_name = result.value.__class__.__name__
                    if isinstance(result.value, RPCException):
                        exc_name = exc_name + f'.{result.value.code().name.lower()}'
                    metrics.increment(
                        'udf_execution',
                        tags=metric_tags
                        + [f'udf:{udf.__class__.__name__}', f'exc_name:{exc_name}', 'result:unexpected_failure'],
                    )
                type_checked_results.append(Err(None))
                continue
            try:
                type_checked_results.append(Ok(udf.check_result_type(result.value)))
                metrics.increment(
                    'udf_execution',
                    tags=metric_tags + [f'udf:{udf.__class__.__name__}', 'exc_name:none', 'result:success'],
                )
            except Exception as e:
                if not isinstance(e, NodeFailurePropagationException):
                    error_info_.append(NodeErrorInfo(e, node))
                type_checked_results.append(Err(None))

        metrics.increment('udf_execution_batch', tags=metric_tags + ['exc_name:none', 'result:success'])
        return type_checked_results


# --- Batching logic ---


async def _enqueue_batches(
    loop: asyncio.AbstractEventLoop,
    semaphore: asyncio.Semaphore,
    context: ExecutionContext,
    error_infos: List[NodeErrorInfo],
    ready_async: Sequence[DependencyChain],
) -> Tuple[
    Sequence[Tuple[DependencyChain, Optional[Any]]],
    Dict[asyncio.Task[Sequence[NodeResult]], Sequence[DependencyChain]],
]:
    """Collect batchable async chains and launch them as tasks.

    Returns ((chain, pre_resolved_arguments) pairs for chains not folded into a batch, dict
    of batch tasks -> chains). `pre_resolved_arguments` is set for a batchable chain whose
    `resolve_arguments` already succeeded here but whose batch group stayed below the
    minimum size, so the caller can reuse it instead of resolving a second time.
    """
    batch_chains: Dict[Tuple[type, str], List[Tuple[DependencyChain, Any, Any]]] = defaultdict(list)
    chains_to_remove: List[DependencyChain] = []

    for async_chain in ready_async:
        if not isinstance(async_chain.executor, CallExecutor):
            continue
        call_executor: CallExecutor = async_chain.executor
        if not isinstance(call_executor._udf, (AsyncBatchableUDFBase, BatchableUDFBase)):
            continue
        udf = call_executor._udf

        batch_type = udf.get_batchable_arguments_type()
        try:
            resolved_arguments = udf.resolve_arguments(context, call_executor)
            batchable_arguments = udf.get_batchable_arguments(resolved_arguments)
            routing_key = udf.get_batch_routing_key(batchable_arguments)
            batch_chains[(batch_type, routing_key)].append((async_chain, resolved_arguments, batchable_arguments))
        except Exception as e:
            if not isinstance(e, NodeFailurePropagationException):
                error_infos.append(NodeErrorInfo(e, call_executor.node))
            chains_to_remove.append(async_chain)
            context.set_resolved_value(async_chain, Err(None))

    new_batch_tasks: Dict[asyncio.Task[Sequence[NodeResult]], Sequence[DependencyChain]] = {}
    pre_resolved_by_chain: Dict[DependencyChain, Any] = {}

    for _, chains_and_args in batch_chains.items():
        if len(chains_and_args) < 2:
            # Batch didn't form — carry the already-resolved arguments forward so the
            # fallthrough to _execute_async_udf doesn't resolve them a second time.
            for chain, resolved_arguments, _ in chains_and_args:
                pre_resolved_by_chain[chain] = resolved_arguments
            continue

        chains, _resolved_args, args = zip(*chains_and_args)
        chains_to_remove.extend(chains)

        batch_udfs = [chain.executor._udf for chain in chains]
        batch_nodes = [chain.executor.node for chain in chains]

        if isinstance(batch_udfs[0], AsyncBatchableUDFBase):
            task = asyncio.create_task(
                _execute_async_batch(semaphore, batch_udfs, batch_nodes, args, context, error_infos)
            )
        else:
            # Legacy sync batchable UDF — run in thread pool
            async def _run_legacy_batch(s, u, n, a, c, e):
                async with _acquire_udf_permit(s, _get_permit_metric_tags(c, u[0])):
                    return await loop.run_in_executor(None, _execute_legacy_batch_sync, u, n, a, c, e)

            task = asyncio.create_task(
                _run_legacy_batch(semaphore, batch_udfs, batch_nodes, args, context, error_infos)
            )
        new_batch_tasks[task] = chains

    remaining = [(chain, pre_resolved_by_chain.get(chain)) for chain in ready_async if chain not in chains_to_remove]
    return remaining, new_batch_tasks


# --- Main executor ---


async def execute(
    execution_graph: ExecutionGraph,
    udf_helpers: UDFHelpers,
    action: Action,
    max_concurrent: int = _DEFAULT_MAX_ASYNC_PER_EXECUTION,
    sample_rate: int = 100,
    parent_tracer_span: Optional[TracerSpan] = None,
) -> ExecutionResult:
    """Async executor for the osprey rules engine.

    Three paths:
    - Sync UDFs (execute_async=False): run inline, pure computation only
    - AsyncUDFBase: awaited as tasks directly on event loop (native async)
    - Legacy UDFBase with execute_async=True: run in thread pool via run_in_executor
      (may fail on gevent calls, errors captured gracefully)
    """
    if parent_tracer_span:
        parent_tracer_span.set_tag('action-name', action.action_name)

    context = ExecutionContext(execution_graph=execution_graph, helpers=udf_helpers, action=action)
    allow_async = max_concurrent > 0
    semaphore = asyncio.Semaphore(max_concurrent)
    loop = asyncio.get_running_loop()
    error_infos: List[NodeErrorInfo] = []

    in_progress_singlets: Dict[asyncio.Task[NodeResult], DependencyChain] = {}
    in_progress_batches: Dict[asyncio.Task[Sequence[NodeResult]], Sequence[DependencyChain]] = {}

    ready_sync, ready_async = _get_ready_sync_and_async(allow_async, context)
    scheduler_tags = [f'action:{context.get_action_name()}', *_CARDINALITY_SUPPRESSION_TAGS]

    while ready_sync or ready_async or in_progress_singlets or in_progress_batches:
        metrics.histogram(
            'udf_ready_depth',
            len(ready_async),
            tags=scheduler_tags,
            sample_rate=_OBSERVABILITY_SAMPLE_RATE,
        )
        metrics.histogram(
            'udf_in_flight',
            len(in_progress_singlets) + sum(len(chains) for chains in in_progress_batches.values()),
            tags=scheduler_tags,
            sample_rate=_OBSERVABILITY_SAMPLE_RATE,
        )

        # Check for already-finished tasks (non-blocking)
        finished_singlets = [t for t in in_progress_singlets if t.done()]
        finished_batches = [t for t in in_progress_batches if t.done()]

        if not ready_sync and not ready_async and not finished_singlets and not finished_batches:
            # Block until at least one async task finishes
            all_pending: Set[asyncio.Task[Any]] = set(in_progress_singlets.keys()) | set(in_progress_batches.keys())
            if all_pending:
                with tracer.start_span('osprey.rules.async_wait_nodes', child_of=parent_tracer_span):
                    done, _ = await asyncio.wait(all_pending, return_when=asyncio.FIRST_COMPLETED)
                finished_singlets = [t for t in done if t in in_progress_singlets]
                finished_batches = [t for t in done if t in in_progress_batches]

        # Process finished singlets
        for task in finished_singlets:
            chain = in_progress_singlets.pop(task)
            context.set_resolved_value(chain, task.result())

        # Process finished batches
        # Distinct loop variable from the singlet `task` above: the two dicts hold
        # tasks with different result types (Task[NodeResult] vs Task[Sequence[NodeResult]]),
        # and reusing one name would unify them to the singlet type.
        for batch_task in finished_batches:
            chains = in_progress_batches.pop(batch_task)
            results = batch_task.result()
            for i, chain in enumerate(chains):
                context.set_resolved_value(chain, results[i])

        # Enqueue async tasks
        if allow_async and ready_async:
            with tracer.start_span('osprey.rules.try_enqueue_batches', child_of=parent_tracer_span):
                remaining_ready_async, new_batch_tasks = await _enqueue_batches(
                    loop, semaphore, context, error_infos, ready_async
                )
            in_progress_batches.update(new_batch_tasks)

            for async_chain, pre_resolved_arguments in remaining_ready_async:
                # Native async UDF → await on event loop
                if isinstance(async_chain.executor, CallExecutor) and isinstance(
                    async_chain.executor._udf, (AsyncUDFBase, AsyncBatchableUDFBase)
                ):
                    task = asyncio.create_task(
                        _execute_async_udf(semaphore, async_chain, context, error_infos, pre_resolved_arguments)
                    )
                else:
                    # Legacy sync UDF with execute_async=True → thread pool fallback
                    task = asyncio.create_task(
                        _execute_legacy_in_executor(loop, semaphore, async_chain, context, error_infos)
                    )
                in_progress_singlets[task] = async_chain

        # Execute sync chains inline (pure computation, fast, no I/O).
        # Only yield deep into a long sync round when async tasks are in flight.
        # Each sleep(0) triggers a full event loop cycle including gRPC C-core polling,
        # so unnecessary yields cause significant context-switch overhead.
        # Short rounds (<100 chains) skip yielding entirely — the asyncio.wait() at the
        # top of the loop provides natural yield points between rounds.
        for i, sync_chain in enumerate(ready_sync):
            if (in_progress_singlets or in_progress_batches) and i > 0 and i % 100 == 0:
                await asyncio.sleep(0)
            result = _execute_sync(sync_chain, context, error_infos)
            context.set_resolved_value(sync_chain, result)

        ready_sync, ready_async = _get_ready_sync_and_async(allow_async, context)

    # --- Build result ---

    unexpected_error_infos = [
        error_info for error_info in error_infos if not isinstance(error_info.error, ExpectedUdfException)
    ]
    validator_results = execution_graph.validated_sources.validation_results

    context.add_custom_extracted_features(
        [
            ActionIdExtractedFeature(action_id=action.action_id),
            TimestampExtractedFeature(timestamp=action.timestamp),
            ErrorCountExtractedFeature(error_count=len(unexpected_error_infos)),
            SampleRateExtractedFeature(sample_rate=sample_rate),
        ]
    )

    effects = context.get_effects()

    actionable_error_infos = [
        error_info
        for error_info in error_infos
        if isinstance(error_info.error, Exception) and not _is_spammy_exception(error_info.error)
    ]
    has_effects = len(effects) > 0
    has_actionable_errors = len(actionable_error_infos) > 0
    action_tags = [
        f'action:{action.action_name}',
        f'had_actionable_errors:{has_actionable_errors}',
        f'had_effects:{has_effects}',
    ]
    metrics.increment('osprey.action_health', tags=action_tags)
    if has_actionable_errors:
        metrics.histogram(
            'osprey.action_error_count', len(actionable_error_infos), tags=[f'action:{action.action_name}']
        )

    trace_id = str(parent_tracer_span.trace_id) if parent_tracer_span else None

    return ExecutionResult(
        extracted_features=context.get_extracted_features(),
        action=action,
        effects=effects,
        validator_results=validator_results,
        error_infos=unexpected_error_infos,
        sample_rate=sample_rate,
        trace_id=trace_id,
        rule_audit_entries=context.get_rule_audit_entries(),
    )
