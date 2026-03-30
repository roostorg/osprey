"""Async executor for Osprey rules engine.

Direct port of osprey.engine.executor.executor using asyncio instead of gevent.
Shares the same execution graph, dependency resolution, and batching logic.
"""

import asyncio
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from ddtrace import tracer
from ddtrace.span import Span as TracerSpan
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
from osprey.worker.lib.pigeon.exceptions import RPCException
from result import Err, Ok

logger = get_logger(__name__)

_DEFAULT_MAX_ASYNC_PER_EXECUTION = 12


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


def _is_spammy_exception(e: Optional[Exception]) -> bool:
    return (
        e is None
        or isinstance(e, ExpectedUdfException)
        or isinstance(e, NodeFailurePropagationException)
        or isinstance(e, MissingJsonPath)
        or isinstance(e, TypeError)
    )


def _get_metric_tags(
    context: ExecutionContext, batchable_udf: Optional[BatchableUDFBase[Any, Any, Any]] = None
) -> List[str]:
    return [
        f'action:{context.get_action_name()}',
        f'encoding:{context.get_data_encoding()}',
        f'batch_type:{batchable_udf.get_batchable_arguments_type().__name__}'
        if batchable_udf is not None
        else 'batch_type:none',
        'host:none',
        'kube_node:none',
        'instance-id:none',
        'internal-hostname:none',
        'name:none',
    ]


def _wrapped_batch_execution(
    udfs: Sequence[BatchableUDFBase[Any, Any, Any]],
    nodes: Sequence[ASTNode],
    batchable_args: Sequence[Any],
    context: ExecutionContext,
    error_info_: List[NodeErrorInfo],
) -> Sequence[NodeResult]:
    """Execute a batch of batchable UDFs synchronously (called via run_in_executor)."""
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
            for udf in udfs:
                metrics.increment(
                    'udf_execution',
                    tags=metric_tags
                    + [f'udf:{udf.__class__.__name__}', f'exc_name:{e.__class__.__name__}', 'result:unexpected_failure'],
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
                    tags=metric_tags + [f'udf:{udf.__class__.__name__}', f'exc_name:{exc_name}', 'result:unexpected_failure'],
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
            if not _is_spammy_exception(e):
                exc_name = e.__class__.__name__
                if isinstance(e, RPCException):
                    exc_name = exc_name + f'.{e.code().name.lower()}'
                metrics.increment(
                    'udf_execution',
                    tags=metric_tags + [f'udf:{udf.__class__.__name__}', f'exc_name:{exc_name}', 'result:unexpected_failure'],
                )
            type_checked_results.append(Err(None))

    metrics.increment('udf_execution_batch', tags=metric_tags + ['exc_name:none', 'result:success'])
    return type_checked_results


def _wrapped_execution(
    chain: DependencyChain,
    context: ExecutionContext,
    error_info_: List[NodeErrorInfo],
) -> NodeResult:
    """Execute a single UDF synchronously (called via run_in_executor for async UDFs)."""
    caught_exception: Optional[Exception] = None
    metric_tags = _get_metric_tags(context)
    if isinstance(chain.executor, CallExecutor):
        call_node: CallExecutor = chain.executor
        metric_tags += [f'udf:{call_node._udf.__class__.__name__}']

    execution_result: NodeResult = Err(None)
    try:
        if chain.executor.execute_async:
            with metrics.timed('udf_execution_duration', tags=metric_tags, sample_rate=0.01):
                execution_result = Ok(chain.executor.execute(execution_context=context))
        else:
            execution_result = Ok(chain.executor.execute(execution_context=context))
    except Exception as e:
        if not isinstance(e, NodeFailurePropagationException):
            error_info_.append(NodeErrorInfo(e, chain.executor.node))
        execution_result = Err(None)
        caught_exception = e
    finally:
        if isinstance(chain.executor, CallExecutor):
            if execution_result.is_ok() and chain.executor and chain.executor.execute_async:
                metrics.increment('udf_execution', tags=metric_tags + ['exc_name:none', 'result:success'])
            elif not _is_spammy_exception(caught_exception):
                exc_name = caught_exception.__class__.__name__
                if isinstance(caught_exception, RPCException):
                    exc_name = exc_name + f'.{caught_exception.code().name.lower()}'
                metrics.increment(
                    'udf_execution',
                    tags=metric_tags + [f'exc_name:{exc_name}', 'result:unexpected_failure'],
                )
        return execution_result


async def _run_in_executor(
    loop: asyncio.AbstractEventLoop,
    semaphore: asyncio.Semaphore,
    func: Any,
    *args: Any,
) -> Any:
    """Run a sync function in the thread pool, respecting the concurrency semaphore."""
    async with semaphore:
        return await loop.run_in_executor(None, func, *args)


async def _enqueue_batches(
    loop: asyncio.AbstractEventLoop,
    semaphore: asyncio.Semaphore,
    context: ExecutionContext,
    error_infos: List[NodeErrorInfo],
    ready_async: Sequence[DependencyChain],
) -> Tuple[Sequence[DependencyChain], Dict[asyncio.Task[Sequence[NodeResult]], Sequence[DependencyChain]]]:
    """Collect batchable async chains and launch them as tasks.

    Returns (remaining non-batched chains, dict of batch tasks -> chains).
    """
    batch_chains: Dict[Tuple[type, str], List[Tuple[DependencyChain, Any]]] = defaultdict(list)
    chains_to_remove: List[DependencyChain] = []

    for async_chain in ready_async:
        if not isinstance(async_chain.executor, CallExecutor):
            continue
        call_executor: CallExecutor = async_chain.executor
        if not isinstance(call_executor._udf, BatchableUDFBase):
            continue
        udf: BatchableUDFBase[Any, Any, Any] = call_executor._udf

        batch_type = udf.get_batchable_arguments_type()
        try:
            resolved_arguments = udf.resolve_arguments(context, call_executor)
            batchable_arguments = udf.get_batchable_arguments(resolved_arguments)
            routing_key = udf.get_batch_routing_key(batchable_arguments)
            batch_chains[(batch_type, routing_key)].append((async_chain, batchable_arguments))
        except Exception as e:
            if not isinstance(e, NodeFailurePropagationException):
                error_infos.append(NodeErrorInfo(e, call_executor.node))
            chains_to_remove.append(async_chain)
            context.set_resolved_value(async_chain, Err(None))

    new_batch_tasks: Dict[asyncio.Task[Sequence[NodeResult]], Sequence[DependencyChain]] = {}

    for _, chains_and_args in batch_chains.items():
        if len(chains_and_args) < 2:
            continue

        chains, args = zip(*chains_and_args)
        chains_to_remove.extend(chains)

        task = asyncio.create_task(
            _run_in_executor(
                loop,
                semaphore,
                _wrapped_batch_execution,
                [chain.executor._udf for chain in chains],
                [chain.executor.node for chain in chains],
                args,
                context,
                error_infos,
            )
        )
        new_batch_tasks[task] = chains

    remaining = [chain for chain in ready_async if chain not in chains_to_remove]
    return remaining, new_batch_tasks


async def execute(
    execution_graph: ExecutionGraph,
    udf_helpers: UDFHelpers,
    action: Action,
    max_concurrent: int = _DEFAULT_MAX_ASYNC_PER_EXECUTION,
    sample_rate: int = 100,
    parent_tracer_span: Optional[TracerSpan] = None,
) -> ExecutionResult:
    """Async executor using asyncio tasks instead of gevent greenlets.

    :param execution_graph: The graph of rules
    :param udf_helpers: Holds additional helpers that UDFs need to operate
    :param action: The action to execute against the rules
    :param max_concurrent: Maximum number of concurrent async UDF executions.
        Replaces the gevent pool size.
    :param sample_rate: From 0 to 100, what percentage of actions should actually be executed.
    :return: The result of the execution.
    """
    if parent_tracer_span:
        parent_tracer_span.set_tag('action-name', action.action_name)

    context = ExecutionContext(execution_graph=execution_graph, helpers=udf_helpers, action=action)
    allow_async = max_concurrent > 0
    semaphore = asyncio.Semaphore(max_concurrent) if allow_async else asyncio.Semaphore(1)
    loop = asyncio.get_running_loop()
    error_infos: List[NodeErrorInfo] = []

    # Maps: task -> chain (singlets) or task -> [chains] (batches)
    in_progress_singlets: Dict[asyncio.Task[NodeResult], DependencyChain] = {}
    in_progress_batches: Dict[asyncio.Task[Sequence[NodeResult]], Sequence[DependencyChain]] = {}

    ready_sync, ready_async = _get_ready_sync_and_async(allow_async, context)

    while ready_sync or ready_async or in_progress_singlets or in_progress_batches:
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
            result = task.result()
            context.set_resolved_value(chain, result)

        # Process finished batches
        for task in finished_batches:
            chains = in_progress_batches.pop(task)
            results = task.result()
            for i, chain in enumerate(chains):
                context.set_resolved_value(chain, results[i])

        # Enqueue async tasks
        if allow_async:
            with tracer.start_span('osprey.rules.try_enqueue_batches', child_of=parent_tracer_span):
                remaining_ready_async, new_batch_tasks = await _enqueue_batches(
                    loop, semaphore, context, error_infos, ready_async
                )
            in_progress_batches.update(new_batch_tasks)

            for async_chain in remaining_ready_async:
                task = asyncio.create_task(
                    _run_in_executor(loop, semaphore, _wrapped_execution, async_chain, context, error_infos)
                )
                in_progress_singlets[task] = async_chain

        # Execute sync chains on the current task (with periodic yields)
        for i, sync_chain in enumerate(ready_sync):
            if (in_progress_singlets or in_progress_batches) and i % 100 == 0:
                await asyncio.sleep(0)
            result = _wrapped_execution(sync_chain, context, error_infos)
            context.set_resolved_value(sync_chain, result)

        ready_sync, ready_async = _get_ready_sync_and_async(allow_async, context)

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
