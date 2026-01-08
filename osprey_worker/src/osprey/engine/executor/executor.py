from collections import defaultdict
from typing import Any, Optional, Sequence, Tuple

import gevent
import gevent.pool
from ddtrace import tracer
from ddtrace.span import Span as TracerSpan
from osprey.engine.ast.grammar import ASTNode
from osprey.engine.executor.custom_extracted_features import (
    ActionIdExtractedFeature,
    ErrorCountExtractedFeature,
    SampleRateExtractedFeature,
    TimestampExtractedFeature,
)
from osprey.engine.stdlib.udfs.json_utils import MissingJsonPath
from osprey.engine.udf.base import BatchableUDFBase
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.pigeon.exceptions import RPCException
from result import Err, Ok

from .dependency_chain import DependencyChain
from .execution_context import (
    Action,
    ExecutionContext,
    ExecutionResult,
    ExpectedUdfException,
    NodeErrorInfo,
    NodeFailurePropagationException,
    NodeResult,
)
from .execution_graph import ExecutionGraph
from .node_executor.call_executor import CallExecutor
from .udf_execution_helpers import UDFHelpers

logger = get_logger(__name__)

InProgressSingletsType = dict['gevent.Greenlet[NodeResult]', DependencyChain]
"""
A dictionary mapping in-progress async greenlets to the chain that they are executing.
"""
InProgressBatchesType = dict['gevent.Greenlet[Sequence[NodeResult]]', Sequence[DependencyChain]]
"""
A dictionary mapping in-progress batch async greenlets to the sequence of chains that they are executing.
"""
FinishedSingletsType = Sequence['gevent.Greenlet[NodeResult]']
"""
A sequence of finished async singlets.
"""
FinishedBatchesType = Sequence['gevent.Greenlet[Sequence[NodeResult]]']
"""
A sequence of finished async batches.
"""


def _get_ready_sync_and_async(
    allow_async: bool, context: ExecutionContext
) -> Tuple[Sequence[DependencyChain], Sequence[DependencyChain]]:
    _ready_sync = []
    _ready_async = []
    # Partition the ready chains into sync and async
    for ready_chain in context.get_ready_to_execute():
        if ready_chain.executor.execute_async and allow_async:
            _ready_async.append(ready_chain)
        else:
            _ready_sync.append(ready_chain)
    return _ready_sync, _ready_async


def _is_spammy_exception(e: Optional[Exception]) -> bool:
    """
    Add any spammy exceptions here to avoid sending them in metrics
    """
    return (
        e is None
        or isinstance(e, ExpectedUdfException)
        or isinstance(e, NodeFailurePropagationException)
        or isinstance(e, MissingJsonPath)
        or isinstance(e, TypeError)
    )


def _get_metric_tags(
    context: ExecutionContext, batchable_udf: Optional[BatchableUDFBase[Any, Any, Any]] = None
) -> list[str]:
    return [
        f'action:{context.get_action_name()}',
        f'encoding:{context.get_data_encoding()}',
        f'batch_type:{batchable_udf.get_batchable_arguments_type().__name__}'
        if batchable_udf is not None
        else 'batch_type:none',
        # These are autodiscovery tags with high cardinality that we want to override so we do not ingest them
        'host:none',
        'kube_node:none',
        'instance-id:none',
        'internal-hostname:none',
        'name:none',
    ]


def _wrapped_batch_execution(
    udfs: Sequence[BatchableUDFBase[Any, Any, Any]],
    nodes: Sequence[ASTNode],  # these are passed in for error tracking ^^
    batchable_args: Sequence[Any],
    context: ExecutionContext,
    error_info_: list[NodeErrorInfo],
) -> Sequence[NodeResult]:
    """
    Executes a batch of batchable UDFs, and returns an ordered list of the results of the execution.

    This function expects that the UDFs have already been sorted by their batchable arguments type and routing key.
    """
    assert len(udfs) == len(nodes) == len(batchable_args), (
        '_wrapped_batch_execution invariant: udfs, nodes, and batchable_args must be the same length'
    )
    num_executions = len(udfs)

    metric_tags = _get_metric_tags(context, udfs[0])

    try:
        with metrics.timed('udf_execution_batch_duration', tags=metric_tags, sample_rate=0.01):
            results = udfs[0].execute_batch(context, udfs, batchable_args)
        # if the result length doesn't properly return the number of executions,
        # we cant guarantee which result corresponds to which execution
        assert len(results) == num_executions, (
            '_wrapped_batch_execution invariant: results must be the same length as the number of executions'
        )
    except Exception as e:
        # no need to re-add this to errors, it's not the root cause
        if not isinstance(e, NodeFailurePropagationException):
            for n in nodes:
                error_info_.append(NodeErrorInfo(e, n))
        # if we couldn't even execute the batch, then everything failed ! :c
        if not _is_spammy_exception(e):
            metrics.increment(
                'udf_execution_batch',
                tags=metric_tags + [f'exc_name:{e.__class__.__name__}', 'result:unexpected_failure'],
            )
            for udf in udfs:
                metrics.increment(
                    'udf_execution',
                    tags=metric_tags
                    + [
                        f'udf:{udf.__class__.__name__}',
                        f'exc_name:{e.__class__.__name__}',
                        'result:unexpected_failure',
                    ],
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
            # No need to re-add this to errors, it's not the root cause
            if not isinstance(e, NodeFailurePropagationException):
                error_info_.append(NodeErrorInfo(e, node))
            if not _is_spammy_exception(e):
                exc_name = e.__class__.__name__
                if isinstance(e, RPCException):
                    exc_name = exc_name + f'.{e.code().name.lower()}'
                metrics.increment(
                    'udf_execution',
                    tags=metric_tags
                    + [f'udf:{udf.__class__.__name__}', f'exc_name:{exc_name}', 'result:unexpected_failure'],
                )
            type_checked_results.append(Err(None))

    metrics.increment('udf_execution_batch', tags=metric_tags + ['exc_name:none', 'result:success'])

    return type_checked_results


def _wrapped_execution(
    chain: DependencyChain,
    context: ExecutionContext,
    error_info_: list[NodeErrorInfo],
) -> NodeResult:
    caught_exception: Optional[Exception] = None

    metric_tags = _get_metric_tags(context)
    if isinstance(chain.executor, CallExecutor):
        # This half step is necessary as mypy has a difficult time linting build in class variables
        call_node: CallExecutor = chain.executor
        metric_tags += [f'udf:{call_node._udf.__class__.__name__}']

    # Make mypy happy
    execution_result: NodeResult = Err(None)
    try:
        # only track time if using an async function
        if chain.executor.execute_async:
            with metrics.timed('udf_execution_duration', tags=metric_tags, sample_rate=0.01):
                execution_result = Ok(chain.executor.execute(execution_context=context))
        else:
            execution_result = Ok(chain.executor.execute(execution_context=context))
    except Exception as e:
        # No need to re-add this to errors, it's not the root cause
        if not isinstance(e, NodeFailurePropagationException):
            error_info_.append(NodeErrorInfo(e, chain.executor.node))
        execution_result = Err(None)
        caught_exception = e

    finally:
        # If this is a call node which executed a UDF, push the results of the execution to the datadog metrics.
        if isinstance(chain.executor, CallExecutor):
            if execution_result.is_ok() and chain.executor and chain.executor.execute_async:
                metrics.increment('udf_execution', tags=metric_tags + ['exc_name:none', 'result:success'])

            # Ignore some well-known "unexpected" exceptions that are spammy.
            elif not _is_spammy_exception(caught_exception):
                exc_name = caught_exception.__class__.__name__
                if isinstance(caught_exception, RPCException):
                    exc_name = exc_name + f'.{caught_exception.code().name.lower()}'
                metrics.increment(
                    'udf_execution',
                    tags=metric_tags + [f'exc_name:{exc_name}', 'result:unexpected_failure'],
                )

        return execution_result


def _enqueue_batches(
    context: ExecutionContext,
    error_infos: list[NodeErrorInfo],
    async_pool: gevent.pool.Pool,
    in_progress_async_batches: InProgressBatchesType,
    ready_async: Sequence[DependencyChain],
) -> Sequence[DependencyChain]:
    """
    Collects all the ready async chains that can be batched together and batches
    them according to their batchable arguments type and routing key.

    Returns the remaining ready async chains that could not be batched together.
    """
    # tuple( batch_type, routing_key ) -> list of tuple( chain, args )
    batch_chains: dict[Tuple[type, str], list[Tuple[DependencyChain, Any]]] = defaultdict(list)
    chains_to_remove: list[DependencyChain] = []
    for async_chain in ready_async:
        if not isinstance(async_chain.executor, CallExecutor):
            continue
        call_executor: CallExecutor = async_chain.executor
        if not isinstance(call_executor._udf, BatchableUDFBase):
            continue
        udf: BatchableUDFBase[Any, Any, Any] = call_executor._udf

        # we group each batch based on the batchable argument type that they support & the routing key.
        # This allows for us to batch together different UDFs that might have different args
        # but are still able to be batched together, i.e. ReadCount & Count share a `counter_batch` endpoint
        batch_type = udf.get_batchable_arguments_type()

        try:
            resolved_arguments = udf.resolve_arguments(context, call_executor)
            batchable_arguments = udf.get_batchable_arguments(resolved_arguments)

            routing_key = udf.get_batch_routing_key(batchable_arguments)
            batch_chains[(batch_type, routing_key)].append((async_chain, batchable_arguments))
        except Exception as e:
            # No need to re-add this to errors, it's not the root cause
            if not isinstance(e, NodeFailurePropagationException):
                error_infos.append(NodeErrorInfo(e, call_executor.node))
            chains_to_remove.append(async_chain)
            context.set_resolved_value(async_chain, Err(None))

    # we can batch the chains together now that they are sorted by type & routing key ₍^ >ヮ<^₎ .ᐟ.ᐟ
    for _, chains_and_args in batch_chains.items():
        if len(chains_and_args) < 2:
            # if there is only one element, we will simply run it as a single async task later
            continue

        chains, args = zip(*chains_and_args)

        chains_to_remove.extend(chains)

        in_progress_batch_greenlet = async_pool.apply_async(
            _wrapped_batch_execution,
            args=(
                [chain.executor._udf for chain in chains],
                [chain.executor.node for chain in chains],
                args,
                context,
                error_infos,
            ),
        )
        in_progress_async_batches[in_progress_batch_greenlet] = chains

    return [chain for chain in ready_async if chain not in chains_to_remove]


def execute(
    execution_graph: ExecutionGraph,
    udf_helpers: UDFHelpers,
    action: Action,
    async_pool: Optional[gevent.pool.Pool],
    sample_rate: int = 100,
    parent_tracer_span: Optional[TracerSpan] = None,
) -> ExecutionResult:
    """A 'parallel' executor using gevent greenlets.

    :param execution_graph: The graph of rules
    :param udf_helpers: Holds additional helpers that UDFs need to operate
    :param action: The action to execute against the rules
    :param async_pool: A pool to run asynchronous tasks in. Can be used to limit how many tasks can be run at any time.
        If None then everything will be run synchronously.
    :param sample_rate: From 0 to 100, what percentage of actions should actually be executed.
    :return: The result of the execution.
    """
    if parent_tracer_span:
        parent_tracer_span.set_tag('action-name', action.action_name)

    context = ExecutionContext(execution_graph=execution_graph, helpers=udf_helpers, action=action)
    allow_async = async_pool is not None
    assert async_pool is None or async_pool.size is None or async_pool.size > 0
    error_infos: list[NodeErrorInfo] = []

    in_progress_async_singlets: InProgressSingletsType = {}
    in_progress_async_batches: InProgressBatchesType = {}

    ready_sync, ready_async = _get_ready_sync_and_async(allow_async, context)

    while ready_sync or ready_async or in_progress_async_singlets or in_progress_async_batches:
        # Get however many async tasks happen to be done already, without blocking.
        finished_async_singlets: FinishedSingletsType = [
            greenlet for greenlet in in_progress_async_singlets.keys() if greenlet.ready()
        ]
        finished_async_batches: FinishedBatchesType = [
            greenlet for greenlet in in_progress_async_batches.keys() if greenlet.ready()
        ]

        if not ready_sync and not ready_async:
            # Since we have nothing else to do (and are therefore fully blocked on existing async) block/wait for
            # at least one to finish.
            if in_progress_async_singlets:
                with tracer.start_span('osprey.rules.gevent_wait_async_nodes', child_of=parent_tracer_span):
                    finished_async_singlets = gevent.wait(in_progress_async_singlets.keys(), count=1)
            elif in_progress_async_batches:
                with tracer.start_span('osprey.rules.gevent_wait_async_batch_nodes', child_of=parent_tracer_span):
                    finished_async_batches = gevent.wait(in_progress_async_batches.keys(), count=1)

        # Handle finished async tasks
        for finished_async_singlet in finished_async_singlets:
            async_chain = in_progress_async_singlets.pop(finished_async_singlet)
            result = finished_async_singlet.get()
            context.set_resolved_value(async_chain, result)
        # Handle finished async batch tasks
        for finished_async_batch in finished_async_batches:
            async_chains = in_progress_async_batches.pop(finished_async_batch)
            results = finished_async_batch.get()
            for i, chain in enumerate(async_chains):
                context.set_resolved_value(chain, results[i])

        # enqueue all ready async tasks
        if async_pool is not None:
            # enqueue any ready async tasks that can be batched
            with tracer.start_span('osprey.rules.try_enqueue_batches', child_of=parent_tracer_span):
                remaining_ready_async = _enqueue_batches(
                    context, error_infos, async_pool, in_progress_async_batches, ready_async
                )

            # enqueue the remaining non-batched async tasks
            for async_chain in remaining_ready_async:
                in_progress_greenlet = async_pool.apply_async(
                    _wrapped_execution, args=(async_chain, context, error_infos)
                )
                in_progress_async_singlets[in_progress_greenlet] = async_chain

        for i, sync_chain in enumerate(ready_sync):
            # If we have a lot of things running, give async a chance every so often.
            if (in_progress_async_singlets or in_progress_async_batches) and i % 100 == 0:
                gevent.sleep(0)
            result = _wrapped_execution(sync_chain, context, error_infos)
            context.set_resolved_value(sync_chain, result)

        ready_sync, ready_async = _get_ready_sync_and_async(allow_async, context)

    unexpected_error_infos = [
        error_info for error_info in error_infos if not isinstance(error_info.error, ExpectedUdfException)
    ]
    validator_results = execution_graph.validated_sources.validation_results

    # some default custom extracted features :O
    context.add_custom_extracted_features(
        [
            ActionIdExtractedFeature(action_id=action.action_id),
            TimestampExtractedFeature(timestamp=action.timestamp),
            ErrorCountExtractedFeature(error_count=len(unexpected_error_infos)),
            SampleRateExtractedFeature(sample_rate=sample_rate),
        ]
    )

    result = ExecutionResult(
        extracted_features=context.get_extracted_features(),
        action=action,
        effects=context.get_effects(),
        validator_results=validator_results,
        error_infos=unexpected_error_infos,
        sample_rate=sample_rate,
    )
    return result
