"""Async rules sink — the main processing loop for the async worker."""

import asyncio
import logging
from typing import Optional

import sentry_sdk
from ddtrace import tracer
from osprey.engine.executor.execution_context import Action, ExecutionResult
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_engine import OspreyEngine
from osprey.worker.lib.osprey_shared.logging import info_log_osprey_action
from osprey.worker.lib.snowflake import generate_snowflake
from osprey.worker.sinks.sink.rules_sink import ActionSampler
from osprey.worker.sinks.utils.acking_contexts import BaseAckingContext, VerdictsAckingContext

from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink
from osprey.async_worker.executor import execute as async_execute
from osprey.async_worker.sinks.sink.input_stream import AsyncBaseInputStream

logger = logging.getLogger(__name__)


class AsyncRulesRunner:
    """Async version of RulesRunner — classifies one action and pushes to output sink."""

    def __init__(
        self,
        engine: OspreyEngine,
        output_sink: AsyncBaseOutputSink,
        udf_helpers: UDFHelpers,
        max_concurrent_udfs: int = 12,
    ) -> None:
        self._engine = engine
        self._sampler = ActionSampler(engine)
        self._output_sink = output_sink
        self._udf_helpers = udf_helpers
        self._max_concurrent_udfs = max_concurrent_udfs

    async def classify_one(self, action: Action, tag: str) -> Optional[ExecutionResult]:
        sample_config = self._sampler.sample(action)
        tags = [
            tag,
            f'action:{action.action_name}',
            f'sample_rate:{sample_config.sample_rate}',
        ]

        if sample_config.drop:
            metrics.increment('dropped_message', tags=tags)
            return None

        result: Optional[ExecutionResult] = None
        try:
            with metrics.timed('handled_message', tags=tags, use_ms=True):
                result = await async_execute(
                    self._engine.execution_graph,
                    self._udf_helpers,
                    action,
                    max_concurrent=self._max_concurrent_udfs,
                    sample_rate=sample_config.sample_rate,
                )
            with metrics.timed('handled_output', tags=tags, use_ms=True):
                await self._output_sink.push(result)
                info_log_osprey_action(action.action_id, action.action_name, 'pushed to output sink')
                return result
        except BaseException:
            sentry_sdk.capture_exception()
            return result


class AsyncRulesSink:
    """Async rules sink — iterates an async input stream, executes rules, pushes to output sinks."""

    def __init__(
        self,
        engine: OspreyEngine,
        input_stream: AsyncBaseInputStream[BaseAckingContext[Action]],
        output_sink: AsyncBaseOutputSink,
        udf_helpers: UDFHelpers,
        max_concurrent_udfs: int = 12,
    ):
        self._input_stream = input_stream
        self._rules_runner = AsyncRulesRunner(engine, output_sink, udf_helpers, max_concurrent_udfs)

    async def run(self) -> None:
        async for message_context in self._input_stream:
            try:
                with message_context as action:
                    if action.data.get('osprey_skip_async_classification', False) or action.data.get(
                        'osprey_skip_async', False
                    ):
                        continue

                    with tracer.start_span('osprey.async.classify_one', child_of=None) as span:
                        tracer.context_provider.activate(span.context)

                        if not action.action_id and action.action_id != 0:
                            action.action_id = generate_snowflake(retries=3).to_int()

                        info_log_osprey_action(action.action_id, action.action_name, 'beginning async classify_one')
                        result = await self._rules_runner.classify_one(action, tag='sink:async-rules-sink')

                        if isinstance(message_context, VerdictsAckingContext):
                            if result is None:
                                metrics.increment('rules_sink.missing_result')
                            else:
                                message_context.set_verdicts(result.get_verdicts_pb2_proto())
                                metrics.increment('rules_sink.captured_verdicts')

                        info_log_osprey_action(action.action_id, action.action_name, 'async classify_one complete')
            except asyncio.CancelledError:
                return
            except Exception as e:
                logging.exception('Unexpected error in async rules sink')
                metrics.increment('rules_sink.unexpected_error', tags=[f'err:{e.__class__.__name__}'])
                sentry_sdk.capture_exception(e)

    async def stop(self) -> None:
        await self._input_stream.stop()
