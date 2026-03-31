import logging
import os
from dataclasses import dataclass
from random import randint
from typing import Optional

import gevent
import sentry_sdk
from ddtrace import tracer
from ddtrace.span import Span as TracerSpan
from osprey.engine.executor.execution_context import Action, ExecutionResult
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.utils.types import add_slots
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_engine import OspreyEngine
from osprey.worker.lib.osprey_shared.logging import info_log_osprey_action
from osprey.worker.lib.snowflake import generate_snowflake
from osprey.worker.lib.sources_config.subkeys.action_config import ActionConfigs
from osprey.worker.sinks.utils.acking_contexts import (
    BaseAckingContext,
    VerdictsAckingContext,
)

from ..utils.envoy_status import get_envoy_check_server
from .base_sink import BaseSink
from .input_stream import BaseInputStream
from .output_sink import BaseOutputSink


@add_slots
@dataclass
class SampleDecision:
    """Holds whether or not a given action was sampled, and what the sample rate was."""

    sample_rate: int
    """A number between 0 and 100, representing the sample rate of the action. 0 being always sampled,
    and 100 being never sampled."""

    drop: bool
    """Whether or not the event should be dropped,."""


_SAMPLE_NEVER = SampleDecision(sample_rate=100, drop=False)
_SAMPLE_ALWAYS = SampleDecision(sample_rate=0, drop=True)


class ActionSampler:
    """Checks whether or not an action should be sampled."""

    def __init__(self, engine: OspreyEngine):
        self._engine = engine

    def sample(self, action: Action) -> SampleDecision:
        """
        Returns a SampleDecision describing if the action should be sampled, and what the sample rate was.
        """
        action_configs = self._engine.get_config_subkey(ActionConfigs)
        action_config = action_configs.get_action_config(action.action_name)

        # A sample rate of 100 = do not sample.
        if not action_config or action_config.sample_rate == 100:
            return _SAMPLE_NEVER

        # Shortcut if we're always sampling this event.
        if action_config.sample_rate == 0:
            return _SAMPLE_ALWAYS

        p = randint(0, 99)
        should_drop = p < action_config.sample_rate
        return SampleDecision(sample_rate=action_config.sample_rate, drop=should_drop)


class RulesRunner:
    """Given a osprey engine and an output sink, can classify a single action and push the result to the output sink."""

    def __init__(
        self,
        engine: OspreyEngine,
        output_sink: BaseOutputSink,
        udf_helpers: UDFHelpers,
    ) -> None:
        self._engine = engine
        self._sampler = ActionSampler(engine)
        self._output_sink = output_sink
        self._udf_helpers = udf_helpers

    def classify_one(
        self, action: Action, tag: str, parent_tracer_span: Optional[TracerSpan] = None
    ) -> Optional[ExecutionResult]:
        sample_config = self._sampler.sample(action)
        tags = [
            tag,
            f'action:{action.action_name}',
            f'sample_rate:{sample_config.sample_rate}',
            f'profiler:{os.environ.get("RUN_PROFILER", "false")}',
            f'rules_hash:{self._engine.execution_graph.validated_sources.sources.hash()}',
        ]

        if sample_config.drop:
            metrics.increment('dropped_message', tags=tags)
            return None
        result: Optional[ExecutionResult] = None
        # noinspection PyBroadException
        try:
            with metrics.timed('handled_message', tags=tags, use_ms=True):
                result = self._engine.execute(
                    self._udf_helpers,
                    action,
                    sample_rate=sample_config.sample_rate,
                    parent_tracer_span=parent_tracer_span,
                )
            with metrics.timed('handled_output', tags=tags, use_ms=True):
                self._output_sink.push(result)
                info_log_osprey_action(action.action_id, action.action_name, 'pushed to output sink')
                return result
        except BaseException:
            sentry_sdk.capture_exception()
            return result


class RulesSink(BaseSink):
    """A rule sink takes an input stream, output sink and engine, executing each action produced by the input stream
    using the given engine, and pushing the result into the output sink."""

    def __init__(
        self,
        engine: OspreyEngine,
        input_stream: BaseInputStream[BaseAckingContext[Action]],
        output_sink: BaseOutputSink,
        udf_helpers: UDFHelpers,
        envoy_check_port: int = 8001,
    ):
        self._input_stream = input_stream
        self._rules_runner = RulesRunner(engine, output_sink, udf_helpers)
        self._envoy_check_port = envoy_check_port

    def run(self) -> None:
        envoy_check_server = get_envoy_check_server(self._envoy_check_port)
        envoy_check_server.start()
        for message_context in self._input_stream:
            try:
                with message_context as action:
                    if action.data.get('osprey_v2_skip_async_classification', False) or action.data.get(
                        'osprey_skip_async', False
                    ):
                        continue

                    with tracer.start_span('osprey.classify_one', child_of=None) as span:
                        tracer.context_provider.activate(span.context)

                        # We usually expect the coordinator to provide the action_id, but fall back to creating our own
                        if not action.action_id and action.action_id != 0:
                            action.action_id = generate_snowflake(retries=3).to_int()

                        info_log_osprey_action(action.action_id, action.action_name, 'beginning classify_one')
                        result = self._rules_runner.classify_one(action, tag='sink:rules-sink', parent_tracer_span=span)
                        if isinstance(message_context, VerdictsAckingContext):
                            if result is None:
                                info_log_osprey_action(
                                    action.action_id, action.action_name, 'execution result is None :<'
                                )
                                metrics.increment('rules_sink.missing_result')
                            else:
                                info_log_osprey_action(action.action_id, action.action_name, 'sending verdicts~')
                                message_context.set_verdicts(result.get_verdicts_pb2_proto())
                                metrics.increment('rules_sink.captured_verdicts')
                        info_log_osprey_action(action.action_id, action.action_name, 'classify_one complete')
            except gevent.GreenletExit:
                envoy_check_server.stop()
                return
            except Exception as e:
                logging.exception('Unexpected error in rules sink')
                metrics.increment('rules_sink.unexpected_error', tags=[f'err:{e.__class__.__name__}'])
                sentry_sdk.capture_exception(e)

    def stop(self) -> None:
        pass
