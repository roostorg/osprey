import abc
from collections import defaultdict
from datetime import datetime
from typing import Any, DefaultDict, Dict, Mapping, Optional, Sequence

import gevent
import sentry_sdk
from osprey.engine.executor.execution_context import (
    ExecutionResult,
)
from osprey.engine.language_types.entities import EntityT
from osprey.engine.language_types.labels import LabelEffect
from osprey.engine.stdlib.udfs.rules import RuleT
from osprey.worker.lib.ddtrace_utils import trace
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.labels import EntityLabelMutation
from osprey.worker.lib.osprey_shared.logging import DynamicLogSampler, get_logger
from osprey.worker.lib.storage.labels import LabelsProvider
from osprey.worker.ui_api.osprey.validators.entities import EntityKey

logger = get_logger()

GEVENT_TIMEOUT = 2


class BaseOutputSink(abc.ABC):
    @abc.abstractmethod
    def will_do_work(self, result: ExecutionResult) -> bool:
        """A quick way to determine if this sink needs to do anything for this result."""
        raise NotImplementedError

    @abc.abstractmethod
    def push(self, result: ExecutionResult) -> None:
        """
        A sink is responsible for handling its own exceptions.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def stop(self) -> None:
        raise NotImplementedError


class MultiOutputSink(BaseOutputSink):
    """An output sink that tees the execution results to multiple children sinks."""

    def __init__(self, sinks: Sequence[BaseOutputSink]):
        self._sinks = sinks

    def will_do_work(self, result: ExecutionResult) -> bool:
        return any(sink.will_do_work(result) for sink in self._sinks)

    def push(self, result: ExecutionResult) -> None:
        errors: Dict[BaseOutputSink, BaseException] = {}

        for sink in self._sinks:
            if sink.will_do_work(result):
                try:
                    with trace(f'{sink.__class__.__name__}.push'), gevent.Timeout(GEVENT_TIMEOUT):
                        sink.push(result)
                except gevent.Timeout as timeout_exc:
                    logger.exception(
                        f'Timeout exception raised when pushing event to sink: {str(sink.__class__.__name__)}'
                    )
                    errors[sink] = timeout_exc
                    metrics.increment('output_sink.timeout', tags=[f'sink:{sink.__class__.__name__}'])
                    # Capture the Timeout exception
                    sentry_sdk.capture_exception()
                except Exception as exc:
                    errors[sink] = exc
                    metrics.increment(
                        'output_sink.error', tags=[f'sink:{sink.__class__.__name__}', f'error:{exc.__class__.__name__}']
                    )
                    # Capture the current exception for now until we fix PartialSinkFailure
                    sentry_sdk.capture_exception()

    def stop(self) -> None:
        for sink in self._sinks:
            sink.stop()

        # TODO: Uncomment after making PartialSinkFailure more useful
        # if errors:
        #     raise PartialSinkFailure(errors)


class StdoutOutputSink(BaseOutputSink):
    """An output sink that prints to standard out!"""

    def __init__(self, log_sampler: Optional[DynamicLogSampler] = None):
        self.logger = get_logger('StdoutOutputSink', log_sampler)

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    def push(self, result: ExecutionResult) -> None:
        self.logger.info(f'result: {result.extracted_features_json} {result.verdicts}')

    def stop(self) -> None:
        pass


def _create_entity_mutation(
    label_effect: LabelEffect, rule: RuleT, expires_at: Optional[datetime]
) -> EntityLabelMutation:
    return EntityLabelMutation(
        label_name=label_effect.name,
        reason_name=rule.name,
        status=label_effect.status,
        description=rule.description,
        features=rule.features,
        expires_at=expires_at,
    )


def _get_label_effects_from_result(result: ExecutionResult) -> Mapping[EntityT[Any], list[EntityLabelMutation]]:
    effects: DefaultDict[EntityT[Any], list[EntityLabelMutation]] = defaultdict(list)

    for label_effect in result.effects.get(LabelEffect, []):
        # assert for typing
        assert isinstance(label_effect, LabelEffect), (
            'impossible D: effect in label effect mapping is not a label effect'
        )

        # The effect was suppressed, so we can skip over this one.
        if label_effect.suppressed:
            continue

        # If we have a dependent rule, but it has not evaluated to true, we
        # can skip this effect entirely.
        dependent_rule = label_effect.dependent_rule
        if dependent_rule and not dependent_rule.value:
            continue

        expires_after = label_effect.expires_after
        expires_at = None if expires_after is None else result.action.timestamp + expires_after
        entity_mutations = effects[label_effect.entity]

        if dependent_rule:
            entity_mutations.append(
                _create_entity_mutation(label_effect=label_effect, rule=dependent_rule, expires_at=expires_at)
            )

        entity_mutations += [
            _create_entity_mutation(label_effect=label_effect, rule=rule, expires_at=expires_at)
            for rule in label_effect.rules
        ]

    return dict(effects)


class LabelOutputSink(BaseOutputSink):
    """An output sink that will send event effects to the label service."""

    def __init__(self, labels_provider: LabelsProvider) -> None:
        self._labels_provider = labels_provider

    def will_do_work(self, result: ExecutionResult) -> bool:
        return len(_get_label_effects_from_result(result)) > 0

    def push(self, result: ExecutionResult) -> None:
        for entity, mutations in _get_label_effects_from_result(result).items():
            entity_key: EntityT[str] = EntityKey(type=str(entity.type), id=str(entity.id))
            result = self._labels_provider.apply_entity_label_mutations(
                entity_key,
                mutations,
            )

    def stop(self) -> None:
        self._labels_provider.stop()
        """
        we need to make sure the provider can stop and immediately execute all delayed label actions
        """
