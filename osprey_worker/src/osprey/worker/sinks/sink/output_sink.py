import abc
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, DefaultDict, Dict, List, Mapping, Optional, Sequence

import gevent
import sentry_sdk
import sqlalchemy
from osprey.engine.executor.execution_context import (
    ExecutionResult,
    ExtendedEntityMutation,
)
from osprey.engine.language_types.entities import EntityT
from osprey.engine.language_types.labels import LabelEffect
from osprey.engine.stdlib.configs.analytics_config import AnalyticsConfig
from osprey.engine.stdlib.configs.feature_flags_config import (
    WEBHOOKS_USE_PUBSUB,
    FeatureFlagsConfig,
)
from osprey.engine.stdlib.configs.webhook_config import WebhookConfig
from osprey.engine.stdlib.udfs.rules import RuleT
from osprey.engine.utils.proto_utils import optional_datetime_to_timestamp
from osprey.rpc.labels.v1.service_pb2 import ApplyEntityMutationReply, EntityKey, EntityMutation
from osprey.rpc.labels.v1.service_pb2 import LabelStatus as LabelStatusPb2
from osprey.worker.lib.ddtrace_utils import trace
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_engine import OspreyEngine
from osprey.worker.lib.osprey_shared.labels import LabelStatus
from osprey.worker.lib.osprey_shared.logging import DynamicLogSampler, get_logger
from osprey.worker.lib.publisher import BasePublisher
from osprey.worker.lib.storage import labels
from osprey.worker.lib.storage.entity_label_webhook import EntityLabelWebhook as StoredEntityLabelWebhook
from osprey.worker.lib.storage.postgres import scoped_session
from osprey.worker.lib.webhooks import WebhookStatus
from osprey.worker.sinks.sink.output_sink_utils.constants import MutationEventType
from osprey.worker.sinks.sink.output_sink_utils.helpers import get_user_id
from osprey.worker.sinks.sink.output_sink_utils.models import (
    OspreyEntityLabelWebhook,
    OspreyLabelMutationAnalyticsEvent,
)
from tenacity import retry, stop_after_attempt, wait_exponential

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
) -> ExtendedEntityMutation:
    return ExtendedEntityMutation(
        mutation=EntityMutation(
            label_name=label_effect.name,
            reason_name=rule.name,
            status=label_effect.status,
            description=rule.description,
            features=rule.features,
            expires_at=optional_datetime_to_timestamp(expires_at),
        ),
        delay_action_by=label_effect.delay_action_by,
    )


def _get_label_effects_from_result(result: ExecutionResult) -> Mapping[EntityT[Any], List[ExtendedEntityMutation]]:
    effects: DefaultDict[EntityT[Any], List[ExtendedEntityMutation]] = defaultdict(list)

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


class EventEffectsOutputSink(BaseOutputSink):
    """An output sink that will send event effects to the label service, and also, enqueue any applied or removed label
    state mutations that there is an interested webhook listener.
    """

    def __init__(
        self, engine: OspreyEngine, analytics_publisher: BasePublisher, webhooks_publisher: BasePublisher
    ) -> None:
        self._engine = engine
        self._analytics_publisher = analytics_publisher
        self._webhooks_publisher = webhooks_publisher

    def will_do_work(self, result: ExecutionResult) -> bool:
        return len(_get_label_effects_from_result(result)) > 0

    def push(self, result: ExecutionResult) -> None:
        for entity, mutations in _get_label_effects_from_result(result).items():
            entity_key = EntityKey(type=entity.type, id=str(entity.id))
            self.apply_label_mutations(
                MutationEventType.OSPREY_ACTION,
                str(result.action.action_id),
                entity_key,
                mutations,
                result.extracted_features,
                mutation_event_action_name=result.action.action_name,
            )

    @staticmethod
    @retry(wait=wait_exponential(min=0.5, max=5), stop=stop_after_attempt(3))
    def apply_entity_mutation_with_retry(
        entity_key: EntityKey, mutations: Sequence[ExtendedEntityMutation]
    ) -> ApplyEntityMutationReply:
        return labels.apply_entity_mutation(
            entity_key=entity_key, mutations=[mutation.mutation for mutation in mutations]
        )

    def apply_label_mutations_pb2(
        self,
        mutation_event_type: MutationEventType,
        mutation_event_id: str,
        entity_key: EntityKey,
        mutations: Sequence[ExtendedEntityMutation],
        features: Optional[Dict[str, Any]] = None,
        mutation_event_action_name: str = '',
    ) -> ApplyEntityMutationReply:
        return self.apply_label_mutations(
            mutation_event_type=mutation_event_type,
            mutation_event_id=mutation_event_id,
            entity_key=entity_key,
            mutations=mutations,
            features=features,
            mutation_event_action_name=mutation_event_action_name,
        )

    def apply_label_mutations(
        self,
        mutation_event_type: MutationEventType,
        mutation_event_id: str,
        entity_key: EntityKey,
        mutations: Sequence[ExtendedEntityMutation],
        features: Optional[Dict[str, Any]] = None,
        mutation_event_action_name: str = '',
    ) -> ApplyEntityMutationReply:
        if not entity_key.id:
            metrics.increment(
                'output_sink.apply_entity_mutation',
                tags=['status:skipped', 'reason:no_entity_id', f'entity_type:{entity_key.type}'],
            )
            return ApplyEntityMutationReply(
                unchanged=[mutation.mutation.label_name for mutation in mutations],
            )

        if not features:
            features = {}

        try:
            result: ApplyEntityMutationReply = EventEffectsOutputSink.apply_entity_mutation_with_retry(
                entity_key, mutations
            )
            metrics.increment('output_sink.apply_entity_mutation', tags=['status:success'])
        except Exception as e:
            logger.error(
                f'Failed to apply entity mutation on entity of type: {entity_key.type} with id: {entity_key.id} - {e}',
                exc_info=True,
            )
            metrics.increment('output_sink.apply_entity_mutation', tags=['status:failure'])
            raise e

        added = set(result.added)
        removed = set(result.removed)
        updated_labels = added | removed

        if not updated_labels:
            metrics.increment('output_sink.no_updated_labels')
            return result

        # These analytics and metrics placed in this sink because we need to know when label states changes.
        self._send_label_mutation_analytics_event(
            mutation_event_type,
            mutation_event_id,
            mutation_event_action_name or mutation_event_type.value,
            entity_key,
            result,
            mutations,
        )
        self._send_monitored_rules_metrics(
            mutations, action_name=mutation_event_action_name or mutation_event_type.value
        )

        webhook_config = self._engine.get_config_subkey(WebhookConfig)
        updated_labels_with_downstream = [
            label_name for label_name in updated_labels if label_name in webhook_config.outgoing_labels
        ]

        if not updated_labels_with_downstream:
            metrics.increment('output_sink.no_updated_labels_with_downstream')
            return result

        delay_action_by_per_label = self._get_action_delay_by_per_label(mutations, updated_labels_with_downstream)

        # if the pgbouncer pod is restarted, the connection pool will be cleared and the session will be invalid
        # we need to retry the transaction in this case
        @retry(
            wait=wait_exponential(min=0.5, max=5),
            stop=stop_after_attempt(3),
        )
        def run_transaction(_features: Dict[str, Any]) -> None:
            with scoped_session(commit=True) as session:
                for label_name in updated_labels_with_downstream:
                    feature_flags = self._engine.get_config_subkey(FeatureFlagsConfig)

                    label_status = LabelStatus.ADDED if label_name in added else LabelStatus.REMOVED
                    metrics.increment('labels', tags=[f'label:{label_name}', f'status:{label_status.name}'])

                    filtered_features = {
                        feature_name: _features[feature_name]
                        for feature_name in webhook_config.outgoing_labels_features_to_include.get(label_name, [])
                        if feature_name in _features
                    }

                    delay_action_by = delay_action_by_per_label[label_name]

                    if delay_action_by is None and feature_flags.is_percentage_enabled(WEBHOOKS_USE_PUBSUB):
                        # Pubsub path
                        pubsub_webhook = OspreyEntityLabelWebhook(
                            entity_type=entity_key.type,
                            entity_id=entity_key.id,
                            label_name=label_name,
                            label_status=label_status,
                            webhook_name=label_name,
                            features=filtered_features,
                            created_at=datetime.now(),
                        )

                        self._webhooks_publisher.publish(pubsub_webhook)
                        metrics.increment('webhook_pubsub_writes', tags=['status:success'])
                    else:
                        # Traditional Postgres path
                        postgres_webhook = EventEffectsOutputSink._create_webhook(
                            entity_key, label_name, label_status, filtered_features, delay_action_by
                        )
                        session.add(postgres_webhook)

        try:
            # lol, this half step is necessary to get the mypy linters passing or else it will complain about
            # subcripting on a NoneType. Therefore the argument to run_transaction is not an optional in order to pass.
            run_transaction(features)
            # database operation has been committed, increment success metric
            metrics.increment('webhook_queue_postgres_writes', tags=['status:success'])
        except Exception:
            # increment failure metric
            metrics.increment('webhook_queue_postgres_writes', tags=['status:failure'])
            logger.error(
                f'Failed to write webhooks to database for labels: {updated_labels_with_downstream} '
                f'on entity of type: {entity_key.type} with id: {entity_key.id}',
                exc_info=True,
            )
            # re-raise the exception so that outer function (multi-output sink) can handle it
            raise

        return result

    def _send_label_mutation_analytics_event(
        self,
        mutation_event_type: MutationEventType,
        mutation_event_id: str,
        mutation_event_action_name: str,
        entity_key: EntityKey,
        label_mutation_result: ApplyEntityMutationReply,
        mutations: Sequence[ExtendedEntityMutation],
    ) -> None:
        analytics_properties = OspreyLabelMutationAnalyticsEvent(
            mutation_event_type=mutation_event_type,
            mutation_event_id=mutation_event_id,
            mutation_event_action_name=mutation_event_action_name,
            user_id=get_user_id(entity_key.id, entity_key.type),
            entity_id_v2=entity_key.id,
            entity_type=entity_key.type,
            labels=[],
            label_statuses=[],
            label_reasons=[],
        )

        config = self._engine.get_config_subkey(AnalyticsConfig)
        filtered_labels_for_analytics = config.filtered_labels
        for mutation in mutations:
            label_name = mutation.mutation.label_name
            if (
                label_name not in (set(label_mutation_result.added) | set(label_mutation_result.removed))
                or label_name in filtered_labels_for_analytics
            ):
                continue

            # it is possible to have no reasons, usually due to bad test state, this gives a sane default
            reason = mutation.mutation.reason_name or 'unknown'
            analytics_properties.labels.append(label_name)
            analytics_properties.label_statuses.append('LabelStatus.' + LabelStatusPb2.Name(mutation.mutation.status))
            analytics_properties.label_reasons.append(reason)

        self._analytics_publisher.publish(analytics_properties)

    def _get_action_delay_by_per_label(
        self,
        mutations: Sequence[ExtendedEntityMutation],
        updated_labels_with_downstream: List[str],
    ) -> Dict[str, Optional[timedelta]]:
        # Build mapping from label to action delay
        delay_action_by_per_label: Dict[str, Optional[timedelta]] = {}
        for mutation in mutations:
            label_name = mutation.mutation.label_name
            if label_name in updated_labels_with_downstream:
                if label_name not in delay_action_by_per_label:
                    new_value = mutation.delay_action_by
                else:
                    # Take the minimum, where None (no delay) takes precedence
                    existing = delay_action_by_per_label[label_name]
                    if existing is None or mutation.delay_action_by is None:
                        new_value = None
                    else:
                        new_value = min(existing, mutation.delay_action_by)
                delay_action_by_per_label[label_name] = new_value
        return delay_action_by_per_label

    @classmethod
    def _create_webhook(
        cls,
        entity_key: EntityKey,
        label_name: str,
        label_status: LabelStatus,
        features: Dict[str, Any],
        delay_action_by: Optional[timedelta],
    ) -> StoredEntityLabelWebhook:
        webhook = StoredEntityLabelWebhook()
        webhook.entity_id = entity_key.id
        webhook.entity_type = entity_key.type
        # The webhook name is currently the label name, which means there is no specifier to only listen to
        # entity effects by entity type. Just by label name.
        webhook.webhook_name = label_name
        webhook.label_name = label_name
        webhook.label_status = LabelStatus(labels.get_effective_label_status(label_status.value))
        webhook.features = features
        webhook.claim_until = webhook.created_at = webhook.updated_at = sqlalchemy.func.now()
        webhook.status = WebhookStatus.QUEUED
        if delay_action_by is not None:
            # Use the claim to prevent sending this webhook until the allotted time.
            webhook.claim_until = sqlalchemy.func.now() + delay_action_by
        return webhook

    def _send_monitored_rules_metrics(self, mutations: Sequence[ExtendedEntityMutation], action_name: str) -> None:
        analytics_config = self._engine.get_config_subkey(AnalyticsConfig)
        for mutation in mutations:
            label_name = mutation.mutation.label_name
            if label_name in analytics_config.monitored_labels:
                metrics.increment(
                    'monitored_rules',
                    tags=[
                        f'rule:{mutation.mutation.reason_name}',
                        f'label:{label_name}',
                        f'status:{mutation.mutation.status}',
                        f'action:{action_name}',
                    ],
                )

    def stop(self) -> None:
        self._analytics_publisher.stop()
