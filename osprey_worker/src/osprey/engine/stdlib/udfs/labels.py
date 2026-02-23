from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any

from osprey.engine.executor.udf_execution_helpers import HasHelperInternal
from osprey.engine.language_types.effects import (
    EffectBase,
)
from osprey.engine.language_types.entities import EntityT
from osprey.engine.language_types.labels import LabelEffect, LabelStatus
from osprey.engine.language_types.rules import RuleT
from osprey.engine.language_types.time_delta import TimeDeltaT
from osprey.engine.stdlib.configs.labels_config import LabelsConfig
from osprey.engine.stdlib.udfs._prelude import (
    ArgumentsBase,
    ConstExpr,
    ExecutionContext,
    UDFBase,
    ValidationContext,
)
from osprey.engine.stdlib.udfs.categories import UdfCategories
from osprey.engine.udf.base import BatchableUDFBase
from osprey.engine.utils.get_closest_string_within_threshold import (
    get_closest_string_within_threshold,
)
from osprey.worker.lib.osprey_shared.labels import EntityLabels
from osprey.worker.lib.storage.labels import LabelsProvider
from result import Err, Ok, Result


class LabelArguments(ArgumentsBase):
    entity: EntityT[Any]
    """An entity to mutate a label on."""
    label: ConstExpr[str]
    """The label to mutate."""
    # NOTE(ayubun): delayed actions are removed; they are legacy code from when discord used osprey
    #               to trigger webhooks upon label adds/removes.
    #
    #               we may eventually add something *similar* to this in the future? but i suspect
    #               that a better abstraction would be to have any sort of "external impact" come
    #               from verdicts, which were created to be an output (whereas labels were created
    #               to simply store state, thus making label webhooks a leaky abstraction)
    # NOTE(@elijaharita): this is being re-added because removing it breaks backwards compatibility.
    delay_action_by: TimeDeltaT | None = None
    """Optional: Delays a label action by a specified `TimeDeltaT` time if Osprey is configured to."""
    apply_if: RuleT | None = None
    """Optional: Conditions that must be met for the label mutation to succeed."""
    expires_after: TimeDeltaT | None = None
    """Optional: Automatically expire the mutation after a specified `TimeDeltaT` time."""


def synthesize_effect(status: LabelStatus, arguments: LabelArguments) -> LabelEffect:
    return LabelEffect(
        entity=arguments.entity,
        status=status,
        name=arguments.label.value,
        expires_after=TimeDeltaT.inner_from_optional(arguments.expires_after),
        delay_action_by=TimeDeltaT.inner_from_optional(arguments.delay_action_by),
        dependent_rule=arguments.apply_if,
        # NOTE: This is fairly significant, if this call node has an `apply_if` ast, but
        # the resolved apply_if is None, that means that the evaluation of the rule failed.
        # In this case, we'll need to suppress evaluation of this effect, as to not fail
        # open, in the event of a failed rule applying a filter. Safer here is better
        # than sorry.
        suppressed=arguments.has_argument_ast('apply_if') and arguments.apply_if is None,
    )


class LabelAdd(UDFBase[LabelArguments, EffectBase]):
    """Adds a label to the provided Entity."""

    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: LabelArguments) -> EffectBase:
        return synthesize_effect(status=LabelStatus.ADDED, arguments=arguments)


class LabelRemove(UDFBase[LabelArguments, EffectBase]):
    """Removes a label from the provided Entity."""

    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: LabelArguments) -> EffectBase:
        return synthesize_effect(status=LabelStatus.REMOVED, arguments=arguments)


class _ManualType(Enum):
    YES = auto()
    NO = auto()
    EITHER = auto()

    @classmethod
    def get(cls, manual: bool | None) -> '_ManualType':
        if manual is True:
            return _ManualType.YES
        if manual is False:
            return _ManualType.NO
        if manual is None:
            return _ManualType.EITHER
        else:
            raise TypeError(f'Unexpected argument {manual!r}')


class _SimpleStatus(Enum):
    ADDED = 'added'
    REMOVED = 'removed'


class EmptyEntityError(Exception):
    """Raised when an entity has no labels and error_on_empty is True."""

    def __init__(self, entity: EntityT[Any], label: str):
        self.entity = entity
        self.label = label
        super().__init__(
            f"Entity '{entity.type}/{entity.id}' has no labels. "
            f'This may indicate the labels service failed to fetch data. '
            f"(Checked for label: '{label}')"
        )


class HasLabelArguments(ArgumentsBase):
    entity: EntityT[Any]
    """An Entity to check for a label on."""
    label: ConstExpr[str]
    """A label name to check the state of."""
    manual: bool | None = None
    """Optional: If `True`, only check if the label was manually added by an operator."""
    status: ConstExpr[str] = ConstExpr.for_default('status', _SimpleStatus.ADDED.value)
    """Optional: A specific status to check for. Default is 'added'."""
    min_label_age: TimeDeltaT | None = None
    """Optional: Checks to see if the label was added after a period of time"""
    error_on_empty: bool = False
    """Optional: If True, raise EmptyEntityError when the entity has no labels at all.

    WARNING: Only use this for safety-critical rules where a false negative (due to labels
    service returning empty data on failure) could cause dangerous rule evaluations, such as
    incorrectly allowing a known-bad entity through. Do not use this for general label checks.

    This parameter should only be used when the entity type is guaranteed to have at least one
    label in the labels service. If the entity type is not guaranteed to have labels, the rule
    should add a dummy/sentinel label to the entity before calling HasLabel with error_on_empty=True.
    """


@dataclass
class BatchableHasLabelArguments:
    entity: EntityT[Any]
    label: str
    manual: bool | None
    status: str
    min_label_age: TimeDeltaT | None
    desired_status: _SimpleStatus | None
    error_on_empty: bool


class HasLabel(
    HasHelperInternal[LabelsProvider], BatchableUDFBase[HasLabelArguments, bool, BatchableHasLabelArguments]
):
    """Returns `True` if the specified label is currently present in a given non-expired state on a provided Entity."""

    category = UdfCategories.ENGINE

    def __init__(self, validation_context: ValidationContext, arguments: HasLabelArguments) -> None:
        super().__init__(validation_context, arguments)
        status_name = arguments.status.value
        try:
            self.desired_status: _SimpleStatus | None = _SimpleStatus(status_name)
        except ValueError:
            self.desired_status = None

            hint = f'expected `{_SimpleStatus.ADDED.value}` or `{_SimpleStatus.REMOVED.value}`, got `{status_name}`'
            if status_name.upper() in (
                LabelStatus.MANUALLY_ADDED.name,
                LabelStatus.MANUALLY_REMOVED.name,
            ):
                hint += '\nto specify a manually set label, set `manual=True`'

            validation_context.add_error(message='unknown label status', span=arguments.status.argument_span, hint=hint)

        label_config: LabelsConfig = validation_context.get_config_subkey(LabelsConfig)
        if arguments.label.value not in label_config.labels:
            hint = f'unknown label `{arguments.label.value}`'
            closest_name = get_closest_string_within_threshold(
                string=arguments.label.value, candidate_strings=label_config.labels
            )
            if closest_name is not None:
                hint += f', did you mean `{closest_name}`?'

            validation_context.add_error(message='unknown label', span=arguments.label.argument_span, hint=hint)

    def _check_error_on_empty(
        self, entity: EntityT[Any], label: str, entity_labels: EntityLabels, error_on_empty: bool
    ) -> None:
        """Fail-closed check for labels service data integrity.

        When error_on_empty is True, raises EmptyEntityError if the entity has zero labels.
        This catches cases where the labels service may have failed to fetch data and returned
        an empty default response. Without this check, `not HasLabel(...)` would incorrectly
        evaluate to True, potentially allowing dangerous entities through safety rules.
        """
        if error_on_empty and len(entity_labels.labels) == 0:
            raise EmptyEntityError(entity, label)

    def _execute(
        self, execution_context: ExecutionContext, arguments: BatchableHasLabelArguments, entity_labels: EntityLabels
    ) -> bool:
        self._check_error_on_empty(arguments.entity, arguments.label, entity_labels, arguments.error_on_empty)
        desired_manual = _ManualType.get(arguments.manual)
        desired_delay = TimeDeltaT.inner_from_optional(arguments.min_label_age)
        label_state = entity_labels.labels.get(arguments.label)
        now = datetime.now(timezone.utc)

        if label_state is not None:
            # Check to see if all reasons have expired, if so, the label should be considered as expired.
            # Only consider a reason expired if it has a meaningful expires_at timestamp (not default/epoch)
            all_reasons_expired = all(
                reason.expires_at
                and reason.expires_at.second > 0  # Check if timestamp is not default/epoch
                and reason.expires_at <= now
                for reason in label_state.reasons.values()
            )
            if all_reasons_expired:
                label_state = None

        if label_state is None:
            return self.desired_status == _SimpleStatus.REMOVED and desired_manual != _ManualType.YES

        if label_state.status == LabelStatus.ADDED:
            actual_status = _SimpleStatus.ADDED
            actual_manual = _ManualType.NO
        elif label_state.status == LabelStatus.MANUALLY_ADDED:
            actual_status = _SimpleStatus.ADDED
            actual_manual = _ManualType.YES
        elif label_state.status == LabelStatus.REMOVED:
            actual_status = _SimpleStatus.REMOVED
            actual_manual = _ManualType.NO
        elif label_state.status == LabelStatus.MANUALLY_REMOVED:
            actual_status = _SimpleStatus.REMOVED
            actual_manual = _ManualType.YES
        else:
            raise TypeError(f'Unknown LabelStatus {label_state.status!r}')

        if desired_delay is not None:
            # Get the oldest non-expired label
            oldest_non_expired = min(
                reason.created_at
                for reason in label_state.reasons.values()
                if reason.created_at
                and (
                    not reason.expires_at
                    or reason.expires_at.second == 0  # No meaningful expiration set
                    or reason.expires_at > now
                )
            )
            actual_delay = now - oldest_non_expired

        return (
            self.desired_status == actual_status
            and desired_manual in (_ManualType.EITHER, actual_manual)
            and (desired_delay is None or actual_delay > desired_delay)
        )

    def execute(self, execution_context: ExecutionContext, arguments: HasLabelArguments) -> bool:
        label_provider = execution_context.get_udf_helper(self)
        accessor = execution_context.get_external_service_accessor(label_provider)
        entity_labels = accessor.get(arguments.entity)
        return self._execute(execution_context, self.get_batchable_arguments(arguments), entity_labels)

    def get_batchable_arguments(self, arguments: HasLabelArguments) -> BatchableHasLabelArguments:
        return BatchableHasLabelArguments(
            entity=arguments.entity,
            label=arguments.label.value,
            manual=arguments.manual,
            status=arguments.status.value,
            min_label_age=arguments.min_label_age,
            desired_status=self.desired_status,
            error_on_empty=arguments.error_on_empty,
        )

    def get_batch_routing_key(self, arguments: BatchableHasLabelArguments) -> str:
        """
        Returns routing key based on entity to ensure same-entity labels are batched together.

        This ensures that execute_batch() always receives arguments for a single unique entity,
        avoiding the NotImplementedError for multiple entities.
        """
        return arguments.entity.type + '/' + str(arguments.entity.id)

    def execute_batch(
        self,
        execution_context: ExecutionContext,
        udfs: Sequence[UDFBase[Any, Any]],
        arguments: Sequence[BatchableHasLabelArguments],
    ) -> Sequence[Result[bool, Exception]]:
        unique_entities = set()
        for arg in arguments:
            unique_entities.add(arg.entity)

        label_provider = execution_context.get_udf_helper(self)
        accessor = execution_context.get_external_service_accessor(label_provider)

        if len(unique_entities) == 1:
            # no need to batch if there is only one unique entity.
            # we actually expect all execute_batches to take this route, since the executor
            # batches based on the routing key (which is the entity string).
            entity_labels_pb2 = accessor.get(unique_entities.pop())
            output = []
            for args in arguments:
                try:
                    output.append(Ok(self._execute(execution_context, args, entity_labels_pb2)))
                except Exception as e:
                    output.append(Err(e))
            return output

        # in case the routing key ever gets changed to allow for multiple unique entities,
        # i went ahead and made sure we had that supported ^^ p.s. you might want to test this locally
        raise NotImplementedError
        # unique_entities_list = list(unique_entities)
        # entity_labels_list = accessor.batch_get([entity for entity in unique_entities_list])
        # entity_to_labels = {entity: labels for entity, labels in zip(unique_entities_list, entity_labels_list)}
        # output = []
        # for args in arguments:
        #     if entity_to_labels[args.entity].is_err():
        #         output.append(Err(entity_to_labels[args.entity].value))
        #         continue
        #     try:
        #         # unfortunately, mypy doesn't recognize that entity_to_labels[args.entity] is an Ok type
        #         output.append(Ok(self._execute(execution_context, args, entity_to_labels[args.entity].value)))  # type: ignore
        #     except Exception as e:
        #         output.append(Err(e))
        # return output
