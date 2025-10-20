import copy
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
from datetime import timedelta
from typing import Any, Generator, Optional, Sequence

from osprey.engine.executor.external_service_utils import ExternalService
from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import (
    DroppedEntityLabelMutation,
    EntityLabelMutation,
    EntityLabelMutationsResult,
    EntityLabels,
    LabelState,
    LabelStateInner,
    LabelStatus,
    MutationDropReason,
)
from osprey.worker.lib.osprey_shared.logging import get_logger
from result import Err, Ok, Result
from tenacity import retry, stop_after_attempt, wait_exponential

logger = get_logger(__name__)


class LabelsServiceBase(ABC):
    """
    An abstract class to represent an implementable labels service backend.

    With the default LabelsProvider, read_labels and batch_read_labels are called
    *during* rule executions (or by the ui api).

    read_modify_write_labels_atomically is called post-rule execution (or by the ui api).
    """

    @abstractmethod
    def read_labels(self, entity: EntityT[Any]) -> EntityLabels:
        """
        A standard read from the labels service. Keep in mind that if there is a cache_ttl greater than 0 seconds,
        this method will not be called for every single label read.

        This method may be retried upon exceptions, so keep that in mind when adding potentially
        non-idempotent behaviour.
        """
        raise NotImplementedError()

    def batch_read_labels(self, entities: Sequence[EntityT[Any]]) -> Sequence[Result[EntityLabels, Exception]]:
        """
        Batching can optimize the number of RPCs that are sent out during executions,
        which has been observed to provide noticeable performance benefits in a python/gevent world.

        The order that the entieties are supplied in the incoming sequence will match the order the results are returned.

        By default, this will just call read_labels in a for-loop, but it is encouraged to implemenent your own batch
        endpoints and logic for the aforementioned performance benefits.
        """
        results: list[Result[EntityLabels, Exception]] = []
        for entity in entities:
            result: Result[EntityLabels, Exception] = Err(
                Exception('invariant: label could not be retrieved but no error was caught')
            )
            try:
                result = Ok(self.read_labels(entity))
            except Exception as e:
                result = Err(e)
            finally:
                results.append(result)
        return results

    @abstractmethod
    @contextmanager
    def read_modify_write_labels_atomically(self, entity: EntityT[Any]) -> Generator[EntityLabels, None, None]:
        """
        Context manager for atomic read-modify-write operations. This generator should yield EntityLabels upon reading
        and should write the EntityLabels post-yield.

        IMPORTANT: Implementations should ensure the entity key is locked/in a transaction so that other read-modify-write
                   calls (even across multiple workers) must wait.

        This code may be retried upon exceptions, so keep that in mind when adding potentially
        non-idempotent behaviour.
        """
        pass


class LabelsProvider(ExternalService[EntityT[Any], EntityLabels]):
    def __init__(self, labels_service: LabelsServiceBase):
        self._labels_service = labels_service

    def _get_mutations_by_label_name_and_drop_conflicts(
        self, mutations: Sequence[EntityLabelMutation]
    ) -> tuple[dict[str, list[EntityLabelMutation]], list[DroppedEntityLabelMutation]]:
        """
        collect mutations based on the value of their status. this means if a higher status and a lower status label mutation
        occur in the same mutations request, the lower status one(s) will be dropped.

        by the end of this method, the returned mutations will all be of the same label status for a given label.
        """
        # first, we collect all of the highest status mutations per label name. we collect a list because
        # same status mutations will need to be merged into a single label state later to represent all
        # applicable mutation reasons
        mutations_by_label_name: dict[str, list[EntityLabelMutation]] = defaultdict(list)
        dropped_mutations: list[DroppedEntityLabelMutation] = []
        for mutation in mutations:
            label_name = mutation.label_name
            if label_name in mutations_by_label_name:
                other_mutation = mutations_by_label_name[label_name][0]
                if mutation.status.value > other_mutation.status.value:
                    for mut in mutations_by_label_name[label_name]:
                        # we may have a list of more than one mutation if the statuses are all the same
                        dropped_mutations.append(
                            DroppedEntityLabelMutation(mutation=mut, reason=MutationDropReason.CONFLICTING_MUTATION)
                        )
                    mutations_by_label_name[label_name] = [mutation]
                    continue
                elif mutation.status.value < other_mutation.status.value:
                    dropped_mutations.append(
                        DroppedEntityLabelMutation(mutation=mutation, reason=MutationDropReason.CONFLICTING_MUTATION)
                    )
                    continue
            # if the status weights are equal or if there is no previous statuses, append
            mutations_by_label_name[label_name].append(mutation)

        return (mutations_by_label_name, dropped_mutations)

    def _get_desired_states_by_label_name(
        self, mutations_by_label_name: dict[str, list[EntityLabelMutation]]
    ) -> dict[str, LabelStateInner]:
        """
        given a dict of label names to entity label mutations, return the desired states that the mutations are seeking.
        if there is more than one mutation for a given label, the resulting state should contain a merge of the mutation reasons.
        """
        desired_states_by_label_name: dict[str, LabelStateInner] = dict()

        for label_name, mutations in mutations_by_label_name.items():
            assert len(mutations) > 0, 'invariant: mutations by label name should not be empty'
            assert len({mutation.status for mutation in mutations}) == 1, (
                'invariant: more than one unique label status AFTER dropping conflicts'
            )
            desired_state = mutations[0].desired_state()
            for i in range(1, len(mutations)):
                desired_state.reasons.insert_or_update(mutations[i].reason_name, mutations[i].reason)
            desired_states_by_label_name[label_name] = desired_state

        return desired_states_by_label_name

    def _compute_new_labels_from_mutations(
        self, labels: EntityLabels, mutations: Sequence[EntityLabelMutation]
    ) -> EntityLabelMutationsResult:
        """
        given an entity's labels and a set of mutations, modify the labels based on the mutations' desired states.

        **this method WILL modify the labels object that is passed into it**.
        it will also return the pre-modification labels in EntityLabelMutationsResult.old_labels
        """
        (mutations_by_label_name, dropped_mutations) = self._get_mutations_by_label_name_and_drop_conflicts(mutations)
        desired_states_by_label_name: dict[str, LabelStateInner] = self._get_desired_states_by_label_name(
            mutations_by_label_name
        )

        # lets take desired states and try to apply them to the entity labels.
        # for end-user convenience, we also track if labels are added, removed, updated, or if mutations are dropped entirely
        added: list[str] = []
        removed: list[str] = []
        updated: list[str] = []
        old_labels = copy.deepcopy(labels)
        for label_name, desired_state in desired_states_by_label_name.items():
            if label_name not in labels.labels:
                labels.labels[label_name] = LabelState.from_inner(desired_state)
                added.append(label_name)
                continue
            current_state = labels.labels[label_name]
            prev_status = current_state.status
            drop_reason = current_state.try_apply_desired_state(desired_state)
            if drop_reason:
                # if the current state rejected the desired state, we will drop the mutation(s) with the provided drop reason
                for mutation in mutations_by_label_name[label_name]:
                    dropped_mutations.append(DroppedEntityLabelMutation(mutation=mutation, reason=drop_reason))
                continue
            # otherwise, let's compare the new status so we can add data to the EntityLabelMutationsResult c:
            new_status = current_state.status
            if prev_status == new_status:
                updated.append(label_name)
                continue
            match new_status.effective_label_status():
                case LabelStatus.ADDED:
                    added.append(label_name)
                    continue
                case LabelStatus.REMOVED:
                    removed.append(label_name)
                    continue

        # finally, return the result! duhh :D
        return EntityLabelMutationsResult(
            new_entity_labels=labels,
            old_entity_labels=old_labels,
            labels_added=added,
            labels_removed=removed,
            labels_updated=updated,
            dropped_mutations=dropped_mutations,
        )

    @retry(wait=wait_exponential(min=0.5, max=5), stop=stop_after_attempt(3))
    def apply_entity_label_mutations_with_retry(
        self, entity: EntityT[Any], mutations: Sequence[EntityLabelMutation]
    ) -> EntityLabelMutationsResult:
        return self.apply_entity_label_mutations(entity=entity, mutations=mutations)

    def apply_entity_label_mutations(
        self, entity: EntityT[Any], mutations: Sequence[EntityLabelMutation]
    ) -> EntityLabelMutationsResult:
        try:
            with self._labels_service.read_modify_write_labels_atomically(entity) as entity_labels:
                result = self._compute_new_labels_from_mutations(entity_labels, mutations)
            return result
        except Exception as e:
            logger.error(f'Could not read-modify-write labels for entity {entity.__repr__()}:', e)
            raise e

    def cache_ttl(self) -> Optional[timedelta]:
        return timedelta(minutes=1)

    def get_from_service(self, key: EntityT[Any]) -> EntityLabels:
        return self._labels_service.read_labels(entity=key)

    def batch_get_from_service(self, keys: Sequence[EntityT[Any]]) -> Sequence[Result[EntityLabels, Exception]]:
        """
        Note: By default, the labels service batch_read_labels calls read_labels in a for loop.
              This is because the HasLabel UDF is batchable and requires batch support on the
              provider.

              If you would like to reap the performance benefits of batching, please re-implement
              the batch_read_labels to call a proper batch endpoint.

              See LabelsServiceBase.batch_read_labels for more information
        """
        return self._labels_service.batch_read_labels(entities=keys)

    def stop(self) -> None:
        """
        this method is called when the output sink receives a shutdown signal. if you would like to
        add shutdown logic, override this~
        """
        pass
