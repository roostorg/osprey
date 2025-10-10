import copy
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
from datetime import timedelta
from typing import Any, Generator, Generic, Optional, Sequence, TypeVar

from osprey.engine.executor.external_service_utils import ExternalService
from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import (
    EntityLabelMutation,
    EntityLabelMutationsResult,
    EntityLabels,
    ExtendedEntityLabelMutation,
)
from osprey.worker.lib.osprey_shared.logging import get_logger
from result import Err, Ok, Result
from tenacity import retry, stop_after_attempt, wait_exponential

logger = get_logger(__name__)

class LabelsServiceBase(ABC):
    @abstractmethod
    def write_labels(self, entity: EntityT[Any], labels: EntityLabels) -> None:
        """
        A standard write to the labels service that attempts to write the value to the primary key.

        This method may be retried upon exceptions, so keep that in mind when adding potentially
        non-idempotent behaviour.
        """
        raise NotImplementedError()

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
            result: Result[EntityLabels, Exception] = Err(Exception('invariant: label could not be retrieved but no error was caught'))
            try:
                result: Result[EntityLabels, Exception] = Ok(self.read_labels(entity))
            except Exception as e:
                result: Result[EntityLabels, Exception] = Err(e)
            finally:
                results.append(result)
        return results

    @abstractmethod
    @contextmanager
    def get_labels_atomically(self, entity: EntityT[Any]) -> Generator[EntityLabels, None, None]:
        """
        Context manager for atomic read-modify-write operations.
        Implementations should ensure the entity key is locked/in a transaction.
        """
        pass


class LabelsProvider(ExternalService[EntityT[Any], EntityLabels]):

    def __init__(self, labels_service: LabelsServiceBase):
        self._labels_service = labels_service

    @retry(wait=wait_exponential(min=0.5, max=5), stop=stop_after_attempt(3))
    def apply_entity_label_mutations_with_retry(
        self, entity: EntityT[Any], mutations: Sequence[ExtendedEntityLabelMutation]
    ) -> EntityLabelMutationsResult:
        return self.apply_entity_label_mutations(
            entity=entity, mutations=mutations
        )

    def _get_mutations_by_label_name_and_dropped_conflicts(self, mutations: Sequence[EntityLabelMutation]) -> tuple[dict[str, list[EntityLabelMutation]], list[EntityLabelMutation]]:
        """
        collect mutations based on the value of their status. this means if a higher status and a lower status label mutation
        occur in the same mutations request, the lower status one(s) will be discarded / dropped.
        """
        mutations_by_label_name: dict[str, list[EntityLabelMutation]] = defaultdict(list)
        dropped_mutations: list[EntityLabelMutation] = []
        for mutation in mutations:
            label_name = mutation.label_name
            if label_name in mutations_by_label_name:
                other_mutation = mutations_by_label_name[label_name][0]
                if mutation.status.value > other_mutation.status.value:
                    for mut in mutations_by_label_name[label_name]:
                        # we may have a list of more than one mutation if the statuses are all the same
                        dropped_mutations.append(mut)
                    mutations_by_label_name[label_name] = mutation
                    continue
                elif mutation.status.weight < other_mut.status.weight:
                    dropped_mutations.append(mutation)
                    continue
            # if the status weights are equal or if there is no previous statuses, append
            mutations_by_label_name[label_name].append(mutation)

        return (mutations_by_label_name, dropped_mutations)

    def apply_entity_label_mutations(
        self, entity: EntityT[Any], mutations: Sequence[EntityLabelMutation]
    ) -> EntityLabelMutationsResult:
        with self._labels_service.get_labels_atomically(entity) as old_labels:
            new_labels = copy.deepcopy(old_labels)

            added: list[str] = []
            removed: list[str] = []
            updated: list[str] = []
            (mutations_by_label_name, dropped) = self._get_mutations_by_label_name_and_dropped_conflicts(mutations)

            # now, perform any necessary status & reason merging
            for label_name, mutations_list in mutations_by_label_name.items():
                assert len({mutation.status for mutation in mutations_list}) == 1, (
                    f'invariant: requested mutations had the same weights but different statuses: {mutations_list} (for label {label_name})'
                )
                label_state = mutations_list[0].desired_state()
                old_state = old_labels.labels.get(label_name)
                for i in range(1, len(mutations_list)):
                    mut = mutations_list[i]
                    success = label_state.append_reason(mut.reason())
                    if not success:
                        logger.error('label state could not be computed from merging mutation: ', mut)
                        dropped.append(mut)
                    continue
                if label_name not in new_labels.labels:
                    new_labels.labels[label_name] = label_state
                else:
                    new_labels.labels[label_name].update_status(label_state.status, label_state.reasons)
                updated.append(label_name)

            # finally, return the result! duhh :D
            return EntityLabelMutationsResult(
                new_entity_labels=new_labels,
                old_entity_labels=old_labels,
                added=added,
                removed=removed,
                updated=updated,
                dropped=dropped,
            )

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

