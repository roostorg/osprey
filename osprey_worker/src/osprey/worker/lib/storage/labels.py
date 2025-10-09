import copy
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import timedelta
from typing import Any, Optional, Sequence

from result import Result
from tenacity import retry, stop_after_attempt, wait_exponential

from osprey.engine.executor.external_service_utils import ExternalService, KeyT, ValueT
from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import (
    EntityLabelMutationsResult,
    EntityLabelMutation,
    EntityLabels,
)
from osprey.worker.lib.osprey_shared.logging import get_logger

logger = get_logger(__name__)

class BaseLabelsService(ABC):
    @abstractmethod
    def write_labels(self, key: EntityT[Any], value: EntityLabels) -> None:
        """
        A standard write to the labels service that attempts to write the value to the primary key.

        This method may be retried upon exceptions, so keep that in mind when adding potentially
        non-idempotent behaviour.
        """
        raise NotImplementedError()

    def get_entity_labels(self, key: EntityT[Any]) -> EntityLabels:
        """
        A standard read from the labels service. Keep in mind that if there is a cache_ttl greater than 0 seconds,
        this method will not be called for every single label read.

        This method may be retried upon exceptions, so keep that in mind when adding potentially
        non-idempotent behaviour.
        """
        raise NotImplementedError()

    @abstractmethod
    def batch_get_entity_labels(self, keys: Sequence[EntityT[Any]]) -> Sequence[Result[EntityLabels, Exception]]:
        """
        Batching can optimize the number of RPCs that are sent out during executions,
        which has been observed to provide noticeable performance benefits in a python/gevent world.

        If your labels service does not support a batch request endpoint, you can simply for-loop calls to get_from_service.
        """
        raise NotImplementedError()


class LabelsProvider(ExternalService[EntityT[Any], EntityLabels]):

    def __init__(self, labels_service: BaseLabelsService):
        self.labels_service = labels_service


    @retry(wait=wait_exponential(min=0.5, max=5), stop=stop_after_attempt(3))
    def apply_entity_label_mutations_with_retry(
        self, entity: EntityT[Any], mutations: Sequence[EntityLabelMutation]
    ) -> EntityLabelMutationsResult:
        return self.apply_entity_label_mutations(
            entity=entity, mutations=mutations
        )

    def apply_entity_label_mutations(
        self, entity: EntityT[Any], mutations: Sequence[EntityLabelMutation]
    ) -> EntityLabelMutationsResult:
        old_labels = self.get_from_service(entity)
        new_labels = copy.deepcopy(old_labels)

        updated: list[str] = []
        dropped: list[EntityLabelMutation] = []

        # first, drop lower weight statuses and collect same-weight statuses
        mutations_by_label_name: dict[str, list[EntityLabelMutation]] = defaultdict(list)
        for mutation in mutations:
            label_name = mutation.label_name
            if label_name in mutations_by_label_name:
                other_mut = mutations_by_label_name[label_name][0]
                if mutation.status.weight > other_mut.status.weight:
                    for mut in mutations_by_label_name[label_name]:
                        dropped.append(mut)
                    mutations_by_label_name[label_name] = mutation
                    continue
                elif mutation.status.weight < other_mut.status.weight:
                    dropped.append(mutation)
                    continue
            # if the status weights are equal or if there is no previous statuses, append
            mutations_by_label_name[label_name].append(mutation)

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
            updated=updated,
            dropped=dropped,
        )


    def cache_ttl(self) -> Optional[timedelta]:
        return timedelta(minutes=1)

    def get_from_service(self, key: EntityT[Any]) -> EntityLabels:
        """
        A standard read from the labels service. Keep in mind that if there is a cache_ttl greater than 0 seconds,
        this method will not be called for every single label read.

        This method may be retried upon exceptions, so keep that in mind when adding potentially
        non-idempotent behaviour.
        """
        return self.labels_service.get_entity_labels(key)

    def batch_get_from_service(self, keys: Sequence[EntityT[Any]]) -> Sequence[Result[EntityLabels, Exception]]:
        """
        Batching can optimize the number of RPCs that are sent out during executions,
        which has been observed to provide noticeable performance benefits in a python/gevent world.

        If your labels service does not support a batch request endpoint, you can simply for-loop calls to get_from_service.
        """
        return self.labels_service.batch_get_entity_labels(keys)
