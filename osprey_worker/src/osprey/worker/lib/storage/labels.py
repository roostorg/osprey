import copy
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import timedelta
from typing import Any, Optional, Sequence

from gevent import Greenlet

from result import Result
from tenacity import retry, stop_after_attempt, wait_exponential

from osprey.engine.executor.external_service_utils import ExternalService, KeyT, ValueT
from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import (
    EntityLabelMutationsResult,
    EntityLabelMutation,
    EntityLabels,
    ExtendedEntityLabelMutation,
)
from osprey.worker.lib.osprey_shared.logging import get_logger

logger = get_logger(__name__)

class LabelsServiceBase(ABC):
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

    def after_add(entity: EntityT[Any], label: str) -> None:
        """
        This method will be called once by the label output sink when a label is added to an entity. 

        If the LabelAdd was provided a delay_action_by param, this method will not be called until 
        after the specified timedelta.

        **Note:** If a rules worker needs to shut down due to a shutdown signal, the label output sink
        stop() method will execute all pending label adds regardless of their delay_action_by timedeltas.
        """
        pass

    def after_remove(entity: EntityT[Any], label: str) -> None:
        """
        This method will be called once by the label output sink when a label is removed from an entity. 

        If the LabelRemove was provided a delay_action_by param, this method will not be called until 
        after the specified timedelta.

        **Note:** If a rules worker needs to shut down due to a shutdown signal, the label output sink
        stop() method will execute all pending label adds regardless of their delay_action_by timedeltas.
        """
        pass


class LabelsProvider(ExternalService[EntityT[Any], EntityLabels]):

    def __init__(self, labels_service: LabelsServiceBase):
        self.labels_service = labels_service
        self._delayed_actions: set[Greenlet] = set()

    @retry(wait=wait_exponential(min=0.5, max=5), stop=stop_after_attempt(3))
    def apply_entity_label_mutations_with_retry(
        self, entity: EntityT[Any], mutations: Sequence[ExtendedEntityLabelMutation]
    ) -> EntityLabelMutationsResult:
        return self.apply_entity_label_mutations(
            entity=entity, mutations=mutations
        )

    def _get_mutations_by_label_name_and_drop_conflicts(self, mutations: Sequence[EntityLabelMutation]) -> tuple[dict[str, list[EntityLabelMutation]], list[EntityLabelMutation]]:
        """
        collect mutations based on the value of their status. this means if a higher status and a lower status label mutation
        occur in the same mutations request, the lower status one(s) will be discarded / dropped.
        """
        mutations_by_label_name: dict[str, list[EntityLabelMutation]] = defaultdict(list)
        for mutation in mutations:
            label_name = mutation.label_name
            if label_name in mutations_by_label_name:
                other_mutation = mutations_by_label_name[label_name][0]
                if mutation.status.value > other_mutation.status.value:
                    for mut in mutations_by_label_name[label_name]:
                        # we may have a list of more than one mutation if the statuses are all the same
                        dropped.append(mut)
                    mutations_by_label_name[label_name] = mutation
                    continue
                elif mutation.status.weight < other_mut.status.weight:
                    dropped.append(mutation)
                    continue
            # if the status weights are equal or if there is no previous statuses, append
            mutations_by_label_name[label_name].append(mutation)

    def apply_entity_label_mutations(
        self, entity: EntityT[Any], mutations: Sequence[EntityLabelMutation]
    ) -> EntityLabelMutationsResult:
        old_labels = self.get_from_service(entity)
        new_labels = copy.deepcopy(old_labels)

        added: list[str] = []
        removed: list[str] = []
        updated: list[str] = []
        dropped: list[EntityLabelMutation] = []

        (mutations_by_label_name, dropped_mutations) = self._get_mutations_by_label_name_and_drop_conflicts(mutations)

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
