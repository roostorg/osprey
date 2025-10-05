from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any, Dict, List, Optional, Sequence

from osprey.engine.executor.execution_context import ExtendedEntityMutation
from osprey.engine.executor.external_service_utils import ExternalService
from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import ApplyEntityMutationReply, EntityMutation, Labels
from osprey.worker.lib.osprey_shared.logging import get_logger
from result import Result
from tenacity import retry, stop_after_attempt, wait_exponential


logger = get_logger(__name__)


class BaseLabelProvider(ExternalService[EntityT[Any], Labels], ABC):

    @retry(wait=wait_exponential(min=0.5, max=5), stop=stop_after_attempt(3))
    def apply_entity_mutation_with_retry(
        self, entity: EntityT, mutations: Sequence[ExtendedEntityMutation]
    ) -> ApplyEntityMutationReply:
        return self.apply_entity_mutation(
            entity_key=entity, mutations=[extended_mutation.mutation for extended_mutation in mutations]
        )

    def apply_label_mutations(
        self,
        entity: EntityT,
        mutations: Sequence[ExtendedEntityMutation],
    ) -> ApplyEntityMutationReply:
        if not entity.id:
            metrics.increment(
                'output_sink.apply_entity_mutation',
                tags=['status:skipped', 'reason:no_entity_id', f'entity_type:{entity.type}'],
            )
            return ApplyEntityMutationReply(
                unchanged=[mutation.mutation.label_name for mutation in mutations],
            )

        try:
            result: ApplyEntityMutationReply = self.apply_entity_mutation_with_retry(entity, mutations)
            metrics.increment('output_sink.apply_entity_mutation', tags=['status:success'])
        except Exception as e:
            logger.error(
                f'Failed to apply entity mutation on entity of type: {entity.type} with id: {entity.id} - {e}',
                exc_info=True,
            )
            metrics.increment('output_sink.apply_entity_mutation', tags=['status:failure'])
            raise e

        return result

    def cache_ttl(self) -> Optional[timedelta]:
        return timedelta(minutes=1)

    @abstractmethod
    def get_from_service(self, key: EntityT[Any]) -> Labels:
        """
        A standard read from the label service. Keep in mind that if there is a cache_ttl greater than 0 seconds,
        this method will not be called for every single label read. Therefore, if you want downstream effects to 
        occur for all read operations, you should consider disabling the cache_ttl (set it to a timedelta(days=-1)).

        note that timedeltas do accept negative values to represent the past, but only on the days field. you *can*
        use timedelta(seconds=0), but a negative time delta *ensures* that even if a time shift occurs (daylight 
        savings, perhaps), the cache_ttl will still always be immediate.
        """
        raise NotImplementedError()

    @abstractmethod
    def batch_get_from_service(self, keys: Sequence[EntityT[Any]]) -> Sequence[Result[Labels, Exception]]:
        """
        Batching can optimize the number of RPCs that are sent out during executions,
        which has been observed to provide noticeable performance benefits in a python/gevent world.

        If your label service does not enable batch requests, you can simply for-loop calls to get_from_service.
        """
        raise NotImplementedError()

    @abstractmethod
    def apply_entity_mutation(
        self, entity_key: EntityT[Any], mutations: List[EntityMutation]
    ) -> ApplyEntityMutationReply:
        """
        Effectively a write_to_service for labels service in specific. If you wish to have
        downstream effects for label mutations, you'll want to add those here.

        Note that this method will likely be called with retries, so be sure to factor in
        possible repeated effects.
        """
        raise NotImplementedError()


