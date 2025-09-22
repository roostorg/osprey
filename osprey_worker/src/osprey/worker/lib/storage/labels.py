from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any, List, Optional, Sequence

from osprey.engine.executor.external_service_utils import ExternalService
from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import ApplyEntityMutationReply, EntityMutation, Labels
from result import Result


class LabelProvider(ExternalService[EntityT[Any], Labels], ABC):
    def cache_ttl(self) -> Optional[timedelta]:
        return timedelta(minutes=5)

    @abstractmethod
    def get_from_service(self, key: EntityT[Any]) -> Labels:
        raise NotImplementedError()

    @abstractmethod
    def batch_get_from_service(self, keys: Sequence[EntityT[Any]]) -> Sequence[Result[Labels, Exception]]:
        raise NotImplementedError()

    @abstractmethod
    def apply_entity_mutation(
        self, entity_key: EntityT[str], mutations: List[EntityMutation]
    ) -> ApplyEntityMutationReply:
        raise NotImplementedError()
