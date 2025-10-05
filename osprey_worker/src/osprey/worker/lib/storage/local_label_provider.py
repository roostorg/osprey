from typing import Any, Dict, List, Sequence

from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import ApplyEntityMutationReply, EntityMutation, Labels
from osprey.worker.lib.storage.labels import BaseLabelProvider
from result import Result


class LocalLabelProvider(BaseLabelProvider):
    def __init__(self):
        self._labels: Dict[str, Labels] = {}

    def batch_get_from_service(self, keys: Sequence[EntityT[Any]]) -> Sequence[Result[Labels, Exception]]:
        raise NotImplementedError()

    def apply_entity_mutation(
        self, entity_key: EntityT[Any], mutations: List[EntityMutation]
    ) -> ApplyEntityMutationReply:
        raise NotImplementedError()

    def get_from_service(self, key: EntityT[Any]) -> Labels:
        raise NotImplementedError()
