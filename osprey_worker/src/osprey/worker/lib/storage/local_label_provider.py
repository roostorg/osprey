from typing import Any, Dict, List, Sequence

from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import EntityLabelMutationsResult, EntityLabelMutation, EntityLabels
from osprey.worker.lib.storage.labels import LabelsProvider
from result import Result


class LocalLabelProvider(LabelsProvider):
    def __init__(self):
        self._labels: Dict[str, EntityLabels] = {}

    def batch_get_from_service(self, keys: Sequence[EntityT[Any]]) -> Sequence[Result[EntityLabels, Exception]]:
        raise NotImplementedError()

    def apply_entity_mutation(
        self, entity_key: EntityT[Any], mutations: List[EntityLabelMutation]
    ) -> EntityLabelMutationsResult:
        raise NotImplementedError()

    def get_from_service(self, key: EntityT[Any]) -> EntityLabels:
        raise NotImplementedError()
