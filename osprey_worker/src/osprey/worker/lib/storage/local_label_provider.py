from collections.abc import Sequence
from typing import Any

from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import EntityLabelMutation, EntityLabelMutationsResult, EntityLabels
from osprey.worker.lib.storage.labels import LabelsProvider
from result import Result


class LocalLabelProvider(LabelsProvider):
    def __init__(self):
        self._labels: dict[str, EntityLabels] = {}

    def batch_get_from_service(self, keys: Sequence[EntityT[Any]]) -> Sequence[Result[EntityLabels, Exception]]:
        raise NotImplementedError()

    def apply_entity_mutation(
        self, entity_key: EntityT[Any], mutations: list[EntityLabelMutation]
    ) -> EntityLabelMutationsResult:
        raise NotImplementedError()

    def get_from_service(self, key: EntityT[Any]) -> EntityLabels:
        raise NotImplementedError()
