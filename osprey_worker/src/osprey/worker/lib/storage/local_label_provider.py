from typing import Any, List, Sequence, Dict

from result import Result, Ok, Err

from osprey.engine.language_types.entities import EntityT
from osprey.engine.language_types.labels import LabelStatus
from osprey.worker.lib.osprey_shared.labels import Labels, EntityMutation, ApplyEntityMutationReply, LabelState
from osprey.worker.lib.storage.labels import LabelProvider


class LocalLabelProvider(LabelProvider):
    def __init__(self):
        self._labels: Dict[str, Labels] = {}

    def get_from_service(self, key: EntityT[Any]) -> Labels:
        """Get labels for a single entity."""
        entity_key = f"{key.type}:{key.id}"
        return self._labels.get(entity_key, Labels())

    def batch_get_from_service(self, keys: Sequence[EntityT[Any]]) -> Sequence[Result[Labels, Exception]]:
        """Get labels for multiple entities."""
        results = []
        for key in keys:
            try:
                labels = self.get_from_service(key)
                results.append(Ok(labels))
            except Exception as e:
                results.append(Err(e))
        return results

    def apply_entity_mutation(
            self,
            entity_key: EntityT[Any],
            mutations: List[EntityMutation]
    ) -> ApplyEntityMutationReply:
        """Apply label mutations to an entity."""
        key = f"{entity_key.type}:{entity_key.id}"

        # Get or create labels for this entity
        if key not in self._labels:
            self._labels[key] = Labels()

        entity_labels = self._labels[key]

        # Track changes for the reply
        added = []
        removed = []
        unchanged = []

        for mutation in mutations:
            label_name = mutation.label_name

            # Get current label state
            current_state = entity_labels.labels.get(label_name)

            # Apply the mutation based on the status
            if mutation.status in [LabelStatus.ADDED, LabelStatus.MANUALLY_ADDED]:
                if current_state is None or current_state.status in ['REMOVED', 'MANUALLY_REMOVED']:
                    # Create new label state
                    new_state = LabelState(
                        status=mutation.status,
                        reasons={mutation.reason_name: mutation}
                    )
                    entity_labels.labels[label_name] = new_state
                    added.append(label_name)
                else:
                    # Label already added, just update reasons
                    current_state.reasons[mutation.reason_name] = mutation
                    unchanged.append(label_name)

            elif mutation.status in ['REMOVED', 'MANUALLY_REMOVED']:
                if current_state is not None and current_state.status in ['ADDED', 'MANUALLY_ADDED']:
                    # Update to removed status
                    current_state.status = mutation.status
                    current_state.reasons[mutation.reason_name] = mutation
                    removed.append(label_name)
                else:
                    unchanged.append(label_name)

        return ApplyEntityMutationReply(
            added=added,
            removed=removed,
            unchanged=unchanged
        )

    def get_labels_for_entity(self, entity_key: EntityT[str]) -> Labels:
        """Helper method to get labels for debugging/testing."""
        key = f"{entity_key.type}:{entity_key.id}"
        return self._labels.get(key, Labels())

    def clear_all_labels(self) -> None:
        """Helper method to clear all stored labels for testing."""
        self._labels.clear()