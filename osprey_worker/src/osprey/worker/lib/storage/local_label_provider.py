from datetime import datetime
from typing import Any, Dict, List, Sequence

from result import Result, Ok, Err

from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import EntityMutation, ApplyEntityMutationReply, Labels, LabelState, \
    LabelReason
from osprey.worker.lib.storage.labels import LabelProvider


class LocalLabelProvider(LabelProvider):
    """
    Local in-memory implementation of LabelProvider using the new label functions.

    This will only work for local testing and as an example of how to implement a LabelProvider.

    Implements sophisticated label management including:
    - Three-level hierarchical expiration system
    - Priority-based conflict resolution using EntityMutation.merge
    - Manual label protection using LabelState.apply
    - Expiration-aware state merging
    """

    def __init__(self):
        self._labels: Dict[str, Labels] = {}

    def get_from_service(self, key: EntityT[Any]) -> Labels:
        """
        Retrieves a single entity's current label state with real-time expiration calculation.
        """
        entity_key = f"{key.type}:{key.id}"
        entity_labels = self._labels.get(entity_key, Labels())

        # Real-time expiration calculation for entity
        entity_labels.expires_at = entity_labels.compute_expiration()

        return entity_labels

    def batch_get_from_service(self, keys: Sequence[EntityT[Any]]) -> Sequence[Result[Labels, Exception]]:
        """
        Batch retrieval with error isolation and concurrent processing simulation.
        """
        results = []
        for key in keys:
            try:
                labels = self.get_from_service(key)
                results.append(Ok(labels))
            except Exception as e:
                results.append(Err(e))
        return results

    def apply_entity_mutation(self, entity_key: EntityT[Any],
                            mutations: List[EntityMutation]) -> ApplyEntityMutationReply:
        """
        Applies label mutations using the new label functions.

        Implements the two-phase process:
        1. Mutation Merging and Conflict Resolution (using EntityMutation.merge)
        2. Entity State Merging with Expiration-Aware Logic (using LabelState.apply)
        """
        key = f"{entity_key.type}:{entity_key.id}"

        # Get or create labels for this entity
        if key not in self._labels:
            self._labels[key] = Labels()

        entity_labels = self._labels[key]

        label_mutations_to_apply = self._merge_mutations_by_label(mutations)

        # Phase 2: Entity State Merging
        added = []
        removed = []

        for label_name, merged_mutation in label_mutations_to_apply.items():
            current_state = entity_labels.labels.get(label_name)

            if current_state is None:
                # No existing state - create new label state
                new_state = self._create_new_label_state(merged_mutation)
                entity_labels.labels[label_name] = new_state

                if merged_mutation.status.effective_label_status().name in ['ADDED']:
                    added.append(label_name)
                else:
                    removed.append(label_name)
            else:
                # Existing state - apply sophisticated merging logic
                updated_state = current_state.apply(merged_mutation)

                if updated_state is not None:
                    # Changes were applied
                    entity_labels.labels[label_name] = updated_state

                    if merged_mutation.status.effective_label_status().name in ['ADDED']:
                        added.append(label_name)
                    else:
                        removed.append(label_name)
                # If updated_state is None, no changes were made (e.g., manual protection)

        # Recompute entity expiration after successful mutations
        entity_labels.expires_at = entity_labels.compute_expiration()

        return ApplyEntityMutationReply(
            added=added,
            removed=removed
        )

    def _merge_mutations_by_label(self, mutations: List[EntityMutation]) -> Dict[str, EntityMutation]:
        """
        Phase 1: Groups mutations by label name and resolves conflicts using EntityMutation.merge.
        """
        # Group mutations by label name
        grouped_mutations: Dict[str, List[EntityMutation]] = {}
        for mutation in mutations:
            label_name = mutation.label_name
            if label_name not in grouped_mutations:
                grouped_mutations[label_name] = []
            grouped_mutations[label_name].append(mutation)

        # Merge mutations for each label (handles single mutation case automatically)
        return {
            label_name: EntityMutation.merge(label_mutations)
            for label_name, label_mutations in grouped_mutations.items()
        }

    def _create_new_label_state(self, mutation: EntityMutation) -> LabelState:
        """
        Creates a new LabelState from an EntityMutation.
        """

        new_reason = LabelReason(
            pending=mutation.pending,
            description=mutation.description,
            features=mutation.features.copy(),
            created_at=datetime.now(),
            expires_at=mutation.expires_at
        )

        return LabelState(
            status=mutation.status,
            reasons={mutation.reason_name: new_reason}
        )


    # Helper methods for testing and debugging
    def get_labels_for_entity(self, entity_key: EntityT[str]) -> Labels:
        """Helper method to get labels for debugging/testing."""
        return self.get_from_service(entity_key)

    def clear_all_labels(self) -> None:
        """Helper method to clear all stored labels for testing."""
        self._labels.clear()