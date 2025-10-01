from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any, List, Optional, Sequence

from osprey.engine.executor.external_service_utils import ExternalService
from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import ApplyEntityMutationReply, EntityMutation, Labels
from result import Result


class LabelProvider(ExternalService[EntityT[Any], Labels], ABC):
    """
    Abstract base class for entity labeling services that manage labels on entities
    (Users, Guilds, etc.) with support for automatic and manual label statuses.

    Implements a three-level hierarchical expiration system:
    1. Reason Expiration: Individual LabelReasons can have optional expires_at timestamps
    2. Label Expiration: Labels expire only when ALL their reasons are expired
    3. Entity Expiration: Entities expire when their latest-expiring label expires

    Key Features:
    - Priority-based conflict resolution (MANUALLY_ADDED > MANUALLY_REMOVED > ADDED > REMOVED)
    - Conservative expiration (entities persist until ALL reasons are definitively expired)
    - Manual label protection (manual labels resist automatic updates unless expired)
    """

    def cache_ttl(self) -> Optional[timedelta]:
        """
        Specifies cache TTL for label data. Default is 5 minutes.

        Returns:
            Optional[timedelta]: Cache duration, or None for no caching
        """
        return timedelta(minutes=5)

    @abstractmethod
    def get_from_service(self, key: EntityT[Any]) -> Labels:
        """
        Retrieves a single entity's current label state.

        Behavior:
        - No entity key validation (allows retrieval of "bad" entities for investigation)
        - Returns empty Labels if entity not found
        - Returns expired data (no automatic cleanup of expired labels)
        - Real-time expiration calculation: Labels.expires_at computed from all active label expirations
        - Read-only operation with no side effects

        Args:
            key: Entity identifier with type and id

        Returns:
            Labels: Contains labels dict (label_name -> LabelState) and computed expires_at

        Raises:
            Exception: For internal failures or invalid entity key format
        """
        raise NotImplementedError()

    @abstractmethod
    def batch_get_from_service(self, keys: Sequence[EntityT[Any]]) -> Sequence[Result[Labels, Exception]]:
        """
        Retrieves multiple entities' label states in a single batch operation.

        Implementation typically processes entity keys concurrently for performance.
        Each result is independent - failed retrievals don't affect successful ones.

        Behavior:
        - Concurrent processing of entity keys
        - Error isolation: individual failures wrapped in Err() results
        - Graceful degradation: partial batch success allowed
        - Consistent expiration semantics: each entity follows same rules as get_from_service

        Args:
            keys: Sequence of entity identifiers to retrieve

        Returns:
            Sequence[Result[Labels, Exception]]: Results for each requested entity,
            either Ok(Labels) for success or Err(Exception) for failures
        """
        raise NotImplementedError()

    @abstractmethod
    def apply_entity_mutation(
        self, entity_key: EntityT[Any], mutations: List[EntityMutation]
    ) -> ApplyEntityMutationReply:
        """
        Applies label mutations to an entity with sophisticated conflict resolution and priority handling.

        Core Logic (2 phases):
        1. Mutation Merging: Groups mutations by label, resolves conflicts using priority hierarchy
        2. Entity State Merging: Applies mutations to entity labels with expiration-aware logic

        Priority Hierarchy (highest to lowest):
        - MANUALLY_ADDED (priority 4) - Manually applied positive label
        - MANUALLY_REMOVED (priority 3) - Manually applied negative label
        - ADDED (priority 2) - Automatically applied positive label
        - REMOVED (priority 1) - Automatically applied negative label

        Expiration-Aware Behaviors:
        - Manual Protection: Manual labels resist automatic updates unless expired
        - Reason Updates: Expired reasons get replaced; non-expired get expiration extended
        - Entity Recomputation: expires_at recalculated after successful mutations

        Args:
            entity_key: Entity identifier with type and id
            mutations: List of EntityMutation objects to apply

        Returns:
            ApplyEntityMutationReply: Contains lists of added/removed/unchanged labels
            and any dropped mutations due to conflicts

        Raises:
            Exception: For entity key validation failures or transaction errors
        """
        raise NotImplementedError()
