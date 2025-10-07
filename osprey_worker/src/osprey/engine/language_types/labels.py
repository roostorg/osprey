from dataclasses import dataclass
from datetime import timedelta
from enum import IntEnum
from typing import Any, List, Optional, Self, cast

from osprey.engine.executor.custom_extracted_features import CustomExtractedFeature
from osprey.engine.language_types.effects import EffectToCustomExtractedFeatureBase
from osprey.engine.shared_constants import (
    ENTITY_LABEL_MUTATION_DIMENSION_NAME,
    ENTITY_LABEL_MUTATION_DIMENSION_VALUE,
)

from .entities import EntityT
from .rules import RuleT, add_slots


class LabelStatus(IntEnum):
    """
    indicates the status of label.

    regular (a.k.a. "automatic") statuses are applied via rules. they can be overwritten by manual
    statuses, which can only be applied via humans using the ui.

    statuses have weights, which control which ones get dropped when conflicting statuses occur during
    a single attempted mutation; i.e., if an execution of the rules results in a label add and a label remove
    of the same entity/label pair.
    """
    ADDED = 0
    REMOVED = 1
    MANUALLY_ADDED = 2
    MANUALLY_REMOVED = 3

    @property
    def weight(self) -> int:
        """
        a higher weight means that a given status will take precedence during merging operations. 
        so, if you have an added mutation and a removed mutation, and added is declared to weigh more
        via this method, then the added mutation will be sent to the label provider and the removed 
        mutation will be dropped.
        """
        match self:
            case LabelStatus.MANUALLY_ADDED:
                return 4
            case LabelStatus.MANUALLY_REMOVED:
                return 3
            case LabelStatus.ADDED:
                return 2
            case LabelStatus.REMOVED:
                return 1

    def effective_label_status(self) -> 'LabelStatus':
        """
        Returns the effective status of the label, which is what the upstreams that are observing label
        status changes will see. Which is to say, the upstreams will currently not see if the label status was
        manually added or manually removed, just that it was added or removed.
        """
        match self:
            case LabelStatus.ADDED | LabelStatus.MANUALLY_ADDED:
                return LabelStatus.ADDED
            case LabelStatus.REMOVED | LabelStatus.MANUALLY_REMOVED:
                return LabelStatus.REMOVED


@add_slots
@dataclass
class LabelEffect(EffectToCustomExtractedFeatureBase[List[str]]):
    """Stores a label effect of a WhenRules(...) invocation, which stores the label mutations that should occur once
    a given action has finished classification."""

    entity: EntityT[Any]
    """The entity that the effect will be applied on."""

    status: LabelStatus
    """The status of the label that will be applied by this effect."""

    name: str
    """The name of the label to apply."""

    expires_after: Optional[timedelta] = None
    """If set, the label effect has a timed expiration, which means that the reason will expire after this time."""

    delay_action_by: Optional[timedelta] = None
    """If set, the propagation of the effect to the upstream (if configured) will be delayed.S"""

    dependent_rule: Optional[RuleT] = None
    """If set, the effect will only be applied if the dependent rule evaluates to true."""

    suppressed: bool = False
    """If set to true, the effect should not be applied."""

    def to_str(self) -> str:
        return ENTITY_LABEL_MUTATION_DIMENSION_VALUE(self.entity.type, self.name, self.status)

    @classmethod
    def build_custom_extracted_feature_from_list(cls, values: List[Self]) -> CustomExtractedFeature[List[str]]:
        return LabelEffectsExtractedFeature(effects=cast(List[LabelEffect], values))


@add_slots
@dataclass
class LabelEffectsExtractedFeature(CustomExtractedFeature[List[str]]):
    effects: List[LabelEffect]

    @classmethod
    def feature_name(cls) -> str:
        return (
            ENTITY_LABEL_MUTATION_DIMENSION_NAME.lstrip('__')
            if ENTITY_LABEL_MUTATION_DIMENSION_NAME.startswith('__')
            else ENTITY_LABEL_MUTATION_DIMENSION_NAME
        )

    def get_serializable_feature(self) -> List[str] | None:
        return [effect.to_str() for effect in self.effects]
