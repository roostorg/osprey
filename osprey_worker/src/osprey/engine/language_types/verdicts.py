from dataclasses import dataclass
from typing import Self, cast

from osprey.engine.executor.custom_extracted_features import CustomExtractedFeature
from osprey.engine.language_types.effects import EffectToCustomExtractedFeatureBase
from osprey.engine.shared_constants import VERDICT_DIMENSION_NAME

from .rules import add_slots


@add_slots
@dataclass
class VerdictEffect(EffectToCustomExtractedFeatureBase[list[str]]):
    """Contains information about verdicts emitted during execution that will be sent
    back to synchronous callers of Osprey if the action was processed synchronously"""

    verdict: str
    """The verdict that was declared."""

    def to_str(self) -> str:
        return self.verdict

    @classmethod
    def build_custom_extracted_feature_from_list(cls, values: list[Self]) -> CustomExtractedFeature[list[str]]:
        return VerdictEffectsExtractedFeature(effects=cast(list[VerdictEffect], values))


@add_slots
@dataclass
class VerdictEffectsExtractedFeature(CustomExtractedFeature[list[str]]):
    effects: list[VerdictEffect]

    @classmethod
    def feature_name(cls) -> str:
        return (
            VERDICT_DIMENSION_NAME.lstrip('__') if VERDICT_DIMENSION_NAME.startswith('__') else VERDICT_DIMENSION_NAME
        )

    def get_serializable_feature(self) -> list[str] | None:
        return [effect.to_str() for effect in self.effects]
