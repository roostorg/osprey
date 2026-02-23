from dataclasses import dataclass
from typing import Self, cast

from osprey.engine.executor.custom_extracted_features import CustomExtractedFeature
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.language_types.effects import EffectBase, EffectToCustomExtractedFeatureBase
from osprey.engine.stdlib.udfs.categories import UdfCategories
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.engine.utils.types import add_slots


class BanUserArguments(ArgumentsBase):
    entity: str
    comment: str


@dataclass
class BanUserEffect(EffectToCustomExtractedFeatureBase[list[str]]):
    """Adds a 'ban user' effect to the action."""

    entity: str
    """The entity that the ban will be applied to."""

    comment: str
    """The comment that will be included with the ban, for internal purposes."""

    def to_str(self) -> str:
        return f'{self.entity}|{self.comment}'

    @classmethod
    def build_custom_extracted_feature_from_list(cls, values: list[Self]) -> CustomExtractedFeature[list[str]]:
        return BanEffectsExtractedFeature(effects=cast(list[BanUserEffect], values))


@add_slots
@dataclass
class BanEffectsExtractedFeature(CustomExtractedFeature[list[str]]):
    effects: list[BanUserEffect]

    @classmethod
    def feature_name(cls) -> str:
        return 'ban_user'

    def get_serializable_feature(self) -> list[str] | None:
        return [effect.to_str() for effect in self.effects]


def synthesize_effect(arguments: BanUserArguments) -> BanUserEffect:
    return BanUserEffect(
        entity=arguments.entity,
        comment=arguments.comment,
    )


class BanUser(UDFBase[BanUserArguments, EffectBase]):
    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: BanUserArguments) -> EffectBase:
        return synthesize_effect(arguments)
