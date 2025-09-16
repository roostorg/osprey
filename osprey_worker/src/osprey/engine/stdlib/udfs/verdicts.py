from osprey.engine.language_types.effects import EffectBase
from osprey.engine.language_types.verdicts import VerdictEffect

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class VerdictArguments(ArgumentsBase):
    verdict: str
    """The verdict value."""


class DeclareVerdict(UDFBase[VerdictArguments, EffectBase]):
    """Adds a verdict to the current action. This (and all declared verdicts) will be sent back to the
    synchronous caller if the action was processed synchronously~"""

    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: VerdictArguments) -> EffectBase:
        return VerdictEffect(verdict=arguments.verdict)
