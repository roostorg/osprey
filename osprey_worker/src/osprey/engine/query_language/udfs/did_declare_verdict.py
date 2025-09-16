from typing import Dict

from osprey.engine.ast_validator.validation_context import ValidationContext
from osprey.engine.udf.arguments import ArgumentsBase, ConstExpr
from osprey.engine.udf.base import QueryUdfBase

from ... import shared_constants
from .registry import register


class Arguments(ArgumentsBase):
    verdict: ConstExpr[str]


@register
class DidDeclareVerdict(QueryUdfBase[Arguments, bool]):
    """
    Filters for actions that declared a verdict

    # Examples

    `DidDeclareVerdict(verdict='reject')`
    """

    def __init__(self, validation_context: ValidationContext, arguments: Arguments):
        super().__init__(validation_context, arguments)
        self.verdict = arguments.verdict.value

    def to_druid_query(self) -> Dict[str, object]:
        return {
            'type': 'arrayContainsElement',
            'column': shared_constants.VERDICT_DIMENSION_NAME,
            'elementMatchType': 'STRING',
            'elementMatchValue': self.verdict,
        }
