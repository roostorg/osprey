from osprey.engine import shared_constants
from osprey.engine.ast_validator.validation_context import ValidationContext
from osprey.engine.query_language.udfs.array_filters import array_contains_filter
from osprey.engine.query_language.udfs.registry import register
from osprey.engine.udf.arguments import ArgumentsBase, ConstExpr
from osprey.engine.udf.base import QueryUdfBase


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

    def to_druid_query(self) -> dict[str, object]:
        return array_contains_filter(shared_constants.VERDICT_DIMENSION_NAME, self.verdict)
