import re

from osprey.engine.ast import grammar
from osprey.engine.ast_validator.validation_context import ValidationContext
from osprey.engine.query_language.udfs.registry import register
from osprey.engine.udf.arguments import ArgumentsBase, ConstExpr
from osprey.engine.udf.base import QueryUdfBase


class Arguments(ArgumentsBase):
    item: str
    regex: ConstExpr[str]


@register
class RegexMatch(QueryUdfBase[Arguments, bool]):
    """
    Checks whether a given column matches a regular expression.

    # Examples

    `RegexMatch(item=UserName, regex='^jake')`
    """

    def __init__(self, validation_context: ValidationContext, arguments: Arguments):
        super().__init__(validation_context, arguments)
        regex = arguments.regex
        with regex.attribute_errors():
            re.compile(regex.value)
            self.regex = regex.value

        item_node = arguments.get_argument_ast('item')
        if isinstance(item_node, grammar.Name):
            self.item = item_node.identifier
        else:
            self.item = ''
            validation_context.add_error(
                message='expected variable', span=item_node.span, hint='argument `item` must be a variable'
            )

    def to_druid_query(self) -> dict[str, object]:
        return {'type': 'regex', 'dimension': self.item, 'pattern': self.regex}
