from typing import TYPE_CHECKING

from osprey.engine.ast import grammar
from osprey.engine.ast.ast_utils import filter_nodes, iter_nodes
from osprey.engine.ast.error_utils import SpanWithHint
from osprey.engine.ast.grammar import BinaryComparison, BooleanOperation, Call, Not, UnaryOperation
from osprey.engine.ast_validator.base_validator import SourceValidator
from osprey.engine.ast_validator.validator_registry import ValidatorRegistry
from osprey.engine.ast_validator.validators import register_standard_rule_validators

if TYPE_CHECKING:
    from osprey.engine.ast_validator.validation_context import ValidationContext


# Ensure standard rule validators are available before cloning for queries
register_standard_rule_validators()

# Make a copy of the validator registry so we can add extra query-only validators and remove validators that
# shouldn't run on queries.
REGISTRY = ValidatorRegistry.from_validator_classes(
    {
        validator
        for validator in ValidatorRegistry.get_instance().get_validators()
        if not validator.exclude_from_query_validation
    }
)


ALLOWED_NODES = (
    grammar.Annotation,
    grammar.AnnotationWithVariants,
    grammar.BinaryComparison,
    grammar.BooleanOperand,
    grammar.BooleanOperation,
    grammar.Call,
    grammar.Comparator,
    grammar.Keyword,
    grammar.Literal,
    grammar.Name,
    grammar.UnaryOperation,
    grammar.UnaryOperator,
)


@REGISTRY.register
class OnlyQueryAstNodes(SourceValidator):
    """
    Validates that stored names across all source files are unique.
    """

    def __init__(self, context: 'ValidationContext'):
        super().__init__(context)

    def validate_source(self, source: 'grammar.Source') -> None:
        if not source.ast_root.statements:
            return

        assign_node = source.ast_root.statements[0]
        assert isinstance(assign_node, grammar.Assign)

        for node in iter_nodes(assign_node.value):
            if not isinstance(node, ALLOWED_NODES):
                span = node.span
                if span is None and isinstance(node.parent, grammar.ASTNode):
                    span = node.parent.span
                self.context.add_error(message=f'unexpected node type: `{node.__class__.__name__}`', span=span)


@REGISTRY.register
class OnlyOneStatementInAstRoot(SourceValidator):
    """
    Validates that there is only a single statement in a osprey query
    """

    def __init__(self, context: 'ValidationContext'):
        super().__init__(context)

    def validate_source(self, source: 'grammar.Source') -> None:
        for node in source.ast_root.statements[1:]:
            self.context.add_error(
                message='unexpected Node',
                span=node.span,
                hint='you cannot define additional statements within the query',
            )


@REGISTRY.register
class BinaryComparisonMustNotBeAllLiteral(SourceValidator):
    def validate_source(self, source: 'grammar.Source') -> None:
        for node in filter_nodes(source.ast_root, grammar.BinaryComparison):
            if isinstance(node.left, grammar.Literal) and isinstance(node.right, grammar.Literal):
                self.context.add_error(
                    message='cannot compare a literal to another literal',
                    span=node.span,
                    hint=(
                        f'one side of the `{node.comparator.original_comparator}` '
                        'comparison must be a non-literal (non-constant) value'
                    ),
                    additional_spans=[
                        SpanWithHint(span=node.left.span, hint='this is a literal value'),
                        SpanWithHint(span=node.right.span, hint='this is a literal value'),
                    ],
                )


@REGISTRY.register
class BooleanOperationMustNotBeAllLiteral(SourceValidator):
    def validate_source(self, source: 'grammar.Source') -> None:
        for node in filter_nodes(source.ast_root, grammar.BooleanOperation):
            if all([isinstance(value, grammar.Literal) for value in node.values]):
                self.context.add_error(
                    message='one value must be a literal',
                    span=node.span,
                    hint=(
                        f'one side of the `{node.operand.original_operand}` must be a non-literal (non-constant) value'
                    ),
                    additional_spans=[SpanWithHint(span=n.span, hint='this is a literal value') for n in node.values],
                )


@REGISTRY.register
class UnaryNotOperationMustNotBeLiteral(SourceValidator):
    def validate_source(self, source: 'grammar.Source') -> None:
        for node in filter_nodes(source.ast_root, grammar.UnaryOperation):
            if isinstance(node.operand, grammar.Literal) and isinstance(node.operator, Not):
                self.context.add_error(
                    message='the operand must not be a literal',
                    span=node.span,
                    hint=(f'the right side of the `{node.operand}` must be a non-literal (non-constant) value'),
                    additional_spans=[SpanWithHint(span=node.operand.span, hint='this is a literal value')],
                )


@REGISTRY.register
class QueryRootIsBooleanOperationBinaryComparisonOrCall(SourceValidator):
    def validate_source(self, source: 'grammar.Source') -> None:
        assign_node = source.ast_root.statements[0]
        assert isinstance(assign_node, grammar.Assign)
        if not isinstance(assign_node.value, (BooleanOperation, BinaryComparison, Call, UnaryOperation)):
            self.context.add_error(
                message='the query must be a `BooleanOperation`, `BinaryComparison`, `UnaryOperation`, or a `Call`',
                span=assign_node.value.span,
                hint=f'you have provided a `{assign_node.value.__class__.__name__}`, try something like `ActionName '
                '!= True`',
            )
