from typing import Any, Dict, Mapping, Type

from osprey.engine.ast import grammar
from osprey.engine.ast_validator.validation_context import (
    ValidatedSources,
    ValidationError,
    ValidationFailed,
)

from . import parse_query_to_validated_ast

# `parse_query_to_validated_ast` prepends `Query = ` so a bare expression
# parses as a module-level assignment; subtract this from line-1 columns so
# reported spans match the caller's source.
_QUERY_PREFIX_LEN = len('Query = ')

_COMPARATOR_KINDS: Mapping[Type[grammar.Comparator], str] = {
    grammar.Equals: 'Equals',
    grammar.NotEquals: 'NotEquals',
    grammar.LessThan: 'LessThan',
    grammar.LessThanEquals: 'LessThanEquals',
    grammar.GreaterThan: 'GreaterThan',
    grammar.GreaterThanEquals: 'GreaterThanEquals',
    grammar.In: 'In',
    grammar.NotIn: 'NotIn',
}

_BOOLEAN_OPERAND_KINDS: Mapping[Type[grammar.BooleanOperand], str] = {
    grammar.And: 'And',
    grammar.Or: 'Or',
}

_UNARY_OPERATOR_KINDS: Mapping[Type[grammar.UnaryOperator], str] = {
    grammar.Not: 'Not',
    grammar.USub: 'USub',
}


def validate_query_filter_to_json(source: str, rules_sources: ValidatedSources) -> Dict[str, Any]:
    """Validate an SML query-filter source and return errors + AST as JSON.

    Returns a dict with:
        error: list of `{kind, message, hint, span}` - one entry per
        parse or validation failure. `kind` is 'syntax' for parse
        errors, 'validation' for semantic errors.

        ast: the serialized expression tree on success. Will return `None` when
        an error is encountered.
    """
    try:
        validated_sources = parse_query_to_validated_ast(source, rules_sources=rules_sources)
    except ValidationFailed as e:
        return {'errors': [_serialize_error(err) for err in e.errors], 'ast': None}

    return {'errors': [], 'ast': _unwrap_query_ast(validated_sources)}


def _serialize_span(span: grammar.Span) -> Dict[str, Any]:
    col = span.start_pos + 1
    if span.start_line == 1:
        col = max(1, col - _QUERY_PREFIX_LEN)
    return {'start': {'line': span.start_line, 'col': col}}


def _serialize_error(e: ValidationError) -> Dict[str, Any]:
    kind = 'syntax' if e.validator_class is None else 'validation'
    return {
        'kind': kind,
        'message': e.message,
        'hint': e.hint or None,
        'span': _serialize_span(e.span),
    }


def _serialize_expression(node: grammar.Expression) -> Dict[str, Any]:
    span = _serialize_span(node.span)

    if isinstance(node, grammar.String):
        return {'kind': 'String', 'value': node.value, 'span': span}
    if isinstance(node, grammar.Number):
        return {'kind': 'Number', 'value': node.value, 'span': span}
    if isinstance(node, grammar.Boolean):
        return {'kind': 'Boolean', 'value': node.value, 'span': span}
    if isinstance(node, grammar.None_):
        return {'kind': 'None', 'span': span}
    if isinstance(node, grammar.List):
        return {
            'kind': 'List',
            'items': [_serialize_expression(i) for i in node.items],
            'span': span,
        }
    if isinstance(node, grammar.Name):
        return {'kind': 'Name', 'identifier': node.identifier, 'span': span}
    if isinstance(node, grammar.Attribute):
        return {
            'kind': 'Attribute',
            'name': _serialize_expression(node.name),
            'attribute': node.attribute,
            'span': span,
        }
    if isinstance(node, grammar.BinaryComparison):
        return {
            'kind': 'BinaryComparison',
            'left': _serialize_expression(node.left),
            'right': _serialize_expression(node.right),
            'comparator': _COMPARATOR_KINDS[type(node.comparator)],
            'span': span,
        }
    if isinstance(node, grammar.BooleanOperation):
        return {
            'kind': 'BooleanOperation',
            'operand': _BOOLEAN_OPERAND_KINDS[type(node.operand)],
            'values': [_serialize_expression(v) for v in node.values],
            'span': span,
        }
    if isinstance(node, grammar.UnaryOperation):
        return {
            'kind': 'UnaryOperation',
            'operator': _UNARY_OPERATOR_KINDS[type(node.operator)],
            'operand': _serialize_expression(node.operand),
            'span': span,
        }
    if isinstance(node, grammar.Call):
        return {
            'kind': 'Call',
            'func': _serialize_expression(node.func),
            'arguments': [
                {
                    'kind': 'Keyword',
                    'name': arg.name,
                    'value': _serialize_expression(arg.value),
                    'span': _serialize_span(arg.span),
                }
                for arg in node.arguments
            ],
            'span': span,
        }

    raise NotImplementedError(f'Cannot serialize AST node type: {type(node).__name__}')


def _unwrap_query_ast(validated_sources: ValidatedSources) -> Dict[str, Any]:
    ast_root = validated_sources.sources.get_entry_point().ast_root
    assign = ast_root.statements[0]
    assert isinstance(assign, grammar.Assign)
    return {
        'kind': 'Root',
        'expression': _serialize_expression(assign.value),
        'span': _serialize_span(assign.value.span),
    }
