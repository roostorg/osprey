from __future__ import annotations

import operator
from typing import Any, Callable, Dict

from osprey.engine.ast import grammar
from osprey.engine.ast_validator.validation_context import ValidatedSources
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.query_language.filter_ir import (
    BooleanFilter,
    BooleanOperator,
    ComparisonFilter,
    ComparisonOperator,
    ContainsFilter,
    DruidRawFilter,
    FeatureRef,
    FilterExpression,
    InFilter,
    LiteralValue,
    NotFilter,
)
from osprey.engine.udf.base import QueryUdfBase
from osprey.engine.utils.osprey_unary_executor import OspreyUnaryExecutor


class FilterIrTransformException(Exception):
    """Some error happened while trying to transform the Osprey AST into a backend-neutral filter."""

    def __init__(self, node: grammar.ASTNode, error: str):
        super().__init__(f'{error}: {node.__class__.__name__}')
        self.node = node


class FilterIrTransformer:
    """Given an Osprey AST node tree, transform it into a backend-neutral filter IR."""

    def __init__(self, validated_sources: ValidatedSources, allow_druid_udf_fallback: bool = False):
        self._allow_druid_udf_fallback = allow_druid_udf_fallback

        try:
            self._udf_node_mapping = validated_sources.get_validator_result(ValidateCallKwargs)
        except KeyError:
            self._udf_node_mapping = {}

        assign_node = validated_sources.sources.get_entry_point().ast_root.statements[0]
        assert isinstance(assign_node, grammar.Assign)
        self._root = assign_node.value

    def transform(self) -> FilterExpression:
        return self._transform(self._root)

    def _transform(self, node: grammar.ASTNode) -> FilterExpression:
        method = 'transform_' + node.__class__.__name__
        transformer = getattr(self, method, None)

        if not transformer:
            raise FilterIrTransformException(node, 'Unknown AST Expression')

        ret = transformer(node)
        return ret

    def transform_BooleanOperation(self, node: grammar.BooleanOperation) -> FilterExpression:
        assert isinstance(node.operand, grammar.And) or isinstance(node.operand, grammar.Or)

        operator_ = BooleanOperator.AND if isinstance(node.operand, grammar.And) else BooleanOperator.OR
        values = tuple(self._transform(v) for v in node.values)
        return BooleanFilter(operator=operator_, fields=values)

    def transform_BinaryComparison(self, node: grammar.BinaryComparison) -> FilterExpression:
        if (
            isinstance(node.left, grammar.Name)
            and not is_lowercase_literal_name(node.left)
            and isinstance(node.right, grammar.Name)
            and not is_lowercase_literal_name(node.right)
        ):
            if isinstance(node.comparator, grammar.Equals):
                operator_ = ComparisonOperator.EQUALS
            elif isinstance(node.comparator, grammar.NotEquals):
                operator_ = ComparisonOperator.NOT_EQUALS
            else:
                raise FilterIrTransformException(
                    node.comparator, 'When comparing two features, only the `==` and `!=` operators are supported'
                )
            return ComparisonFilter(
                left=FeatureRef(node.left.identifier), operator=operator_, right=FeatureRef(node.right.identifier)
            )

        dimension = get_comparison_dimension(node)
        value = get_comparison_value(node)

        if isinstance(node.comparator, grammar.In):
            if isinstance(value, str):
                return ContainsFilter(feature=FeatureRef(dimension), value=LiteralValue(value))
            elif isinstance(value, list):
                return InFilter(feature=FeatureRef(dimension), values=tuple(value))
            else:
                raise FilterIrTransformException(node, 'Invalid "in" comparison value type, must be string or list')
        elif isinstance(node.comparator, grammar.NotIn):
            if isinstance(value, str):
                return NotFilter(field=ContainsFilter(feature=FeatureRef(dimension), value=LiteralValue(value)))
            elif isinstance(value, list):
                return NotFilter(field=InFilter(feature=FeatureRef(dimension), values=tuple(value)))
            else:
                raise FilterIrTransformException(node, 'Invalid "in" comparison value type, must be string or list')

        if isinstance(node.left, grammar.Name):
            return ComparisonFilter(
                left=FeatureRef(dimension), operator=get_comparison_operator(node), right=LiteralValue(value)
            )
        elif isinstance(node.right, grammar.Name):
            return ComparisonFilter(
                left=LiteralValue(value), operator=get_comparison_operator(node), right=FeatureRef(dimension)
            )

        raise FilterIrTransformException(node, 'Binary Comparator must contain at least one column')

    def transform_UnaryOperation(self, node: grammar.UnaryOperation) -> FilterExpression:
        if isinstance(node.operator, grammar.Not):
            return NotFilter(field=self._transform(node.operand))
        else:
            raise FilterIrTransformException(node, 'Unknown Unary Operator')

    def transform_Call(self, node: grammar.Call) -> FilterExpression:
        udf, _ = self._udf_node_mapping[id(node)]

        if not isinstance(udf, QueryUdfBase):
            raise FilterIrTransformException(node, 'Unknown function call type')

        try:
            return udf.to_filter_ir()
        except NotImplementedError:
            if not self._allow_druid_udf_fallback:
                raise FilterIrTransformException(
                    node, f'UDF {udf.__class__.__name__} does not implement to_filter_ir()'
                )
            return DruidRawFilter(filter=udf.to_druid_query())


_BINARY_OPERATORS: Dict[type, Callable[[Any, Any], Any]] = {
    grammar.Add: operator.add,
    grammar.Subtract: operator.sub,
    grammar.Multiply: operator.mul,
    grammar.Divide: operator.truediv,
    grammar.FloorDivide: operator.floordiv,
    grammar.Modulo: operator.mod,
    grammar.Pow: operator.pow,
    grammar.LeftShift: operator.lshift,
    grammar.RightShift: operator.rshift,
    grammar.BitwiseOr: operator.or_,
    grammar.BitwiseAnd: operator.and_,
    grammar.BitwiseXor: operator.xor,
}


def get_comparison_operator(node: grammar.BinaryComparison) -> ComparisonOperator:
    if isinstance(node.comparator, grammar.Equals):
        return ComparisonOperator.EQUALS
    elif isinstance(node.comparator, grammar.NotEquals):
        return ComparisonOperator.NOT_EQUALS
    elif isinstance(node.comparator, grammar.LessThan):
        return ComparisonOperator.LESS_THAN
    elif isinstance(node.comparator, grammar.LessThanEquals):
        return ComparisonOperator.LESS_THAN_EQUALS
    elif isinstance(node.comparator, grammar.GreaterThan):
        return ComparisonOperator.GREATER_THAN
    elif isinstance(node.comparator, grammar.GreaterThanEquals):
        return ComparisonOperator.GREATER_THAN_EQUALS
    else:
        raise FilterIrTransformException(node.comparator, 'Unknown Binary Comparator')


def get_comparison_dimension(node: grammar.BinaryComparison) -> str:
    """Extracts the dimension name for a binary comparison."""

    if isinstance(node.left, grammar.Name) and not is_lowercase_literal_name(node.left):
        return node.left.identifier
    elif isinstance(node.right, grammar.Name) and not is_lowercase_literal_name(node.right):
        return node.right.identifier
    else:
        raise FilterIrTransformException(node, 'Binary Comparator must contain at least one column')


def get_comparison_value(node: grammar.BinaryComparison) -> Any:
    """Extracts the non-feature value for a binary comparison."""

    if isinstance(node.left, grammar.Name) and is_lowercase_literal_name(node.left):
        return get_lowercase_literal_name_value(node.left)
    elif isinstance(node.left, (grammar.Literal, grammar.UnaryOperation, grammar.BinaryOperation)):
        return get_ast_node_value(node.left)
    elif isinstance(node.right, grammar.Name) and is_lowercase_literal_name(node.right):
        return get_lowercase_literal_name_value(node.right)
    elif isinstance(node.right, (grammar.Literal, grammar.UnaryOperation, grammar.BinaryOperation)):
        return get_ast_node_value(node.right)


def evaluate_binary_operation(node: grammar.BinaryOperation) -> Any:
    """Evaluates a BinaryOperation node with constant values."""

    left_value = get_ast_node_value(node.left)
    right_value = get_ast_node_value(node.right)

    operator_func = _BINARY_OPERATORS.get(node.operator.__class__)
    if operator_func is None:
        raise FilterIrTransformException(node, f'Unsupported binary operator: {node.operator.__class__.__name__}')

    return operator_func(left_value, right_value)


def get_ast_node_value(node: grammar.ASTNode) -> Any:
    """Gets the relevant value from any given literal-like expression type."""

    if isinstance(node, grammar.UnaryOperation):
        return OspreyUnaryExecutor(node).get_execution_value()
    elif isinstance(node, grammar.BinaryOperation):
        return evaluate_binary_operation(node)
    elif isinstance(node, grammar.List):
        return [get_ast_node_value(i) for i in node.items]
    elif isinstance(node, grammar.Name) and is_lowercase_literal_name(node):
        return get_lowercase_literal_name_value(node)
    elif isinstance(node, grammar.None_):
        return None
    elif isinstance(node, grammar.String) or isinstance(node, grammar.Number) or isinstance(node, grammar.Boolean):
        return node.value
    else:
        raise FilterIrTransformException(node, 'Node has no known value attribute')


_LOWERCASE_LITERAL_NAME_VALUES = {
    'true': True,
    'false': False,
    'none': None,
}


def is_lowercase_literal_name(node: grammar.Name) -> bool:
    return node.identifier in _LOWERCASE_LITERAL_NAME_VALUES


def get_lowercase_literal_name_value(node: grammar.Name) -> Any:
    try:
        return _LOWERCASE_LITERAL_NAME_VALUES[node.identifier]
    except KeyError as exc:
        raise FilterIrTransformException(node, 'Unknown lowercase literal name') from exc
