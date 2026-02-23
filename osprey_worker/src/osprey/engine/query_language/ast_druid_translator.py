from typing import Any

from osprey.engine.ast import grammar
from osprey.engine.ast_validator.validation_context import ValidatedSources
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.udf.base import QueryUdfBase
from osprey.engine.utils.osprey_unary_executor import OspreyUnaryExecutor


class DruidQueryTransformException(Exception):
    """Some error happened while trying to transform the Osprey AST into a Druid Query"""

    def __init__(self, node: grammar.ASTNode, error: str):
        super().__init__(f'{error}: {node.__class__.__name__}')
        self.node = node


class DruidQueryTransformer:
    """Given a osprey_ast node tree, transform it into a Druid query"""

    def __init__(self, validated_sources: ValidatedSources):
        try:
            self._udf_node_mapping = validated_sources.get_validator_result(ValidateCallKwargs)
        except KeyError:
            self._udf_node_mapping = {}

        assign_node = validated_sources.sources.get_entry_point().ast_root.statements[0]
        assert isinstance(assign_node, grammar.Assign)
        self._root = assign_node.value

    def transform(self) -> dict[str, Any]:
        return {'filter': self._transform(self._root)}

    def _transform(self, node: grammar.ASTNode) -> dict[str, Any]:
        method = 'transform_' + node.__class__.__name__
        transformer = getattr(self, method, None)

        if not transformer:
            raise DruidQueryTransformException(node, 'Unknown AST Expression')

        ret = transformer(node)
        assert isinstance(ret, dict)
        return ret

    def transform_BooleanOperation(self, node: grammar.BooleanOperation) -> dict[str, Any]:
        assert isinstance(node.operand, grammar.And) or isinstance(node.operand, grammar.Or)

        filter_type = 'and' if isinstance(node.operand, grammar.And) else 'or'
        values = [self._transform(v) for v in node.values]
        return {'type': filter_type, 'fields': values}

    def transform_BinaryComparison(self, node: grammar.BinaryComparison) -> dict[str, Any]:
        if isinstance(node.left, grammar.Name) and isinstance(node.right, grammar.Name):
            column_comparison = {
                'type': 'columnComparison',
                'dimensions': [node.left.identifier, node.right.identifier],
            }
            if isinstance(node.comparator, grammar.Equals):
                return column_comparison
            elif isinstance(node.comparator, grammar.NotEquals):
                return {'type': 'not', 'field': column_comparison}
            else:
                raise DruidQueryTransformException(
                    node.comparator, 'When comparing two features, only the `==` and `!=` operators are supported'
                )

        dimension = get_comparison_dimension(node)
        value = get_comparison_value(node)

        if isinstance(node.comparator, grammar.Equals):
            return {'type': 'selector', 'dimension': dimension, 'value': value}
        elif isinstance(node.comparator, grammar.In):
            return get_in_query_by_value_type(node, dimension, value)
        elif isinstance(node.comparator, grammar.NotEquals):
            return {'type': 'not', 'field': {'type': 'selector', 'dimension': dimension, 'value': value}}
        elif isinstance(node.comparator, grammar.NotIn):
            return {'type': 'not', 'field': get_in_query_by_value_type(node, dimension, value)}

        bound_query = {
            'type': 'bound',
            'dimension': dimension,
            'ordering': get_value_bound_ordering(value),
            **get_druid_bound_query_props(node, value),
        }

        # greater than and less than queries require an explicit not null check
        return {
            'type': 'and',
            'fields': [
                {'type': 'not', 'field': {'type': 'selector', 'dimension': dimension, 'value': None}},
                bound_query,
            ],
        }

    def transform_UnaryOperation(self, node: grammar.UnaryOperation) -> dict[str, Any]:
        if isinstance(node.operator, grammar.Not):
            return {'type': 'not', 'field': self._transform(node.operand)}
        else:
            raise DruidQueryTransformException(node, 'Unknown Unary Operator')

    def transform_Call(self, node: grammar.Call) -> dict[str, Any]:
        udf, _ = self._udf_node_mapping[id(node)]

        if not isinstance(udf, QueryUdfBase):
            raise DruidQueryTransformException(node, 'Unknown function call type')

        return udf.to_druid_query()


def get_in_query_by_value_type(node: grammar.BinaryComparison, dimension: str, comparison_value: Any) -> dict[str, Any]:
    if isinstance(comparison_value, str):
        return {
            'type': 'search',
            'dimension': dimension,
            'query': {'type': 'insensitive_contains', 'value': comparison_value},
        }
    elif isinstance(comparison_value, list):
        return {'type': 'in', 'dimension': dimension, 'values': comparison_value}
    else:
        raise DruidQueryTransformException(node, 'Invalid "in" comparison value type, must be string or list')


def get_druid_bound_query_props(node: grammar.BinaryComparison, comparison_value: Any) -> dict[str, Any]:
    """Get the correct query properties for the various type of `bound` filters"""

    if isinstance(node.comparator, grammar.LessThan):
        return {'upper': comparison_value, 'upperStrict': True}
    elif isinstance(node.comparator, grammar.LessThanEquals):
        return {'upper': comparison_value}
    elif isinstance(node.comparator, grammar.GreaterThan):
        return {'lower': comparison_value, 'lowerStrict': True}
    elif isinstance(node.comparator, grammar.GreaterThanEquals):
        return {'lower': comparison_value}
    else:
        raise DruidQueryTransformException(node.comparator, 'Unknown Binary Comparator')


def get_comparison_dimension(node: grammar.BinaryComparison) -> str:
    """Extracts the dimension name for a binary comparison"""

    if isinstance(node.left, grammar.Name):
        return node.left.identifier
    elif isinstance(node.right, grammar.Name):
        return node.right.identifier
    else:
        raise DruidQueryTransformException(node, 'Binary Comparator must contain at least one column')


def get_comparison_value(node: grammar.BinaryComparison) -> Any:
    """Extracts the value for a binary comparison"""

    if isinstance(node.left, (grammar.Literal, grammar.UnaryOperation)):
        return get_ast_node_value(node.left)
    elif isinstance(node.right, (grammar.Literal, grammar.UnaryOperation)):
        return get_ast_node_value(node.right)


def get_ast_node_value(node: grammar.ASTNode) -> Any:
    """Gets the relevant value from any given expression type (Name or Literal)

    Unary operations can be evaluated into literals here (for negative Numbers)
    """

    if isinstance(node, grammar.UnaryOperation):
        return OspreyUnaryExecutor(node).get_execution_value()
    elif isinstance(node, grammar.List):
        return [get_ast_node_value(i) for i in node.items]
    elif isinstance(node, grammar.None_):
        return None
    elif isinstance(node, grammar.String) or isinstance(node, grammar.Number) or isinstance(node, grammar.Boolean):
        return node.value
    else:
        raise DruidQueryTransformException(node, 'Node has no known value attribute')


def get_value_bound_ordering(value: Any) -> str:
    """Given a value, return the appropriate comparator for the value to be used in a bound filter, throwing if it
    cannot be compared."""

    if isinstance(value, (int, float)):
        return 'numeric'
    elif isinstance(value, str):
        return 'lexicographic'

    raise TypeError(f'Cannot compare a {value.__class__.__name__}')
