from abc import ABC, abstractmethod
from typing import Any, Dict, get_origin

from osprey.engine.ast import grammar
from osprey.engine.ast_validator.validation_context import ValidatedSources
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes
from osprey.engine.query_language.ast_druid_translator import get_comparison_dimension, get_comparison_value
from osprey.worker.ui_api.osprey.singletons import CLICKHOUSE


class BaseAstTranslator(ABC):
    def __init__(self, validated_sources: ValidatedSources) -> None:
        self._validated_sources = validated_sources

        try:
            self._udf_node_mapping = validated_sources.get_validator_result(ValidateCallKwargs)
        except KeyError:
            self._udf_node_mapping = {}

        assign_node = validated_sources.sources.get_entry_point().ast_root.statements[0]
        assert isinstance(assign_node, grammar.Assign)
        self._root = assign_node.value

    @abstractmethod
    def transform_BooleanOperation(self, node: grammar.BooleanOperation) -> Any:
        pass

    @abstractmethod
    def transform_BinaryComparison(self, node: grammar.BinaryComparison) -> Any:
        pass

    @abstractmethod
    def transform_UnaryOperation(self, node: grammar.UnaryOperation) -> Any:
        pass

    @abstractmethod
    def transform_Call(self, node: grammar.Call) -> Any:
        pass


class ClickhouseTransformException(Exception):
    """Some error happened while trying to transform the Osprey AST into a ClickHouse query"""

    def __init__(self, node: grammar.ASTNode, error: str) -> None:
        super().__init__(f'{error}: {node.__class__.__name__}')


class ClickhouseTranslator(BaseAstTranslator):
    """Given an osprey_ast node tree, transform it into a Clickhouse WHERE clause"""

    def __init__(self, validated_sources: ValidatedSources) -> None:
        super().__init__(validated_sources)

        try:
            static_types_result = validated_sources.get_validator_result(ValidateStaticTypes)
            self._name_types = static_types_result.name_type_and_span_cache
        except KeyError:
            self._name_types = {}

        self._client = CLICKHOUSE.instance().client

        # we'll use this to keep track of what paramter we are currently working on
        # and give it a unique name
        self._param_counter = 0
        self._params: Dict[str, Any] = {}  # TODO: we might be able to type this better, idk yet

    def transform(self) -> Dict[str, Any]:
        sql = self._transform(self._root)
        return {'sql': sql, 'params': self._params}

    def _transform(self, node: grammar.ASTNode) -> str:
        method = 'transform_' + node.__class__.__name__
        transformer = getattr(self, method, None)

        if not transformer:
            raise ClickhouseTransformException(node, 'Unknown AST Expression')

        ret = transformer(node)
        assert isinstance(ret, str)

        return ret

    def _get_next_param_name(self) -> str:
        """Helper to return the next valid param name for the query"""
        name = f'param_{self._param_counter}'
        self._param_counter += 1
        return name

    def _add_param(self, val: Any) -> str:
        name = self._get_next_param_name()
        self._params[name] = val

        if isinstance(val, str):
            type_annotation = 'String'
        elif isinstance(val, int):
            type_annotation = 'Int64'
        elif isinstance(val, float):
            type_annotation = 'Float64'
        elif isinstance(val, list):
            type_annotation = 'Array(String)'
        else:
            type_annotation = 'String'

        return f'{{{name}: {type_annotation}}}'

    def transform_BooleanOperation(self, node: grammar.BooleanOperation) -> str:
        assert isinstance(node.operand, grammar.And) or isinstance(node.operand, grammar.Or)
        operator = 'AND' if isinstance(node.operand, grammar.And) else 'OR'
        conds = [self._transform(v) for v in node.values]

        return f'({f" {operator} ".join(conds)})'

    def transform_BinaryComparison(self, node: grammar.BinaryComparison) -> str:
        if isinstance(node.left, grammar.Name) and isinstance(node.right, grammar.Name):
            left_col = node.left.identifier
            right_col = node.right.identifier

            if isinstance(node.comparator, grammar.Equals):
                return f'{left_col} = {right_col}'
            elif isinstance(node.comparator, grammar.NotEquals):
                return f'{left_col} != {right_col}'
            else:
                raise ClickhouseTransformException(
                    node.comparator, 'When comparing two features, only the `==` and `!=` operators are supported'
                )

        dim = get_comparison_dimension(node)
        val = get_comparison_value(node)

        if val is None:
            if isinstance(node.comparator, grammar.Equals):
                return f'{dim} IS NULL'
            elif isinstance(node.comparator, grammar.NotEquals):
                return f'{dim} IS NOT NULL'
            else:
                raise ClickhouseTransformException(node, 'NULL comparisons only support `==` and `!=` operators')

        # Username == 'Kitten'
        if isinstance(node.comparator, grammar.Equals):
            param = self._add_param(val)
            return f'{dim} = {param}'
        # Username != 'Bunny'
        elif isinstance(node.comparator, grammar.NotEquals):
            param = self._add_param(val)
            return f'{dim} != {param}'
        elif isinstance(node.comparator, grammar.In):
            return self._transform_in_comparison(node, dim, val)
        elif isinstance(node.comparator, grammar.NotIn):
            in_clause = self._transform_in_comparison(node, dim, val)
            return f'NOT ({in_clause})'
        # NumEars < 2
        elif isinstance(node.comparator, grammar.LessThan):
            param = self._add_param(val)
            return f'{dim} < {param}'
        # NumEyes <= 2
        elif isinstance(node.comparator, grammar.LessThanEquals):
            param = self._add_param(val)
            return f'{dim} <= {param}'
        # NumPaws > 4
        elif isinstance(node.comparator, grammar.GreaterThan):
            param = self._add_param(val)
            return f'{dim} > {param}'
        # NumTails >= 1
        elif isinstance(node.comparator, grammar.GreaterThanEquals):
            param = self._add_param(val)
            return f'{dim} >= {param}'
        else:
            raise ClickhouseTransformException(node.comparator, 'Unknown Binary Comparator')

    def _is_array_column(self, column_name: str) -> bool:
        """Check if a column is a list/array type based on static type information. Needed for some CH queries."""
        if column_name not in self._name_types:
            return False

        col_type = self._name_types[column_name].type
        origin = get_origin(col_type)
        return origin is list

    def _transform_in_comparison(self, node: grammar.BinaryComparison, dim: str, val: Any) -> str:
        """Transform queries like 'Mariners' in PostText to a where query"""
        if isinstance(val, str):
            if self._is_array_column(dim):
                param = self._add_param(val)
                return f'has({dim}, {param})'
            else:
                param = self._add_param(f'%{val}%')
                # TODO: do we want this to be case-insensitive?
                return f'LOWER({dim}) LIKE LOWER({param})'
        elif isinstance(val, list):
            # if the guy is empty, then it is obviously false
            if not val:
                return 'FALSE'

            param_name = self._get_next_param_name()

            self._params[param_name] = val
            return f'{dim} IN {{{param_name}}}'
        else:
            raise ClickhouseTransformException(node, 'Invalid "IN" comparison value type, must be string or list')

    def transform_UnaryOperation(self, node: grammar.UnaryOperation) -> str:
        """Trnsform unary operations into a SQL where"""
        if isinstance(node.operator, grammar.Not):
            operand_sql = self._transform(node.operand)
            return f'NOT ({operand_sql})'
        else:
            raise ClickhouseTransformException(node, 'Unknown Unary Operator')

    # TODO: actually implement this
    def transform_Call(self, node: grammar.Call) -> str:
        """Transform various function calls into SQL where. Requires UDFs implement to_clickhouse_query()"""

        raise ClickhouseTransformException(node, 'Unimplemented Call')
