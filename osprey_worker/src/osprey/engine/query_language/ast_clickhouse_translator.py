from typing import Any, Dict, List

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from osprey.engine.ast import grammar
from osprey.engine.ast_validator.validation_context import ValidatedSources
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes


class BaseAstTranslator:
    def __init__(self, validated_sources: ValidatedSources) -> None:
        self._validated_sources = validated_sources

        try:
            self._udf_node_mapping = validated_sources.get_validator_result(ValidateCallKwargs)
        except KeyError:
            self._udf_node_mapping = {}

        assign_node = validated_sources.sources.get_entry_point().ast_root.statements[0]
        assert isinstance(assign_node, grammar.Assign)
        self._root = assign_node.value


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

        self._ch_client: Client = clickhouse_connect.get_client(
            host='',
            port=8443,
            username='',
            password='',
        )

        # we'll use this to keep track of what paramter we are currently working on
        # and give it a unique name
        self._param_counter = 0
        self._params: List[Any] = []  # TODO: we might be able to type this better, idk yet

    def transform(self) -> Dict[str, Any]:
        sql = self._transform(self._root)
        return {'sql', sql, 'params', self._params}

    def _transform(self, node: grammar.ASTNode) -> str:
        method = 'transform_' + node.__class__.__name__
        transformer = getattr(self, method, None)

        if not transformer:
            raise ClickhouseTransformException(node, 'Unknown AST Expression')

        ret = transformer(node)
        assert isinstance

        return ret

    def _get_next_param_name(self) -> str:
        """Helper to return the next valid param name for the query"""
