from __future__ import annotations

from typing import Any, Dict

from osprey.engine.ast import grammar
from osprey.engine.ast_validator.validation_context import ValidatedSources
from osprey.engine.query_language.ast_filter_ir_translator import (
    FilterIrTransformer,
    FilterIrTransformException,
)
from osprey.engine.query_language.druid_filter_translator import DruidFilterTranslator


class DruidQueryTransformException(Exception):
    """Some error happened while trying to transform the Osprey AST into a Druid Query."""

    def __init__(self, node: grammar.ASTNode, error: str):
        super().__init__(f'{error}: {node.__class__.__name__}')
        self.node = node


class DruidQueryTransformer:
    """Given an Osprey AST node tree, transform it into a Druid query."""

    def __init__(self, validated_sources: ValidatedSources):
        self._validated_sources = validated_sources

    def transform(self) -> Dict[str, Any]:
        try:
            filter_ir = FilterIrTransformer(
                validated_sources=self._validated_sources, allow_druid_udf_fallback=True
            ).transform()
        except FilterIrTransformException as e:
            raise DruidQueryTransformException(e.node, str(e)) from e

        return {'filter': DruidFilterTranslator().transform(filter_ir)}
