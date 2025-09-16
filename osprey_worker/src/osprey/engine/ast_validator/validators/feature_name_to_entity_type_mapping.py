from typing import Dict

from osprey.engine.ast import grammar
from osprey.engine.stdlib.udfs.entity import EntityArgumentsBase

from ..base_validator import HasResult, SourceValidator
from ..validation_context import ValidationContext
from .validate_call_kwargs import UDFNodeMapping, ValidateCallKwargs


class FeatureNameToEntityTypeMapping(SourceValidator, HasResult[Dict[str, str]]):
    def __init__(self, context: 'ValidationContext'):
        super().__init__(context)
        self._feature_name_to_entity_type: Dict[str, str] = {}
        self._udf_node_mapping: UDFNodeMapping = context.get_validator_result(ValidateCallKwargs)

    def validate_source(self, source: 'grammar.Source') -> None:
        for statement in source.ast_root.statements:
            if (
                isinstance(statement, grammar.Assign)
                and not statement.target.is_local
                and isinstance(statement.value, grammar.Call)
            ):
                _, args = self._udf_node_mapping[id(statement.value)]
                if isinstance(args, EntityArgumentsBase):
                    self._feature_name_to_entity_type[statement.target.identifier] = args.type.value

    def get_result(self) -> Dict[str, str]:
        return self._feature_name_to_entity_type
