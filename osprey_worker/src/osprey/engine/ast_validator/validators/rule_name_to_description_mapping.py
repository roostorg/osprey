from osprey.engine.ast.grammar import Assign, Call, FormatString, Source, String

from ..base_validator import HasResult, SourceValidator
from ..validation_context import ValidationContext


class RuleNameToDescriptionMapping(SourceValidator, HasResult[dict[str, str]]):
    def __init__(self, context: 'ValidationContext'):
        super().__init__(context)
        self._rule_to_info_mapping: dict[str, str] = {}

    def validate_source(self, source: 'Source') -> None:
        for statement in source.ast_root.statements:
            if (
                isinstance(statement, Assign)
                and isinstance(statement.value, Call)
                and statement.value.func.identifier == 'Rule'
                and statement.value.find_argument('description')
            ):
                rule_name = statement.target.identifier
                description = statement.value.find_argument('description')

                assert description
                if isinstance(description.value, FormatString):
                    self._rule_to_info_mapping[rule_name] = description.value.format_string
                elif isinstance(description.value, String):
                    self._rule_to_info_mapping[rule_name] = description.value.value

    def get_result(self) -> dict[str, str]:
        return self._rule_to_info_mapping
