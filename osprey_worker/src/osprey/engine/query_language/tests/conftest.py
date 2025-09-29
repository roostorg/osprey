from typing import Sequence, Tuple, Union

import pytest
from osprey.engine.ast_validator.validation_context import ValidatedSources
from osprey.engine.conftest import RunValidationFunction
from typing_extensions import Protocol


class MakeRulesSourcesFunction(Protocol):
    def __call__(self, names: Sequence[Union[str, Tuple[str, str]]]) -> ValidatedSources: ...


@pytest.fixture()
def make_rules_sources(run_validation: RunValidationFunction) -> MakeRulesSourcesFunction:
    def _make_rules_sources(names: Sequence[Union[str, Tuple[str, str]]]) -> ValidatedSources:
        source_lines = []
        for name_and_value in names:
            if isinstance(name_and_value, tuple):
                name, value = name_and_value
            else:
                name = name_and_value
                value = '1'
            source_lines.append(f'{name} = {value}')
        return run_validation('\n'.join(source_lines))

    return _make_rules_sources
