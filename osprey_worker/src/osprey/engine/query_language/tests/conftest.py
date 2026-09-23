from collections.abc import Sequence

import pytest
from osprey.engine.ast_validator.validation_context import ValidatedSources
from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes
from osprey.engine.ast_validator.validators.variables_must_be_defined import VariablesMustBeDefined
from osprey.engine.conftest import RunValidationFunction
from osprey.engine.query_language.ast_validator import REGISTRY
from osprey.engine.query_language.udfs.registry import UDF_REGISTRY
from typing_extensions import Protocol


class MakeRulesSourcesFunction(Protocol):
    def __call__(self, names: Sequence[str | tuple[str, str]]) -> ValidatedSources: ...


@pytest.fixture()
def make_rules_sources(run_validation: RunValidationFunction) -> MakeRulesSourcesFunction:
    def _make_rules_sources(names: Sequence[str | tuple[str, str]]) -> ValidatedSources:
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


@pytest.fixture(autouse=True)
def register_ast_validators():
    REGISTRY.register(UniqueStoredNames)
    REGISTRY.register(ValidateCallKwargs)
    REGISTRY.register(ValidateStaticTypes)
    REGISTRY.register(VariablesMustBeDefined)
    REGISTRY.register(ImportsMustNotHaveCycles)
    REGISTRY.register(ValidateDynamicCallsHaveAnnotatedRValue)


@pytest.fixture(autouse=True)
def restore_rejected_udf_registrations():
    """Undoes any UDF_REGISTRY.rejected_registrations entries a test adds, so a test that deliberately
    registers a broken UDF can't leak state into test_no_udf_registrations_were_rejected (or any other
    test) that happens to run afterward. Snapshotting before and restoring to that snapshot after (not
    just clearing) preserves anything that was already rejected at collection time, which is the actual
    thing that test is meant to catch."""
    snapshot = list(UDF_REGISTRY.rejected_registrations)
    yield
    UDF_REGISTRY.rejected_registrations[:] = snapshot
