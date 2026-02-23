import json
from collections.abc import Callable
from typing import Any

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import RunValidationFunction
from osprey.engine.query_language.udfs.did_mutate_label import DidAddLabel, DidRemoveLabel
from osprey.engine.stdlib import get_config_registry
from osprey.engine.udf.registry import UDFRegistry

pytestmark: list[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs, UniqueStoredNames, get_config_registry().get_validator()]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(DidAddLabel, DidRemoveLabel)),
]


def source_with_config(source: str) -> dict[str, str]:
    return {'main.sml': source, 'config.yaml': json.dumps({'labels': {'my_label': {'valid_for': ['MyEntity']}}})}


@pytest.mark.parametrize(
    'query',
    [
        "DidAddLabel(entity_type='MyEntity', label_name='my_label')",
        "DidRemoveLabel(entity_type='MyEntity', label_name='my_label')",
    ],
)
def test_did_mutate_label_accepts_valid_call(run_validation: RunValidationFunction, query: str) -> None:
    run_validation(source_with_config(query))
