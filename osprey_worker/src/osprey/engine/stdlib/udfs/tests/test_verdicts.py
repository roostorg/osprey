from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.feature_name_to_entity_type_mapping import (
    FeatureNameToEntityTypeMapping,
)
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import ExecuteWithResultFunction
from osprey.engine.stdlib import get_config_registry
from osprey.engine.stdlib.udfs.rules import Rule
from osprey.engine.stdlib.udfs.verdicts import DeclareVerdict
from osprey.engine.udf.registry import UDFRegistry

from ..rules import WhenRules

# Moved here because WhenRules is not included in the MVP yet

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators(
        [
            FeatureNameToEntityTypeMapping,
            ValidateCallKwargs,
            UniqueStoredNames,
            get_config_registry().get_validator(),
        ]
    ),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(DeclareVerdict, Rule, WhenRules)),
]


def test_declare_verdict(execute_with_result: ExecuteWithResultFunction) -> None:
    result = execute_with_result(
        {
            'main.sml': """
                R = Rule(when_all=[True], description='simple rule')
                WhenRules(
                    rules_any=[R],
                    then=[
                        DeclareVerdict(verdict="reject")
                    ]
                )
            """
        }
    )

    assert result.extracted_features['__verdicts'] == ['reject']
