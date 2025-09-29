import pytest
from osprey.engine.ast_validator.validators.feature_name_to_entity_type_mapping import FeatureNameToEntityTypeMapping
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import RunValidationFunction

pytestmark = [
    pytest.mark.use_validators([FeatureNameToEntityTypeMapping, ValidateCallKwargs, UniqueStoredNames]),
    pytest.mark.use_osprey_stdlib,
]


def test_builds_mapping_as_validator_result(run_validation: RunValidationFunction) -> None:
    validated_sources = run_validation(
        """
        UserId = Entity(type='User', id=1234)
        UserEmail = Entity(type='Email', id='hello@foo.bar')
        _UserPhone = Entity(type='Phone', id='123-456-7890')
        """
    )
    assert validated_sources.get_validator_result(FeatureNameToEntityTypeMapping) == {
        'UserId': 'User',
        'UserEmail': 'Email',
    }
