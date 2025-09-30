from typing import Sequence, Type

from osprey.engine.ast_validator.base_validator import BaseValidator
from osprey.engine.ast_validator.validator_registry import ValidatorRegistry
from osprey.engine.ast_validator.validators.feature_name_to_entity_type_mapping import (
    FeatureNameToEntityTypeMapping,
)
from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import (
    ImportsMustNotHaveCycles,
)
from osprey.engine.ast_validator.validators.no_unused_locals import NoUnusedLocals
from osprey.engine.ast_validator.validators.rule_name_to_description_mapping import (
    RuleNameToDescriptionMapping,
)
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_call_rvalue import ValidateCallRValue
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.ast_validator.validators.validate_experiments import ValidateExperiments
from osprey.engine.ast_validator.validators.validate_labels import ValidateLabels
from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes
from osprey.engine.ast_validator.validators.variables_must_be_defined import (
    VariablesMustBeDefined,
)

# The set of built-in rule validators that are considered "standard" for rules files
STANDARD_RULE_VALIDATORS: Sequence[Type[BaseValidator]] = [
    FeatureNameToEntityTypeMapping,
    ValidateStaticTypes,
    ValidateCallRValue,
    ImportsMustNotHaveCycles,
    ValidateLabels,
    NoUnusedLocals,
    ValidateExperiments,
    ValidateCallKwargs,
    RuleNameToDescriptionMapping,
    UniqueStoredNames,
    VariablesMustBeDefined,
    ValidateDynamicCallsHaveAnnotatedRValue,
]


def register_standard_rule_validators(registry: ValidatorRegistry | None = None) -> None:
    """Register the standard rule validators into the given registry instance.

    If no registry is provided, the global ValidatorRegistry singleton is used.
    """
    reg = registry or ValidatorRegistry.get_instance()
    for validator in STANDARD_RULE_VALIDATORS:
        reg.register(validator)
