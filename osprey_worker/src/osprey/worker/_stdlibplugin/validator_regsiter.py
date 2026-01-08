from typing import Sequence, Type

from osprey.engine.ast_validator.base_validator import BaseValidator
from osprey.engine.ast_validator.validators.feature_name_to_entity_type_mapping import FeatureNameToEntityTypeMapping
from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
from osprey.engine.ast_validator.validators.no_unused_locals import NoUnusedLocals
from osprey.engine.ast_validator.validators.rule_name_to_description_mapping import RuleNameToDescriptionMapping
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_call_rvalue import ValidateCallRValue
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.ast_validator.validators.validate_experiments import ValidateExperiments
from osprey.engine.ast_validator.validators.validate_extractable_feature_types import ValidateExtractableFeatureTypes
from osprey.engine.ast_validator.validators.validate_labels import ValidateLabels
from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes
from osprey.engine.ast_validator.validators.variables_must_be_defined import VariablesMustBeDefined
from osprey.worker.adaptor.plugin_manager import hookimpl_osprey


@hookimpl_osprey
def register_ast_validators() -> Sequence[Type[BaseValidator]]:
    return [
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
        ValidateExtractableFeatureTypes,
    ]
