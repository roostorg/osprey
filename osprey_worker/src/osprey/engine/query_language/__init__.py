from osprey.engine.ast.sources import SOURCE_ENTRY_POINT_PATH, Sources
from osprey.engine.ast_validator.validation_context import ValidatedSources, ValidationContext, ValidationFailed
from osprey.engine.ast_validator.validator_registry import ValidatorRegistry
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes
from osprey.engine.ast_validator.validators.variables_must_be_defined import VariablesMustBeDefined
from osprey.engine.query_language import udfs
from osprey.engine.query_language.ast_validator import REGISTRY
from osprey.engine.query_language.udfs.registry import UDF_REGISTRY
from osprey.engine.query_language.validate_count_over import ValidateCountOver  # noqa: F401
from osprey.engine.utils.imports import import_all_direct_children


def _consolidate_registry(validator_registry: ValidatorRegistry) -> ValidatorRegistry:
    validators = {
        validator
        for validator in ValidatorRegistry.get_instance().get_validators()
        if not validator.exclude_from_query_validation
    }
    registry_validators = set()
    for validator in validator_registry.get_validators():
        registry_validators.add(validator)

    return ValidatorRegistry.from_validator_classes(validators.union(registry_validators))


def parse_query_to_validated_ast(query: str, rules_sources: ValidatedSources) -> ValidatedSources:
    """
    Takes a string query (e.g. 'A == B or C == D', 'C <= 3 and D not in [4, 5, 6]')
    and returns a validated AST representation of it.
    """

    try:
        consolidated_registry = _consolidate_registry(REGISTRY)
        sources = Sources.from_dict({SOURCE_ENTRY_POINT_PATH: 'Query = ' + query})
        validation_context = (
            ValidationContext(sources=sources, udf_registry=UDF_REGISTRY, validator_registry=consolidated_registry)
            .set_validator_input(
                VariablesMustBeDefined, set(rules_sources.get_validator_result(UniqueStoredNames).keys())
            )
            .set_validator_input(
                ValidateStaticTypes,
                ValidateStaticTypes.to_post_execution_types(rules_sources.get_validator_result(ValidateStaticTypes)),
            )
        )
        validated_sources = validation_context.run()
        return validated_sources
    except ValidationFailed:
        raise


import_all_direct_children(udfs)
