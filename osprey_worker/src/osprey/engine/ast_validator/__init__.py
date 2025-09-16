from osprey.engine.ast.sources import Sources
from osprey.engine.udf.registry import UDFRegistry

from .validation_context import ValidatedSources, ValidationContext
from .validator_registry import ValidatorRegistry


def validate_sources(
    sources: Sources, udf_registry: UDFRegistry, validator_registry: ValidatorRegistry
) -> ValidatedSources:
    """Given a sources collection, run the set of validators, returning a ValidatedSources if the sources
    are valid, or throwing a ValidationFailed error if there are any validation errors."""
    return ValidationContext(sources, udf_registry=udf_registry, validator_registry=validator_registry).run()
