from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, Type, TypeVar

from osprey.engine.utils.periodic_execution_yielder import maybe_periodic_yield
from pydantic import BaseModel

T_co = TypeVar('T_co', covariant=True)
T = TypeVar('T')

if TYPE_CHECKING:
    from osprey.engine.ast.grammar import Source

    from .validation_context import ValidationContext


class BaseValidator(ABC):
    """The base class that all validators need to inherit."""

    context: 'ValidationContext'
    """The context that is currently running the validator."""

    exclude_from_query_validation: bool = False
    """Whether the validator should be excluded from query validation."""

    def __init__(self, context: 'ValidationContext'):
        self.context = context

    def run(self) -> None:
        """Runs the validation, generally this involves introspecting the sources AST, and emitting
        errors via `self.context.add_error(...), if something is wrong, or warnings via
        `self.context.add_warning(...)"""
        raise NotImplementedError


class SourceValidator(BaseValidator):
    """Similar to base validator, but to avoid a level of nesting, you implement `validate_source` instead of `run`.

    This is useful when you don't need to look at global state across all sources, and just want to look at each
    source file individually."""

    def run(self) -> None:
        for source in self.context.sources:
            self.validate_source(source)
            maybe_periodic_yield()

    def validate_source(self, source: 'Source') -> None:
        """Implement this to validate a single source file, similar to how BaseValidator.run() would be implemented."""
        raise NotImplementedError


class ValidatorFailed(Exception):
    """Thrown when a validator completes and has emitted one or more error."""

    validator_class: Type[BaseValidator]
    """The validator class that had failed."""

    def __init__(self, validator_class: Type[BaseValidator]):
        super().__init__(validator_class)
        self.validator_class = validator_class


class HasResult(ABC, Generic[T_co]):
    """Mixin to indicate that the validator returns a result."""

    @abstractmethod
    def get_result(self) -> T_co:
        """Implement this to return a specific result for the source validator."""
        raise NotImplementedError


class HasInput(Generic[T]):
    """Mixin to indicate that the validator may take data as an input."""


class ConfigValidatorBase(BaseValidator, HasResult[dict[Type[BaseModel], BaseModel]], ABC):
    """Validates that a subkey of the config is compatible with a given Pydantic model."""
