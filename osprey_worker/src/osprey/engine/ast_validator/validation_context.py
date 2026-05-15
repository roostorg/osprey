from abc import ABC
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Type, cast

from osprey.engine.ast.error_utils import SpanWithHint, render_span_context_with_message
from osprey.engine.ast.errors import OspreySyntaxError
from osprey.engine.ast.grammar import Source, Span
from osprey.engine.ast.sources import Sources
from osprey.engine.config.config_subkey_handler import ModelT
from osprey.engine.utils.periodic_execution_yielder import maybe_periodic_yield

from .base_validator import BaseValidator, ConfigValidatorBase, HasInput, HasResult, T, T_co, ValidatorFailed
from .validator_registry import ValidatorRegistry

if TYPE_CHECKING:
    from osprey.engine.udf.registry import UDFRegistry


class ValidationContext:
    """Validation context holds data used by the validators that will run over a Sources collection.

    - It coordinates validation ordering, ensuring that validators run in the order they require, and stores
    validation output for use in downstream validators.
    - It stores error messages/warnings emitted from validators, and handles error propagation.
    """

    sources: Sources
    """The sources this validator is validating."""

    _validation_results: dict[Type[BaseValidator] | Type[HasResult[Any]], Any]
    """The stored results of a given validator."""

    _errors: list['ValidationError']
    """Errors emitted by the validators that have run."""

    _warnings: list['ValidationWarning']
    """Warnings emitted by the validators that have run."""

    _validator_stack: list[Type[BaseValidator]]
    """Stack of the currently running validators."""

    _validator_registry: ValidatorRegistry
    """The registry holding the validators that must run."""

    _udf_registry: 'UDFRegistry'
    """The registry holding the user defined functions that may be used by validators."""

    _validator_inputs: dict[Type[HasInput[Any]], Any]
    """Holds any dynamic inputs that the validators might need."""

    _warning_as_error: bool
    """Whether or not to treat warnings as errors."""

    def __init__(
        self,
        sources: Sources,
        udf_registry: 'UDFRegistry',
        validator_registry: ValidatorRegistry | None = None,
        warning_as_error: bool = False,
    ):
        self.sources = sources

        self._validation_results = {}
        self._errors = []
        self._warnings = []
        self._validator_registry = validator_registry or ValidatorRegistry.get_instance()
        self._udf_registry = udf_registry
        self._validator_stack = []
        self._validator_inputs = {}
        self._warning_as_error = warning_as_error

    def set_validator_input(self, validator_class: Type[HasInput[T]], value: T) -> 'ValidationContext':
        self._validator_inputs[validator_class] = value
        return self

    def get_validator_input(self, validator_class: Type[HasInput[T]], default: T) -> T:
        """If the validator has an input, returns that input to the validator. This input is provided in the constructor
        of the ValidationContext."""
        return cast(T, self._validator_inputs.get(validator_class, default))

    def add_error(
        self,
        message: str,
        span: Span,
        hint: str = '',
        additional_spans_message: str = '',
        additional_spans: Sequence[Span | SpanWithHint] = tuple(),
    ) -> None:
        """Adds an error message from the currently running validator. A validator can add many errors. Once an error
        is added, the validation is considered as failed. This means that any dependent validators will not run,
        however, unrelated validators may continue to run, and accumulate more errors."""
        self.add_message(
            ValidationError(
                message=message,
                span=span,
                hint=hint,
                additional_spans_message=additional_spans_message,
                additional_spans=additional_spans,
            )
        )

    def add_warning(
        self,
        message: str,
        span: Span,
        hint: str = '',
        additional_spans_message: str = '',
        additional_spans: Sequence[Span | SpanWithHint] = tuple(),
    ) -> None:
        """Adds a warning from the current running validator. A validator can add many warnings. Warnings are not fatal,
        and will not mark the validation as failed."""
        self.add_message(
            ValidationWarning(
                message=message,
                span=span,
                hint=hint,
                additional_spans_message=additional_spans_message,
                additional_spans=additional_spans,
            )
        )

    def add_message(self, message: 'ValidationWarning' | 'ValidationError') -> None:
        """Adds a given validation message to the validation context. It's preferred to use
        `add_warning` or `add_error` instead."""
        # If we have a currently running validator at the top of the stack, we'll tag the
        # message as originating from that validator.
        message.validator_class = self.current_validator

        if isinstance(message, ValidationWarning):
            self._warnings.append(message)

        elif isinstance(message, ValidationError):
            self._errors.append(message)

        else:
            raise ValueError(f'Unexpected message: {message!r}')

    @property
    def current_validator(self) -> Type[BaseValidator] | None:
        """Returns the currently running validator, or None if there is no current validator."""
        if not self._validator_stack:
            return None

        return self._validator_stack[-1]

    @property
    def udf_registry(self) -> 'UDFRegistry':
        """The UDF registry that was provided for use with validation."""
        return self._udf_registry

    def run(self) -> 'ValidatedSources':
        """Runs the validators that were registered with the given (or global) validator registry,
        throwing a `ValidationFailed` exception if any validators emitted an error.

        If successful, returns a `ValidatedSources`, which wraps the sources that have been validated."""
        # First, we need to parse all sources.
        self._parse_sources()

        # There were no parse errors, so we can run the validators.
        if not self._errors:
            # Iterate over all the validator classes and run them in no particular order.
            # their ordering is implicitly defined by their runtime requirements.
            for validator_class in self._validator_registry.get_validators():
                try:
                    self.run_validator(validator_class)
                except ValidatorFailed:
                    # If a validator failed, we don't do anything here, as it should have populated
                    # the `_errors` array, which we will then raise in the end. This allows us to have
                    # sibling validators return errors, such that we can present the most comprehensive
                    # error message list to the end user possible.
                    pass
                maybe_periodic_yield()

        if self._errors or (self._warning_as_error and self._warnings):
            raise ValidationFailed(self._errors, self._warnings)

        validation_results: dict[Type[HasResult[Any]], Any] = {}
        for k, v in self._validation_results.items():
            if issubclass(k, HasResult):
                validation_results[k] = v

        return ValidatedSources(sources=self.sources, warnings=self._warnings, validation_results=validation_results)

    def run_validator(self, validator_class: Type[BaseValidator]) -> None:
        """Gets the validation result of a given validator class"""

        # If the validator has not run yet, we'll need to run it, and then store it's result.
        if validator_class in self._validation_results:
            return

        # A quick sanity check to make sure that an unregistered validator class cannot be invoked.
        if not self._validator_registry.is_registered(validator_class):
            raise ValueError(f'Validator {validator_class.__name__} is not registered.')

        # Cycle detection: Report the validation cycle such that we can break it.
        if validator_class in self._validator_stack:
            raise ValidationCycleError(validator_class, self._validator_stack)

        # Mark the validator as running, by pushing it to the top of the validator stack.
        self._validator_stack.append(validator_class)
        try:
            validator = validator_class(self)
            validator.run()
            result = None

            # The validator returned an error, which means it's failed, and all
            # requisite validators thus will be marked as failed.
            if self._has_errors_for(validator_class):
                raise ValidatorFailed(validator_class)

            if isinstance(validator, HasResult):
                result = validator.get_result()

            self._validation_results[validator_class] = result
        except ValidatorFailed as err:
            self._validation_results[validator_class] = err
        # This assert should never fire, unless something has gone horribly wrong.
        assert self._validator_stack.pop() == validator_class

    def get_validator_result(self, validator_class: Type[HasResult[T_co]]) -> T_co:
        """Given a validator class, runs it if it hasn't run already, and returns its result."""
        if issubclass(validator_class, BaseValidator):
            self.run_validator(validator_class)

        result = self._validation_results[validator_class]
        # The result was actually a failure! Let's re-throw it it!
        if isinstance(result, ValidatorFailed):
            raise result

        return cast(T_co, result)

    def validator_depends_on(self, validator_classes: list[Type[BaseValidator]]) -> None:
        """Call from a validator's `.run()` or `__init__()` function, marking it as being dependent on the provided
        `validator_classes` to have run first."""

        for validator_class in validator_classes:
            # We force the validator to run, thus resolving the dependency.
            self.run_validator(validator_class)

        # Make sure none of these failed
        for validator_class in validator_classes:
            result = self._validation_results[validator_class]
            if isinstance(result, ValidatorFailed):
                raise result

    def _has_errors_for(self, validator_class: Type[BaseValidator]) -> bool:
        """Returns if any errors were emitted by the given validator class."""
        return any(e.validator_class == validator_class for e in self._errors)

    def _parse_sources(self) -> None:
        """Pre-parse the sources. This will trigger any AST validation errors, which we shall store.
        No further validators will run, if any of the sources fail to parse."""
        for source in self.sources:
            try:
                _unused = source.ast_root  # noqa E841
            except OspreySyntaxError as e:
                self.add_message(ValidationError(message=e.error, hint=e.hint, span=e.span, source=source))
            maybe_periodic_yield()

    def _test_only_inject_validator_result(self, validator_class: Type[BaseValidator], result: Any) -> None:
        assert validator_class not in self._validation_results
        self._validation_results[validator_class] = result

    def has_warnings(self) -> bool:
        """Returns whether or not this validation context has warnings."""
        return bool(self._warnings)

    def get_config_subkey(self, model_class: Type[ModelT]) -> ModelT:
        """Returns the parsed config model for the given subkey.

        Requires that exactly one config validator has been registered.
        """
        config_validator_classes = [
            validator
            for validator in self._validator_registry.get_validators()
            if issubclass(validator, ConfigValidatorBase)
        ]
        if len(config_validator_classes) == 0:
            raise ValueError('ConfigValidator is not registered!')
        elif len(config_validator_classes) > 1:
            raise ValueError('Multiple ConfigValidator classes registered!')
        config_validator = config_validator_classes[0]

        config_result = self.get_validator_result(config_validator)
        config_value = config_result[model_class]
        assert isinstance(config_value, model_class)
        return config_value


class _ValidationMessage(ABC):
    message_type: ClassVar[str]

    def __init__(
        self,
        message: str,
        hint: str,
        span: Span,
        source: Source | None = None,
        additional_spans_message: str = '',
        additional_spans: Sequence[Span | SpanWithHint] = tuple(),
    ):
        if source is None:
            source = span.source

        self.message = message
        self.span = span
        self.hint = hint
        self.source = source
        self.additional_spans_message = additional_spans_message
        self.additional_spans = additional_spans
        self.validator_class: Type[BaseValidator] | None = None

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} message={self.message!r} hint={self.hint!r} span={self.span!r}>'

    def rendered(self) -> str:
        """Renders the error message for printing out to the end-user."""
        return render_span_context_with_message(
            message=self.message,
            span=self.span,
            hint=self.hint,
            additional_spans_message=self.additional_spans_message,
            additional_spans=self.additional_spans,
            message_type=self.message_type,
        )


class ValidationError(_ValidationMessage):
    """Holds an error that a validator has emitted."""

    message_type = 'error'


class ValidationWarning(_ValidationMessage):
    """Holds a warning that a validator has emitted."""

    message_type = 'warning'


class ValidationCycleError(Exception):
    """Thrown when a validator requires another validator that depends
    on itself (through some direct or indirect cycle)"""


class ValidationFailed(Exception):
    """One or more of the validators has reported an error."""

    def __init__(self, errors: Sequence[ValidationError], warnings: Sequence[ValidationWarning]):
        assert errors or warnings, 'There should always be errors or warnings, if we have a failed validation.'
        super().__init__(errors, warnings)
        self.errors = errors
        self.warnings = warnings

    def rendered(self) -> str:
        parts = []
        if self.errors:
            parts.append(_render_validation_messages(self.errors, 'error'))

        if self.warnings:
            parts.append(_render_validation_messages(self.warnings, 'warning'))

        return '\n'.join(parts)


class ValidatedSources:
    """A wrapper object, that is created wrapping a Sources object, after it has been validated."""

    def __init__(
        self,
        sources: Sources,
        validation_results: dict[Type[HasResult[Any]], Any],
        warnings: Sequence[ValidationWarning],
    ):
        self.sources = sources
        self.warnings = warnings
        self._validation_results = validation_results

    @property
    def validation_results(self) -> dict[Type[HasResult[Any]], Any]:
        return self._validation_results

    def get_validator_result(self, validator_class: Type[HasResult[T_co]]) -> T_co:
        """Gets the result of a given validator. This is useful if the result of any validation
        computation is required by the downstream execution graph builder. That is to say, a validator can be
        coupled with the node executor. A good example of this is the Call node compiler, being dependent on
        the call node validator."""
        if validator_class not in self._validation_results:
            raise KeyError(
                f'Validator {validator_class.__name__} did not run - and thus there is no validation result for it'
            )

        return cast(T_co, self._validation_results[validator_class])

    def render_warnings(self) -> str:
        """Returns a human-readable string representing the warnings that were encountered while
        validating the sources."""
        return _render_validation_messages(self.warnings, 'warning')


def _render_validation_messages(messages: Sequence[_ValidationMessage], message_type: str) -> str:
    total = len(messages)
    if total == 1:
        return messages[0].rendered()

    parts = [f'{total} {message_type}s occurred while validating:']

    def sorting_key(validation_message: _ValidationMessage) -> tuple[str | int, ...]:
        return (
            validation_message.source.path,
            validation_message.span.start_line,
            validation_message.span.start_pos,
            validation_message.message,
        )

    for i, message in enumerate(sorted(messages, key=sorting_key), 1):
        rendered = message.rendered()
        parts.append(f'[{i}/{total}] {rendered}')

    return '\n'.join(parts)
