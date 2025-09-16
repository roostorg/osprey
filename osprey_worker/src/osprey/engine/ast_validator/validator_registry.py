from typing import ClassVar, Iterator, Optional, Set, Type

from .base_validator import BaseValidator


class ValidatorRegistry:
    """Holds all the registered validators."""

    _instance: ClassVar[Optional['ValidatorRegistry']] = None

    @classmethod
    def get_instance(cls) -> 'ValidatorRegistry':
        """Gets the singleton instance of the validator registry.

        This is generally the one you'd want to use when executing osprey AST validation, however, for the purpose of
        tests, you can create your own validator registry, to only run a subset of validations."""
        if cls._instance is None:
            cls._instance = ValidatorRegistry()

        return cls._instance

    @classmethod
    def register_to_instance(cls, validator_class: Type[BaseValidator]) -> Type[BaseValidator]:
        """Register a validator to the global validator registry."""
        return cls.get_instance().register(validator_class)

    @classmethod
    def instance_with_additional_validators(cls, *additional_validators: Type[BaseValidator]) -> 'ValidatorRegistry':
        instance_clone = ValidatorRegistry()
        instance_clone._validators = cls.get_instance()._validators.copy()
        instance_clone._validators.update(additional_validators)

        return instance_clone

    def __init__(self) -> None:
        self._validators: Set[Type[BaseValidator]] = set()

    def get_validators(self) -> Iterator[Type[BaseValidator]]:
        """Return the registered validators."""
        return iter(self._validators)

    def register(self, validator_class: Type[BaseValidator]) -> Type[BaseValidator]:
        """Registers a given validator."""
        self._validators.add(validator_class)
        return validator_class

    def is_registered(self, validator_class: Type[BaseValidator]) -> bool:
        """Checks to see if the given validator class was registered."""
        return validator_class in self._validators

    @classmethod
    def from_validator_classes(cls, validator_classes: Set[Type[BaseValidator]]) -> 'ValidatorRegistry':
        """Convenience method to construct a validator registry from a list of validator classes."""
        validator_registry = cls()
        for validator_class in validator_classes:
            validator_registry.register(validator_class)

        return validator_registry
