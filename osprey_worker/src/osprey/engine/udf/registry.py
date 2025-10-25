from typing import Any, Dict, Iterator, Optional, Type

import typing_inspect
from osprey.engine.udf.arguments import EXTRA_ARGS_ATTR

from .base import UDFBase
from .type_evaluator import is_compatible_type
from .type_helpers import UnsupportedTypeError, get_osprey_generic_param, is_typevar, to_display_str


class UDFRegistry:
    def __init__(self) -> None:
        self._functions: Dict[str, Type[UDFBase[Any, Any]]] = {}

    @classmethod
    def with_udfs(cls, *funcs: Type[UDFBase[Any, Any]]) -> 'UDFRegistry':
        instance = cls()
        for func in funcs:
            instance.register(func)

        return instance

    def register(self, func: Type[UDFBase[Any, Any]]) -> Type[UDFBase[Any, Any]]:
        # Allow idempotent re-registration of the exact same class.
        existing = self.get(func.__name__)
        if existing is not None:
            if existing is func:
                return existing
            raise Exception(f'A function with the name {func.__name__} is already registered with {func}.')

        try:
            rvalue_type = func.get_rvalue_type()
            arguments_type = func.get_arguments_type()
        except IndexError:
            raise TypeError('Must subclass the generic UDFBase[Arguments, RValue].')

        # If generic, make sure it's valid
        _assert_valid_generic(func)

        # Check that the types involved are supported by our typing system
        _assert_supported_type(rvalue_type)
        for arg_name, arg_type in arguments_type.items().items():
            if arg_name == EXTRA_ARGS_ATTR:
                continue
            _assert_supported_type(arg_type)

        self._functions[func.__name__] = func
        return func

    def iter_functions(self) -> Iterator[Type[UDFBase[Any, Any]]]:
        """Iterate over all registered functions."""
        return iter(self._functions.values())

    def iter_function_names(self) -> Iterator[str]:
        return iter(self._functions.keys())

    def merge(self, other_registry: 'UDFRegistry') -> 'UDFRegistry':
        """Merges another registry into this one. Throws an error if there are any conflicting functions."""
        # Compute collisions first, in-case the merge will not cleanly apply.
        collisions = set(self._functions) & set(other_registry.iter_function_names())

        # Drop collisions if the items are the same
        for collision in list(collisions):
            if self.get(collision) is other_registry.get(collision):
                collisions.discard(collision)

        if collisions:
            collisions_str = ', '.join(sorted(collisions))
            raise Exception(
                f'Could not merge registry, the following functions exist in both registries: {collisions_str}'
            )

        # This should no longer throw, cos we've checked above.
        for func in other_registry.iter_functions():
            if self.get(func.__name__) is None:
                self.register(func)

        return self

    def get(self, function_name: str) -> Optional[Type[UDFBase[Any, Any]]]:
        return self._functions.get(function_name)


def _assert_valid_generic(func: Type[UDFBase[Any, Any]]) -> None:
    # See the UDFBase docstring for a full explanation of what this enforces.
    if get_osprey_generic_param(func, kind='UDF') is None:
        return

    _assert_valid_return_generic_type(func)
    _assert_valid_arguments_generic_type(func)


def _assert_valid_return_generic_type(func: Type[UDFBase[Any, Any]]) -> None:
    ret_type = func.get_rvalue_type()
    if is_typevar(ret_type):
        return

    ret_params = typing_inspect.get_parameters(ret_type)
    if len(ret_params) == 0:
        raise UnsupportedTypeError(
            message='generic UDF return type must have exactly one type parameter',
            hint=f'UDF {to_display_str(func)} return type {to_display_str(ret_type)} is not generic',
        )
    elif len(ret_params) > 1:
        raise UnsupportedTypeError(
            message='generic UDF return type must have exactly one type parameter',
            hint=f'UDF {to_display_str(func)} return type {to_display_str(ret_type)} has multiple generic parameters',
        )


def _assert_valid_arguments_generic_type(func: Type[UDFBase[Any, Any]]) -> None:
    args_type = func.get_arguments_type()
    if args_type.get_generic_param() is None:
        return

    if not args_type.get_generic_item_names(func_name=to_display_str(func, include_quotes=False)):
        raise UnsupportedTypeError(
            message='generic arguments type must have at least one generic item',
            hint=f'UDF {to_display_str(func)} generic arguments type {to_display_str(args_type)} has no generic items',
        )


def _assert_supported_type(t: type) -> None:
    if is_typevar(t):
        return

    if len(typing_inspect.get_parameters(t)) == 1:
        t = t[Any]  # type: ignore # Not happy with runtime type stuff
        assert len(typing_inspect.get_parameters(t)) == 0

    result = is_compatible_type(t, t)
    if result.is_err():
        raise result.unwrap_err()
    assert result.unwrap(), t
