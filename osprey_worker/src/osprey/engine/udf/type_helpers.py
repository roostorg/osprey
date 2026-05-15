from typing import TYPE_CHECKING, Any, Type, TypeVar, overload

from osprey.engine.ast.grammar import Span
from osprey.engine.language_types.osprey_invariant_generic import OspreyInvariantGeneric
from typing_inspect import get_args, get_parameters, is_generic_type, is_union_type
from typing_inspect import get_origin as get_origin_not_normalized

if TYPE_CHECKING:
    from osprey.engine.ast_validator.validation_context import ValidationContext

    from .arguments import ArgumentsBase


_T = TypeVar('_T')


class UnsupportedTypeError(Exception):
    """Indicates that we've come across a type that our system doesn't handle.

    This can be things like nested generics or generics with the wrong number of arguments. Message and hint are
    expected to be passed to `ValidationContext.add_error(...)`.
    """

    def __init__(self, message: str, hint: str = '') -> None:
        self.message = message
        self.hint = hint

    def add_error(self, context: 'ValidationContext', span: Span) -> None:
        context.add_error(message=self.message, span=span, hint=self.hint)

    def __str__(self) -> str:
        hint_part = '' if not self.hint else f': {self.hint}'
        return f'{self.message}{hint_part}'


def is_typevar(t: type) -> bool:
    """Helper to check if a value is a `TypeVar`, since mypy doesn't like doing this."""
    type_var_type: type = TypeVar
    return isinstance(t, type_var_type)


def get_osprey_generic_param(t: type, kind: str) -> type | None:
    """
    If the type is generic, asserts that it inherits from `OspreyInvariantGeneric` and returns the `TypeVar` in the
    generic. If not generic, returns `None`. The `kind` is used for better error messages.
    """
    generic_params = get_parameters(t)
    if len(generic_params) == 0:
        return None

    origin = get_normalized_origin(t) or t
    if not issubclass(origin, OspreyInvariantGeneric):
        raise UnsupportedTypeError(
            message='Osprey generics must inherit from `OspreyInvariantGeneric`',
            hint=f'generic {kind} type {to_display_str(t)} does not inherit from `OspreyInvariantGeneric`',
        )

    if len(generic_params) > 1:
        raise UnsupportedTypeError(
            message='Osprey generics must have exactly one parameter',
            hint=f'generic {kind} type {to_display_str(t)} has {len(generic_params)} parameters',
        )
    (generic_param,) = generic_params

    return generic_param


def to_display_str(t: type | None, include_quotes: bool = True) -> str:
    """Given a type, returns a string suitable for pretty printing it, eg in an error message.

    By defaults includes quotes around the message, but those can be turned off with the include_quotes option.
    """

    def _to_display_str(inner_t: type | None) -> str:
        if inner_t is None or inner_t is type(None):  # noqa: E721
            return 'None'
        elif isinstance(inner_t, type) or is_typevar(t):
            return inner_t.__name__
        elif is_generic_type(inner_t) or is_union_type(inner_t):
            origin_name = get_origin_name(inner_t)
            args = get_args(inner_t)

            # Special case Optional
            args_without_none = [arg for arg in args if arg is not type(None)]  # noqa: E721
            if is_union_type(inner_t) and len(args) == 2 and type(None) in args and len(args_without_none) == 1:
                (optional_arg,) = args_without_none
                return f'Optional[{_to_display_str(optional_arg)}]'

            args_str = ', '.join(_to_display_str(arg) for arg in args)
            return f'{origin_name}[{args_str}]'
        else:
            return repr(inner_t).replace('typing.', '')

    display_str = _to_display_str(t)
    if include_quotes:
        return f'`{display_str}`'
    else:
        return display_str


_ORIGIN_TO_NORMALIZED_ORIGIN: dict[type | None, type] = {
    list: list,
}


def get_normalized_origin(t: type) -> type | None:
    """Like typing_inspect.get_origin, but normalizes special types, eg `list` -> `list`."""
    origin_not_normalized = get_origin_not_normalized(t)
    return _ORIGIN_TO_NORMALIZED_ORIGIN.get(origin_not_normalized, origin_not_normalized)


def get_origin_name(t: type) -> str | None:
    """Gets the name of the generic origin class, since the origin isn't always actually a `type`."""
    origin = get_normalized_origin(t)
    origin_name = getattr(origin, '__name__', None) or getattr(origin, '_name', None)
    assert isinstance(origin_name, (str, type(None)))
    return origin_name


def is_list(type_t: type) -> bool:
    """Returns whether or not the given type is a list."""
    return get_normalized_origin(type_t) is list


def get_list_item_type(t: type) -> type:
    """Gets the type of the list, or raises UnsupportedTypeError if it's an unsupported list type."""
    return get_single_inner_type(t)


def get_single_inner_type(t: type) -> type:
    """Asserts the given type is a generic type with a single argument and returns that argument type."""
    t_args = get_args(t)
    t_origin = get_normalized_origin(t)

    if len(t_args) == 0 or t is t_origin:
        t_origin_str = to_display_str(t_origin, include_quotes=False)
        raise UnsupportedTypeError(
            message=f'`{t_origin_str}` types must specify their item type',
            hint=f'eg `{t_origin_str}[int]`; if you want a `{t_origin_str}` of `Any` type use `{t_origin_str}[Any]`',
        )
    elif len(t_args) != 1:
        raise UnsupportedTypeError(
            message=f'{to_display_str(t_origin)} types must have exactly one item type, got {len(t_args)} types'
        )

    return t_args[0]


@overload
def validate_kwarg_node_type(
    context: 'ValidationContext',
    udf: object,
    arguments: 'ArgumentsBase',
    kwarg: str,
    message_ending: str,
    expected_type: Type[_T],
    expected_str: str,
) -> _T | None: ...


# See https://gitlab.com/pycqa/flake8/issues/423
@overload  # noqa: F811
def validate_kwarg_node_type(  # noqa: F811
    context: 'ValidationContext',
    udf: object,
    arguments: 'ArgumentsBase',
    kwarg: str,
    message_ending: str,
    expected_type: tuple[type, ...],
    expected_str: str,
) -> object: ...


# See https://gitlab.com/pycqa/flake8/issues/423
def validate_kwarg_node_type(  # noqa: F811
    context: 'ValidationContext',
    udf: object,
    arguments: 'ArgumentsBase',
    kwarg: str,
    message_ending: str,
    expected_type: Type[_T] | tuple[type, ...],
    expected_str: str,
) -> object:
    kwarg_ast = arguments.get_argument_ast(kwarg)
    if isinstance(kwarg_ast, expected_type):
        return kwarg_ast

    context.add_error(
        message=f'argument `{kwarg}` to {to_display_str(type(udf))} {message_ending}',
        span=kwarg_ast.span,
        hint=f'got {to_display_str(type(kwarg_ast))} node, expected {expected_str}',
    )

    return None


AnyType: type = Any  # type: ignore # Mypy thinks Any is an object.


def _get_args_excluding_nonetype(t: type) -> list[type]:
    return [arg for arg in get_args(t) if arg is not type(None)]  # noqa: E721


def get_typevar_substitution(
    generic_type: type,
    resolved_type: type,
) -> type:
    """
    Returns the resolved type for the single type variable contained in `generic_type`. If
    `generic_type` is not either a bare typevar or a generic type where the typevar is the only
    argument, or if `generic_type` and `resolved_type` don't match, then this raises an
    `UnsupportedTypeError`.
    """
    if get_parameters(resolved_type):
        raise UnsupportedTypeError(
            f'resolved type must be fully specified, got {resolved_type}, which has typevar parameters'
        )

    if is_typevar(generic_type):
        return resolved_type

    params = get_parameters(generic_type)
    if len(params) != 1:
        raise UnsupportedTypeError(f'generic type {generic_type} should only have 1 typevar, got {params}')

    generic_type_args = _get_args_excluding_nonetype(generic_type)
    resolved_type_args = _get_args_excluding_nonetype(resolved_type)

    if len(generic_type_args) != 1 or len(resolved_type_args) != 1:
        raise UnsupportedTypeError(
            f'both generic type {generic_type} and resolved type {resolved_type} can only have one argument'
        )

    if not is_typevar(generic_type_args[0]):
        raise UnsupportedTypeError(
            f'generic type {generic_type} must have exactly one typevar nested at most one level deep'
        )

    if get_normalized_origin(generic_type) != get_normalized_origin(resolved_type):
        raise UnsupportedTypeError(
            f'generic type {generic_type} and concrete type {resolved_type} have different origins'
        )

    resolved = resolved_type_args[0]

    return resolved
