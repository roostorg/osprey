import typing
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, TypeVar

from osprey.engine.language_types.osprey_invariant_generic import OspreyInvariantGeneric
from osprey.engine.language_types.post_execution_convertible import PostExecutionConvertible
from osprey.engine.utils.types import add_slots
from result import Err, Ok, Result
from typing_inspect import (
    get_args,
    get_parameters,
    is_callable_type,
    is_classvar,
    is_generic_type,
    is_tuple_type,
    is_union_type,
)

from .arguments import ConstExpr
from .type_helpers import UnsupportedTypeError, get_normalized_origin, get_single_inner_type, to_display_str


@add_slots
@dataclass
class CompatibleTypeInfo:
    unwrapped_to_type: type | None
    """If non-None, the given type had to be unwrapped (to this type) in order to be considered compatible."""


def is_compatible_type(
    type_t: type, accepted_by_t: type, allow_unwrap: bool = True
) -> Result[CompatibleTypeInfo | None, UnsupportedTypeError]:
    """Determines whether the given `type_t` is compatible with `accepted_by_t`.

    This assumes that the given type relationship is *covaraint*, aka type_t is allowed to be a subtype of
    accepted_by_t. For more on different types of variance see:
        https://mypy.readthedocs.io/en/stable/generics.html#variance-of-generic-types

    This will normally return a Result with an optional CompatibleTypeInfo in the Ok value. If that info is None,
    then the types aren't compatible. Otherwise, if the info is non-None, then the types *are* compatible. In this
    case, there's additional information (such as whether we needed to unwrap the type to make it compatible) on the
    object. However, if either of the types is not supported by our limited type system, this will return an Err with
    an UnsupportedTypeError.
    """
    try:
        if _is_compatible_type(type_t, accepted_by_t):
            # Non-None indicates it *is* compatible
            return Ok(CompatibleTypeInfo(unwrapped_to_type=None))

        # If we can, retry after unwrapping the type.
        if allow_unwrap:
            unwrapped_type_t = PostExecutionConvertible.maybe_post_execution_type(type_t)
            if unwrapped_type_t is not None and _is_compatible_type(unwrapped_type_t, accepted_by_t):
                # We are compatible *if* the value is unwrapped
                return Ok(CompatibleTypeInfo(unwrapped_to_type=unwrapped_type_t))

        # None indicates types aren't compatible
        return Ok(None)
    except UnsupportedTypeError as e:
        return Err(e)


def _is_compatible_type(type_t: type, accepted_by_t: type) -> bool:
    if type_t is Any or accepted_by_t is Any:
        return True

    # Unwrap any ConstExpr's we have
    if get_normalized_origin(type_t) == ConstExpr:
        type_t = get_single_inner_type(type_t)
    if get_normalized_origin(accepted_by_t) == ConstExpr:
        accepted_by_t = get_single_inner_type(accepted_by_t)

    # Special case for single argument invariant generics (eg inner types must be exactly the same).
    if _is_single_arg_invariant_generic(type_t) or _is_single_arg_invariant_generic(accepted_by_t):
        if get_normalized_origin(type_t) != get_normalized_origin(accepted_by_t):
            # Not both the same generic
            return False

        if len(get_parameters(type_t)) != 0:
            raise UnsupportedTypeError(
                message='generics must be specified',
                hint=f'got {to_display_str(type_t)}, which has a type variable in it',
            )
        if len(get_parameters(accepted_by_t)) != 0:
            raise UnsupportedTypeError(
                message='generics must be specified',
                hint=f'got {to_display_str(accepted_by_t)}, which has a type variable in it',
            )

        type_t_item = get_single_inner_type(type_t)
        accepted_by_t_item = get_single_inner_type(accepted_by_t)
        return _is_compatible_type(type_t_item, accepted_by_t_item)
        # return type_t_item is accepted_by_t_item or type_t_item is Any or accepted_by_t_item is Any

    # Fall through to Union-like behavior
    type_t_candidates = _get_type_candidates(type_t)
    accepted_by_t_candidates = _get_type_candidates(accepted_by_t)

    return all(_is_acceptable_candidate(candidate_t, accepted_by_t_candidates) for candidate_t in type_t_candidates)


_typevar_type: type = TypeVar


def _is_parameterized_generic(t: type) -> bool:
    # The goal here is to check for classes that are not themselves generic, even if they inherit from a generic
    # class that they fully specify. Eg we want to allow `class Foo(Sequence[str])` (which we are calling "non
    # parameterized") but not allow `class Foo(Generic[T], Sequence[T])` (which we are calling "parameterized").

    # Not a generic, bail early
    if not is_generic_type(t):
        return False

    # Ensure we're working with the base/origin of the generic class. Otherwise things like `Foo[str]` will show as
    # having no parameters.
    t = get_normalized_origin(t) or t

    # From python 3.9, get_parameters will return an empty tuple for non-indexed built-in generics
    # https://github.com/ilevkivskyi/typing_inspect/pull/94
    # Hack: Built-in generics are defined a special class typing._SpecialGenericAlias with an _nparams attribute
    if isinstance(t, typing._SpecialGenericAlias):  # type: ignore
        return t._nparams > 0

    # If this has any parameters
    return len(get_parameters(t)) > 0


def _is_simple_type(t: type) -> bool:
    # is_generic_type doesn't tell you about certain "special" generics, for seemingly historical reasons
    return (
        not _is_parameterized_generic(t)
        and not is_union_type(t)
        and not is_tuple_type(t)
        and not is_classvar(t)
        and not is_callable_type(t)
    )


def _is_single_arg_invariant_generic(t: type) -> bool:
    origin = get_normalized_origin(t)
    return (
        # NOTE: Treating lists as invariant is the only safe way to handle lists that might be mutated. If we assume
        # no mutation then we could do a `is_compatible_type` check on the list item types.
        origin is list or (isinstance(origin, type) and issubclass(origin, OspreyInvariantGeneric))
    )


def _get_type_candidates(type_t: type) -> Sequence[type]:
    if _is_simple_type(type_t):
        return [_coerce_none_type(type_t)]

    elif is_union_type(type_t):
        type_t_candidates = []
        for arg in get_args(type_t):
            # We only allow simple types inside a Union
            if not _is_simple_type(arg):
                raise UnsupportedTypeError(
                    f'cannot get type candidates for {to_display_str(type_t)},'
                    f' arg {to_display_str(arg)} is not a simple type'
                )

            type_t_candidates.append(_coerce_none_type(arg))
        return type_t_candidates

    else:
        raise UnsupportedTypeError(f'cannot get type candidates for {to_display_str(type_t)}, is an unknown generic')


def _coerce_none_type(type_t: type) -> type:
    if type_t is None:
        type_t = type(type_t)

    return type_t


def _is_acceptable_candidate(candidate_t: type, accepted_by_t_candidates: Sequence[type]) -> bool:
    return any(
        (
            (candidate_t is Any or accepted_t is Any)
            or (
                issubclass(candidate_t, accepted_t)
                # bools are subclasses of ints, and that sucks.
                and not (accepted_t is int and candidate_t is bool)
                # But we do want to allow putting an int where a float is needed.
                or (accepted_t is float and candidate_t is int)
            )
        )
        for accepted_t in accepted_by_t_candidates
    )
