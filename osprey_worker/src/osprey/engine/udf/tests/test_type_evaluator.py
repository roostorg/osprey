from typing import Any, Callable, Optional, Tuple, TypeVar, Union

import pytest
from osprey.engine.language_types.entities import EntityT
from osprey.engine.language_types.osprey_invariant_generic import OspreyInvariantGeneric
from osprey.engine.udf.arguments import ConstExpr
from osprey.engine.udf.type_evaluator import is_compatible_type
from osprey.engine.udf.type_helpers import to_display_str


class A:
    pass


class B(A):
    pass


class C(B):
    pass


_T = TypeVar('_T')


class Generic1(OspreyInvariantGeneric[_T]):
    pass


class Generic2(OspreyInvariantGeneric[_T]):
    pass


@pytest.mark.parametrize(
    'type_t, accepted_type_t, is_compatible',
    [
        # Simple types
        (int, int, True),
        (int, float, True),
        (int, bool, False),
        (bool, int, False),
        (float, int, False),
        # Class types
        (A, B, False),
        (A, A, True),
        (B, A, True),
        (C, A, True),
        (C, B, True),
        # Union types
        (int, Optional[int], True),
        (None, Optional[int], True),
        (Optional[int], int, False),
        (Union[int, float], Union[int, float, str], True),
        (Union[int, float], Union[int, str], False),
        (Union[int, float], int, False),
        (int, Union[int, bool], True),
        (bool, Union[int, bool], True),
        (A, Union[A, B], True),
        (C, Union[A, C], True),
        # List types (guh!)
        (list[int], list[int], True),
        (list[float], list[int], False),
        (list[float], list[Union[int, float]], True),
        (list[float], str, False),
        (str, list[float], False),
        # Any types
        (int, Any, True),
        (Optional[int], Any, True),
        (Union[int, str], Any, True),
        (A, Any, True),
        (list[int], Any, True),
        (list[Any], Any, True),
        (list[int], list[Any], True),
        (int, list[Any], False),
        (Any, int, True),
        (Any, Optional[int], True),
        (Any, Union[int, str], True),
        (Any, A, True),
        (Any, list[int], True),
        (Any, list[Any], True),
        (list[Any], list[int], True),
        (list[Any], int, False),
        (Any, Any, True),
        # ConstExpr
        (str, ConstExpr[str], True),
        (int, ConstExpr[str], False),
        (list[str], ConstExpr[list[str]], True),
        (ConstExpr[str], str, True),
        (ConstExpr[str], int, False),
        (ConstExpr[list[str]], list[str], True),
        # Allowed generics
        (Generic1[str], Generic1[str], True),
        (Generic1[A], Generic1[A], True),
        (Generic1[str], Generic1[int], False),
        (Generic1[B], Generic1[A], True),
        (Generic1[A], Generic2[A], False),
        (Optional[A], Optional[A], True),
    ],
    ids=lambda arg: arg if isinstance(arg, bool) else to_display_str(arg, include_quotes=False),  # type: ignore
)
def test_is_compatible_type_supported_types(type_t: type, accepted_type_t: type, is_compatible: bool) -> None:
    is_compatible_result = is_compatible_type(type_t, accepted_type_t)
    # First try to expose any errors we ran into
    if is_compatible_result.is_err():
        raise is_compatible_result.unwrap_err()
    # Then check the value is correct
    if is_compatible:
        assert is_compatible_result.unwrap() is not None
    else:
        assert is_compatible_result.unwrap() is None


@pytest.mark.parametrize(
    'type_t',
    [
        Tuple[str, ...],
        # Note: Union[list[str], str] is now supported with native types since list[str] is a fully specified generic
        Callable[[int], int],
        # Note: bare list is now supported as a simple type (unlike typing.List which was always generic)
        ConstExpr,
        EntityT,
        EntityT[_T],  # type: ignore[valid-type]
    ],
    ids=lambda arg: to_display_str(arg, include_quotes=False),  # type: ignore
)
def test_is_compatible_type_unsupported_types(type_t: type) -> None:
    is_compatible_result = is_compatible_type(type_t, type_t)
    assert is_compatible_result.is_err()
