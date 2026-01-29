from typing import Any, Optional, TypeVar, Union

import pytest
from osprey.engine.language_types.entities import EntityT
from osprey.engine.udf.arguments import ConstExpr
from osprey.engine.udf.type_helpers import UnsupportedTypeError, get_typevar_substitution, to_display_str


@pytest.mark.parametrize(
    'type_t, expected_str',
    [
        (str, '`str`'),
        (None, '`None`'),
        (type(None), '`None`'),
        (Optional[str], '`Optional[str]`'),
        (Union[str, None], '`Optional[str]`'),
        (Union[str, int], '`Union[str, int]`'),
        (list[Any], '`list[Any]`'),
        (ConstExpr[str], '`ConstExpr[str]`'),
        (EntityT, '`Entity`'),
    ],
)
def test_to_display_str(type_t: type, expected_str: str) -> None:
    assert to_display_str(type_t) == expected_str


_T = TypeVar('_T')
_T2 = TypeVar('_T2')


@pytest.mark.parametrize(
    'generic_type, resolved_type, expected_typevar_type',
    [
        (_T, str, str),
        (_T, list[str], list[str]),
        (Optional[_T], Optional[int], int),
        (list[_T], list[bool], bool),  # type: ignore[valid-type]
        (Optional[_T], Optional[Any], Any),
        (Optional[_T], Optional[str], str),
    ],
)
def test_get_typevar_substitution(generic_type: type, resolved_type: type, expected_typevar_type: type) -> None:
    assert get_typevar_substitution(generic_type, resolved_type) == expected_typevar_type


@pytest.mark.parametrize(
    'generic_type, resolved_type',
    [
        (_T, Optional[_T]),
        (Optional[_T], Optional[_T]),
        (Optional[_T], list[str]),
        (list[_T], str),  # type: ignore[valid-type]
        (dict[str, _T], dict[int, str]),  # type: ignore[valid-type]
        (dict[_T, _T], dict[str, int]),  # type: ignore[valid-type]
        (dict[_T, list[_T]], dict[str, Optional[str]]),  # type: ignore[valid-type]
        (dict[_T, _T2], dict[str, int]),  # type: ignore[valid-type]
        (dict[_T, int], dict[str, int]),  # type: ignore[valid-type]
    ],
)
def test_get_typevar_substitution_fails(generic_type: type, resolved_type: type) -> None:
    with pytest.raises(UnsupportedTypeError):
        get_typevar_substitution(generic_type, resolved_type)
