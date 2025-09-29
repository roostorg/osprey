from typing import Any, Dict, List, Optional, TypeVar, Union

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
        (List[Any], '`List[Any]`'),
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
        (_T, List[str], List[str]),
        (Optional[_T], Optional[int], int),
        (List[_T], List[bool], bool),  # type: ignore[valid-type]
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
        (Optional[_T], List[str]),
        (List[_T], str),  # type: ignore[valid-type]
        (Dict[str, _T], Dict[int, str]),  # type: ignore[valid-type]
        (Dict[_T, _T], Dict[str, int]),  # type: ignore[valid-type]
        (Dict[_T, List[_T]], Dict[str, Optional[str]]),  # type: ignore[valid-type]
        (Dict[_T, _T2], Dict[str, int]),  # type: ignore[valid-type]
        (Dict[_T, int], Dict[str, int]),  # type: ignore[valid-type]
    ],
)
def test_get_typevar_substitution_fails(generic_type: type, resolved_type: type) -> None:
    with pytest.raises(UnsupportedTypeError):
        get_typevar_substitution(generic_type, resolved_type)
