from typing import Any, Dict, List, Tuple

import pytest
from jsonpath_rw import JSONPath, parse
from osprey.engine.stdlib.udfs.json_utils import (
    _MISSING,
    FieldChainPath,
    _compile_field_chain,
    parse_path,
)
from osprey.engine.udf.arguments import ConstExpr


def _parse_path(path: str) -> Any:
    return parse_path(ConstExpr.for_default('path', path))


@pytest.mark.parametrize(
    ('path', 'expected_keys'),
    [
        ('$.foo', ('foo',)),
        ('$.foo.bar', ('foo', 'bar')),
        ('$.http.headers.Cookie', ('http', 'headers', 'Cookie')),
    ],
)
def test_compiles_simple_field_chains(path: str, expected_keys: Tuple[str, ...]) -> None:
    compiled = _parse_path(path)
    assert isinstance(compiled, FieldChainPath)
    assert _compile_field_chain(parse(path)) == expected_keys
    # str() is what MissingJsonPath/InvalidJsonType report, so it must survive specialization.
    assert str(compiled) == path


@pytest.mark.parametrize(
    'path',
    [
        '$.a.*',  # wildcard field
        '$.a[*]',  # slice
        '$.a[0]',  # index
        '$.a[1:2]',  # bounded slice
        '$.a[0].b',  # index in the middle of a chain
        '$..b',  # descendants
        '$.a,b',  # comma selector
        '$',  # bare root
        'foo.bar',  # no root
        '@.a',  # `this` base
    ],
)
def test_falls_back_to_jsonpath_rw(path: str) -> None:
    assert not isinstance(_parse_path(path), FieldChainPath)
    assert _compile_field_chain(parse(path)) is None


_EQUIVALENCE_CASES: List[Tuple[str, Dict[str, object]]] = [
    # Simple chains against documents that hit, miss, or hold None.
    ('$.a', {'a': 1}),
    ('$.a', {'a': None}),
    ('$.a', {}),
    ('$.a', {'b': 1}),
    ('$.a.b', {'a': {'b': 'x'}}),
    ('$.a.b', {'a': {'b': None}}),
    ('$.a.b', {'a': {}}),
    ('$.a.b', {}),
    ('$.a.b.c', {'a': {'b': {'c': 0}}}),
    ('$.a.b.c', {'a': {'b': {}}}),
    # Missing intermediate node.
    ('$.a.b.c', {'a': None}),
    ('$.a.b.c', {'a': {'b': None}}),
    # Non-dict intermediates: list, str, int, and an object with no __getitem__.
    ('$.a.b', {'a': [1, 2]}),
    ('$.a.b', {'a': 'string'}),
    ('$.a.b', {'a': 7}),
    ('$.a.b', {'a': object()}),
    # A key whose value shadows a falsy match.
    ('$.a.b', {'a': {'b': False}}),
    ('$.a.b', {'a': {'b': []}}),
    # Non-simple shapes still route through jsonpath_rw.
    ('$.a.*', {'a': {'b': 'x'}}),
    ('$.a[0]', {'a': ['x', 'y']}),
    ('$.a[0].b', {'a': [{'b': 'x'}]}),
    ('$..b', {'a': {'b': 'x'}}),
]


@pytest.mark.parametrize(('path', 'document'), _EQUIVALENCE_CASES)
def test_fast_path_matches_jsonpath_rw(path: str, document: Dict[str, object]) -> None:
    """The specialized accessor and jsonpath_rw must agree on both "did it match" and the value."""
    matches = parse(path).find(document)
    expected: Any = matches[0].value if matches else _MISSING

    compiled = _parse_path(path)
    if isinstance(compiled, FieldChainPath):
        actual = compiled.get_first(document)
    else:
        found = compiled.find(document)
        actual = found[0].value if found else _MISSING

    assert actual is expected or actual == expected


@pytest.mark.parametrize(('path', 'document'), _EQUIVALENCE_CASES)
def test_find_delegates_to_jsonpath_rw(path: str, document: Dict[str, object]) -> None:
    """`find()` stays available and identical for any caller holding a parsed path."""
    assert _parse_path(path).find(document) == parse(path).find(document)


@pytest.mark.parametrize('path', ['$.a', '$.missing'])
def test_update_delegates_to_jsonpath_rw(path: str) -> None:
    assert _parse_path(path).update({'a': 1}, 2) == parse(path).update({'a': 1}, 2)


def test_repr_and_equality() -> None:
    compiled = _parse_path('$.a.b')
    original = parse('$.a.b')
    assert isinstance(compiled, JSONPath)
    assert repr(compiled) == "FieldChainPath(('a', 'b'))"
    assert compiled == original
    assert original == compiled
    assert compiled == _parse_path('$.a.b')
    assert compiled != _parse_path('$.a.c')
