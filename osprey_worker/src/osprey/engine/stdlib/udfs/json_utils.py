from typing import Any, TypeVar

from jsonpath_rw import Child, Fields, JSONPath, Root, parse
from jsonpath_rw import jsonpath as jsonpath_module
from osprey.engine.executor.execution_context import ExpectedUdfException
from osprey.engine.udf.arguments import ConstExpr
from osprey.engine.udf.rvalue_type_checker import RValueTypeChecker
from osprey.engine.udf.type_helpers import to_display_str

_MISSING: Any = object()
"""Sentinel for "the path matched nothing", which a matched value of `None` must not collide with."""


class FieldChainPath(Child):  # type: ignore[misc]  # jsonpath_rw does not provide type information.
    """A plain field chain with a direct lookup path."""

    __slots__ = ('_keys',)

    def __init__(self, keys: tuple[str, ...], expr: Child):
        super().__init__(expr.left, expr.right)
        self._keys = keys

    def get_first(self, data: object) -> Any:
        """The value at this path, or `_MISSING` when jsonpath_rw would report no match.

        `Fields.get_field_datum` subscripts (never `.get()`, so a present-but-null field is a
        match holding `None`) and treats exactly these exceptions as "no match".
        """
        value: Any = data
        for key in self._keys:
            try:
                value = value[key]
            except (TypeError, KeyError, AttributeError):
                return _MISSING
        return value

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._keys!r})'


def _compile_field_chain(expr: JSONPath) -> tuple[str, ...] | None:
    """The key tuple for a `$.a.b.c` expression, or `None` when the shape isn't provably a plain chain.

    Only a `Root` base and `Child(<left>, Fields(<one non-wildcard field>))` links qualify —
    wildcards, comma selectors, indices, slices, descendants, filters and `@`/`parent` bases all
    keep jsonpath_rw. Exact type checks (not `isinstance`) so a future jsonpath_rw node subclassing
    one of these can't silently inherit the fast path.
    """
    # The auto-id feature rewrites what a *missing* field resolves to, so only compile while it is
    # off. Osprey never enables it; a monkeypatch after rule compilation would not be picked up.
    if jsonpath_module.auto_id_field is not None:
        return None

    keys: list[str] = []
    node = expr
    while type(node) is Child:
        right = node.right
        if type(right) is not Fields or len(right.fields) != 1:
            return None
        field = right.fields[0]
        if not isinstance(field, str) or field == '*':
            return None
        keys.append(field)
        node = node.left

    if type(node) is not Root or not keys:
        return None

    keys.reverse()
    return tuple(keys)


def parse_path(path: ConstExpr[str]) -> JSONPath:
    with path.attribute_errors():
        try:
            expr = parse(path.value)
        except Exception as e:
            # There is a bug in jsonpath_rw that is throwing an error while trying to generate an error.
            # that's cool, but we can catch it and transmute it to something a little more relevant for now.
            if 'NoneType' in str(e):
                raise Exception('invalid json-path supplied')
            raise

    keys = _compile_field_chain(expr)
    if keys is None:
        return expr
    assert isinstance(expr, Child)
    return FieldChainPath(keys, expr)


class MissingJsonPath(Exception):
    def __init__(self, path: str) -> None:
        self.path = path

    def __str__(self) -> str:
        return f'Missing Json data at path `{self.path}`.'


class InvalidJsonType(TypeError):
    def __init__(self, path: str, expected_type: type, actual_type: type) -> None:
        self.path = path
        self.expected_type = expected_type
        self.actual_type = actual_type

    def __str__(self) -> str:
        return (
            f'Invalid type in Json data at path `{self.path}`.'
            f' Has type {to_display_str(self.actual_type)}, expected {to_display_str(self.expected_type)}.'
        )


_T = TypeVar('_T')


def get_from_data(
    expr: JSONPath,
    data: dict[str, object],
    required: bool,
    coerce_type: bool,
    rvalue_type_checker: RValueTypeChecker,
) -> Any:
    value: Any
    if type(expr) is FieldChainPath:
        value = expr.get_first(data)
    else:
        matches = expr.find(data)
        value = matches[0].value if matches else _MISSING

    if value is _MISSING:
        # If we can return None, do that
        if rvalue_type_checker.check(None):
            return None

        # Only perform the `required` check if our return type is incompatible with None
        if required:
            raise MissingJsonPath(str(expr))

        # Otherwise raise an expected/ignored failure to prevent dependencies from running, but not be reported
        # as an exception.
        raise ExpectedUdfException()

    if not rvalue_type_checker.check(value):
        if value is None and not required:
            # Special case to allow None with required=False to act like it wasn't specified
            raise ExpectedUdfException()
        else:
            if coerce_type and value is not None:
                try:
                    return rvalue_type_checker.coerce(value)
                except (TypeError, ValueError):
                    pass  # fallthrough to below InvalidJsonType
            raise InvalidJsonType(str(expr), rvalue_type_checker.to_typing_type(), type(value))

    return value
