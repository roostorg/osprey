from typing import Any, Dict, TypeVar

from jsonpath_rw import JSONPath, parse
from osprey.engine.executor.execution_context import ExpectedUdfException
from osprey.engine.udf.arguments import ConstExpr
from osprey.engine.udf.rvalue_type_checker import RValueTypeChecker
from osprey.engine.udf.type_helpers import to_display_str


def parse_path(path: ConstExpr[str]) -> JSONPath:
    with path.attribute_errors():
        try:
            return parse(path.value)
        except Exception as e:
            # There is a bug in jsonpath_rw that is throwing an error while trying to generate an error.
            # that's cool, but we can catch it and transmute it to something a little more relevant for now.
            if 'NoneType' in str(e):
                raise Exception('invalid json-path supplied')
            raise


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
    data: Dict[str, object],
    required: bool,
    coerce_type: bool,
    rvalue_type_checker: RValueTypeChecker,
) -> Any:
    matches = expr.find(data)

    if matches:
        value = matches[0].value
    else:
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
