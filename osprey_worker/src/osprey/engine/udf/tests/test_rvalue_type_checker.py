from typing import List, Sequence, Tuple, Type

import pytest
from osprey.engine.ast.grammar import Annotation, AnnotationWithVariants, Span
from osprey.engine.ast.sources import Sources
from osprey.engine.language_types.entities import EntityT
from osprey.engine.udf.rvalue_type_checker import AnnotationConversionError, convert_ast_annotation_to_type_checker
from result import Err, Ok, Result


@pytest.fixture(scope='module')
def sources() -> Sources:
    return Sources.from_dict({'main.sml': 'hello'})


@pytest.fixture(scope='module')
def span(sources: Sources) -> Span:
    return Span(source=sources.get_entry_point(), start_line=0, start_pos=0)


@pytest.mark.parametrize(
    'identifier, value, success',
    (
        ('str', 'hello', True),
        ('str', 123, False),
        ('str', 123.4, False),
        ('str', True, False),
        ('str', None, False),
        ('int', 'hello', False),
        ('int', 123, True),
        ('int', 123.4, False),
        ('int', True, False),
        ('int', None, False),
        ('float', 'hello', False),
        ('float', 123, True),
        ('float', 123.4, True),
        ('float', True, False),
        ('float', None, False),
        ('bool', 'hello', False),
        ('bool', 123, False),
        ('bool', 123.4, False),
        ('bool', True, True),
        ('bool', None, False),
        ('None', 'hello', False),
        ('None', 123, False),
        ('None', 123.4, False),
        ('None', True, False),
        ('None', None, True),
    ),
)
def test_conversion_of_simple_annotation(span: Span, identifier: str, value: object, success: bool) -> None:
    annotation = Annotation(identifier=identifier, span=span)
    checker = convert_ast_annotation_to_type_checker(annotation)

    if success:
        assert checker.check(value)
    else:
        assert not checker.check(value)


def test_conversion_of_invalid_simple_annotation(span: Span) -> None:
    annotation = Annotation(identifier='invalid', span=span)

    with pytest.raises(AnnotationConversionError) as e:
        convert_ast_annotation_to_type_checker(annotation)

    assert e.match('can only check dynamic return values of')


@pytest.mark.parametrize(
    'outer_identifier, inner_identifiers, value, success',
    (
        ('Union', ('str', 'int'), 'hello', True),
        ('Union', ('str', 'int'), 1234, True),
        ('Union', ('str', 'int'), None, False),
        ('Union', ('str', 'int'), True, False),
        ('List', ('str',), ['hello', 'world'], True),
        ('List', ('str',), ['hello', 1234], False),
        ('List', ('str',), 'hello', False),
        ('List', ('str',), 1234, False),
        ('Optional', ('str',), None, True),
        ('Optional', ('str',), 'hello', True),
        ('Optional', ('str',), 1234, False),
        ('Entity', ('str',), EntityT('Email', 'hello@example.com'), True),
        ('Entity', ('str',), EntityT('User', 1234), False),
    ),
    ids=repr,
)
def test_conversion_of_complex_annotation(
    span: Span, outer_identifier: str, inner_identifiers: Sequence[str], value: object, success: bool
) -> None:
    annotation = AnnotationWithVariants(
        span=span,
        identifier=outer_identifier,
        variants=[Annotation(span=span, identifier=inner_identifier) for inner_identifier in inner_identifiers],
    )
    checker = convert_ast_annotation_to_type_checker(annotation)

    if success:
        assert checker.check(value)
    else:
        assert not checker.check(value)


@pytest.mark.parametrize(
    'outer_identifier, inner_identifiers, error_fragment',
    (
        ('Union', ('str', 'str'), 'duplicated type'),
        ('Union', ('str', 'invalid'), 'expected simple type'),
        ('Union', ('str',), '`Union` of one item'),
        ('List', ('str', 'int'), 'unexpected additional'),
        ('Unknown', ('str', 'int'), 'can only check dynamic return values of'),
        ('Optional', (), '`Optional` must have exactly one argument'),
        ('Optional', ('str', 'int'), '`Optional` must have exactly one argument'),
        ('Optional', ('List',), 'expected simple type in `Optional`'),
        ('Entity', (), '`Entity` must have exactly one argument'),
        ('Entity', ('str', 'int'), '`Entity` must have exactly one argument'),
        ('Entity', ('float',), '`Entity` only supports'),
    ),
    ids=repr,
)
def test_conversion_of_invalid_complex_annotation(
    span: Span, outer_identifier: str, inner_identifiers: Sequence[str], error_fragment: str
) -> None:
    annotation = AnnotationWithVariants(
        span=span,
        identifier=outer_identifier,
        variants=[Annotation(span=span, identifier=inner_identifier) for inner_identifier in inner_identifiers],
    )

    with pytest.raises(AnnotationConversionError) as e:
        convert_ast_annotation_to_type_checker(annotation)

    assert e.match(error_fragment)


@pytest.mark.parametrize(
    'identifier, value, result',
    (
        ('str', 123, Ok('123')),
        ('str', 123.4, Ok('123.4')),
        ('str', 'abc', Ok('abc')),
        ('str', True, Ok('True')),
        ('str', None, Err('str')),
        ('int', 123, Ok(123)),
        ('int', 123.4, Ok(123)),
        ('int', '123', Ok(123)),
        ('int', 'abc', Err('int')),
        ('int', True, Err('int')),
        ('int', False, Err('int')),
        ('int', None, Err('int')),
        ('float', 123, Ok(123.0)),
        ('float', 123.4, Ok(123.4)),
        ('float', 'abc', Err('float')),
        ('float', True, Err('float')),
        ('float', False, Err('float')),
        ('float', None, Err('float')),
        ('bool', 'abc', Ok(True)),
        ('bool', '', Ok(False)),
        ('bool', 123, Ok(True)),
        ('bool', 0, Ok(False)),
        ('bool', 123.4, Ok(True)),
        ('bool', 0.0, Ok(False)),
        ('bool', True, Ok(True)),
        ('bool', False, Ok(False)),
        ('bool', None, Ok(False)),
        ('None', '', Err('Cannot coerce non-None')),
        ('None', 123, Err('Cannot coerce non-None')),
        ('None', 123.4, Err('Cannot coerce non-None')),
        ('None', True, Err('Cannot coerce non-None')),
        ('None', False, Err('Cannot coerce non-None')),
        ('None', None, Ok(None)),
    ),
)
def test_coerce_simple(span: Span, identifier: str, value: object, result: Result[object, str]) -> None:
    annotation = Annotation(identifier=identifier, span=span)
    checker = convert_ast_annotation_to_type_checker(annotation)

    if result.is_ok():
        assert checker.coerce(value) == result.unwrap()
    else:
        expected_exceptions: Tuple[Type[Exception], ...] = (ValueError, TypeError)
        with pytest.raises(expected_exceptions) as e:
            checker.coerce(value)

        assert e.match(result.unwrap_err())


@pytest.mark.parametrize(
    'outer_identifier, inner_identifiers, value, result',
    (
        ('List', ['int'], [1, 2, 3], Ok([1, 2, 3])),
        ('List', ['int'], ['1', 2, 3.0], Ok([1, 2, 3])),
        ('List', ['int'], (1, 2, 3), Ok([1, 2, 3])),
        ('List', ['int'], {1, 2, 3}, Err('Cannot coerce non-Sequence')),
        ('List', ['int'], 123, Err('Cannot coerce non-Sequence')),
        ('List', ['str'], 'abc', Err('Cannot coerce non-Sequence')),
        ('List', ['str'], b'abc', Err('Cannot coerce non-Sequence')),
        ('Union', ['str', 'float'], 123.4, Ok(123.4)),
        ('Union', ['str', 'int'], 123.4, Ok('123.4')),
        ('Union', ['int', 'str'], 123.4, Ok(123)),
        ('Union', ['int', 'str'], None, Err('Cannot coerce `None')),
        ('Optional', ['str'], 'abc', Ok('abc')),
        ('Optional', ['str'], None, Ok(None)),
        ('Optional', ['str'], 123, Ok('123')),
        ('Optional', ['int'], '123', Ok(123)),
        ('Optional', ['int'], None, Ok(None)),
    ),
    ids=repr,
)
def test_coerce_complex(
    span: Span,
    outer_identifier: str,
    inner_identifiers: List[str],
    value: object,
    result: Result[object, str],
) -> None:
    annotation = AnnotationWithVariants(
        identifier=outer_identifier,
        variants=[Annotation(identifier=inner_identifier, span=span) for inner_identifier in inner_identifiers],
        span=span,
    )
    checker = convert_ast_annotation_to_type_checker(annotation)

    if result.is_ok():
        assert checker.coerce(value) == result.unwrap()
    else:
        expected_exceptions: Tuple[Type[Exception], ...] = (ValueError, TypeError)
        with pytest.raises(expected_exceptions) as e:
            checker.coerce(value)

        assert e.match(result.unwrap_err())
