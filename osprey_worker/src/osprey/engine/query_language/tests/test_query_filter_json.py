from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes
from osprey.engine.ast_validator.validators.variables_must_be_defined import VariablesMustBeDefined
from osprey.engine.query_language.query_filter_json import validate_query_filter_to_json
from osprey.engine.query_language.tests.conftest import MakeRulesSourcesFunction

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_standard_rules_validators(),
    pytest.mark.use_validators(
        [
            UniqueStoredNames,
            ValidateStaticTypes,
            ValidateCallKwargs,
            ImportsMustNotHaveCycles,
            ValidateDynamicCallsHaveAnnotatedRValue,
            VariablesMustBeDefined,
        ]
    ),
]


_STR_FEATURE = ('UserName', '"default"')
_INT_FEATURE = ('ActionCount', '0')


def test_binary_comparison_success(make_rules_sources: MakeRulesSourcesFunction) -> None:
    result = validate_query_filter_to_json('UserName == "alice"', make_rules_sources([_STR_FEATURE]))

    assert result['errors'] == []
    assert result['ast'] == {
        'kind': 'Root',
        'span': {'start': {'line': 1, 'col': 1}},
        'expression': {
            'kind': 'BinaryComparison',
            'span': {'start': {'line': 1, 'col': 1}},
            'comparator': 'Equals',
            'left': {
                'kind': 'Name',
                'span': {'start': {'line': 1, 'col': 1}},
                'identifier': 'UserName',
            },
            'right': {
                'kind': 'String',
                'span': {'start': {'line': 1, 'col': 13}},
                'value': 'alice',
            },
        },
    }


@pytest.mark.parametrize(
    ('comparator_source', 'expected_kind'),
    [
        ('==', 'Equals'),
        ('!=', 'NotEquals'),
        ('<', 'LessThan'),
        ('<=', 'LessThanEquals'),
        ('>', 'GreaterThan'),
        ('>=', 'GreaterThanEquals'),
    ],
)
def test_comparator_kinds(
    make_rules_sources: MakeRulesSourcesFunction, comparator_source: str, expected_kind: str
) -> None:
    result = validate_query_filter_to_json(
        f'ActionCount {comparator_source} 1', make_rules_sources([_INT_FEATURE])
    )
    assert result['errors'] == []
    assert result['ast'] is not None
    assert result['ast']['expression']['comparator'] == expected_kind


def test_in_and_not_in(make_rules_sources: MakeRulesSourcesFunction) -> None:
    in_result = validate_query_filter_to_json('"alice" in UserName', make_rules_sources([_STR_FEATURE]))
    assert in_result['errors'] == []
    assert in_result['ast'] is not None
    assert in_result['ast']['expression']['comparator'] == 'In'

    not_in_result = validate_query_filter_to_json(
        '"alice" not in UserName', make_rules_sources([_STR_FEATURE])
    )
    assert not_in_result['errors'] == []
    assert not_in_result['ast'] is not None
    assert not_in_result['ast']['expression']['comparator'] == 'NotIn'


def test_boolean_operation_and(make_rules_sources: MakeRulesSourcesFunction) -> None:
    result = validate_query_filter_to_json(
        'UserName == "a" and ActionCount > 5', make_rules_sources([_STR_FEATURE, _INT_FEATURE])
    )
    assert result['errors'] == []
    expr = result['ast']['expression']
    assert expr['kind'] == 'BooleanOperation'
    assert expr['operand'] == 'And'
    assert len(expr['values']) == 2
    assert all(v['kind'] == 'BinaryComparison' for v in expr['values'])


def test_boolean_operation_or_flattens(make_rules_sources: MakeRulesSourcesFunction) -> None:
    # `A or B or C` flattens into a single BooleanOperation with three values,
    # matching Python's own ast.BoolOp shape.
    result = validate_query_filter_to_json(
        'UserName == "a" or UserName == "b" or UserName == "c"', make_rules_sources([_STR_FEATURE])
    )
    assert result['errors'] == []
    expr = result['ast']['expression']
    assert expr['kind'] == 'BooleanOperation'
    assert expr['operand'] == 'Or'
    assert len(expr['values']) == 3


def test_unary_not(make_rules_sources: MakeRulesSourcesFunction) -> None:
    result = validate_query_filter_to_json('not ActionCount == 1', make_rules_sources([_INT_FEATURE]))
    assert result['errors'] == []
    expr = result['ast']['expression']
    assert expr['kind'] == 'UnaryOperation'
    assert expr['operator'] == 'Not'
    assert expr['operand']['kind'] == 'BinaryComparison'


def test_unary_usub(make_rules_sources: MakeRulesSourcesFunction) -> None:
    result = validate_query_filter_to_json('-ActionCount == -1', make_rules_sources([_INT_FEATURE]))
    assert result['errors'] == []
    expr = result['ast']['expression']
    assert expr['kind'] == 'BinaryComparison'
    assert expr['left']['kind'] == 'UnaryOperation'
    assert expr['left']['operator'] == 'USub'


def test_list_literal(make_rules_sources: MakeRulesSourcesFunction) -> None:
    # `x in ["a", "b", "c"]` — the list is a literal on the right, the feature
    # on the left. Osprey's grammar for `in` normalises these into a
    # BinaryComparison; the serializer preserves the shape.
    result = validate_query_filter_to_json(
        'UserName in ["a", "b", "c"]', make_rules_sources([_STR_FEATURE])
    )
    assert result['errors'] == []
    expr = result['ast']['expression']
    assert expr['kind'] == 'BinaryComparison'
    list_side = expr['right'] if expr['right']['kind'] == 'List' else expr['left']
    assert list_side['kind'] == 'List'
    assert [item['value'] for item in list_side['items']] == ['a', 'b', 'c']


def test_boolean_and_none_literals(make_rules_sources: MakeRulesSourcesFunction) -> None:
    bool_result = validate_query_filter_to_json(
        'IsVerified == True', make_rules_sources([('IsVerified', 'True')])
    )
    assert bool_result['errors'] == []
    assert bool_result['ast']['expression']['right'] == {
        'kind': 'Boolean',
        'span': {'start': {'line': 1, 'col': 15}},
        'value': True,
    }

    none_result = validate_query_filter_to_json(
        'Nullable == None', make_rules_sources([('Nullable', 'None')])
    )
    assert none_result['errors'] == []
    assert none_result['ast']['expression']['right']['kind'] == 'None'


def test_unknown_feature_is_validation_error(make_rules_sources: MakeRulesSourcesFunction) -> None:
    result = validate_query_filter_to_json('Unknown == "x"', make_rules_sources([_STR_FEATURE]))

    assert result['ast'] is None
    assert len(result['errors']) >= 1
    assert all(e['kind'] == 'validation' for e in result['errors'])


def test_all_literal_comparison_is_validation_error(make_rules_sources: MakeRulesSourcesFunction) -> None:
    result = validate_query_filter_to_json('1 == 2', make_rules_sources([_INT_FEATURE]))

    assert result['ast'] is None
    assert len(result['errors']) >= 1
    assert all(e['kind'] == 'validation' for e in result['errors'])


def test_syntax_error_is_reported(make_rules_sources: MakeRulesSourcesFunction) -> None:
    # `a b` — invalid expression syntax (two names with no operator). Python's
    # parser raises SyntaxError which osprey surfaces as OspreySyntaxError.
    result = validate_query_filter_to_json('UserName UserName', make_rules_sources([_STR_FEATURE]))

    assert result['ast'] is None
    assert len(result['errors']) == 1
    assert result['errors'][0]['kind'] == 'syntax'


def test_span_columns_are_relative_to_user_input(make_rules_sources: MakeRulesSourcesFunction) -> None:
    # The `Query = ` prefix osprey adds internally is 8 characters. The user
    # sees their source starting at col 1, so `UserName` should report as
    # starting at col 1 even though osprey internally sees it at col 9.
    result = validate_query_filter_to_json('UserName == "x"', make_rules_sources([_STR_FEATURE]))
    name_span = result['ast']['expression']['left']['span']
    assert name_span == {'start': {'line': 1, 'col': 1}}


def test_error_hint_is_null_when_absent(make_rules_sources: MakeRulesSourcesFunction) -> None:
    # Ensure the `hint: None` JSON field is present (not omitted) when the
    # underlying error carried no hint.
    result = validate_query_filter_to_json('UserName UserName', make_rules_sources([_STR_FEATURE]))
    assert 'hint' in result['errors'][0]
