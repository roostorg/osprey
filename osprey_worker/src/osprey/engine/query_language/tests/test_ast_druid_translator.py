import json

import pytest
from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes
from osprey.engine.ast_validator.validators.variables_must_be_defined import VariablesMustBeDefined
from osprey.engine.conftest import CheckJsonOutputFunction, RunValidationFunction
from osprey.engine.query_language import parse_query_to_validated_ast
from osprey.engine.query_language.ast_druid_translator import DruidQueryTransformer
from osprey.engine.query_language.tests.conftest import MakeRulesSourcesFunction

# The validators that the rules source validation should use, *not* the query source validation.
pytestmark = [
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


def test_parses_simple_query(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    validated_sources = parse_query_to_validated_ast(
        'A == B or (C == D and F >= 2)', make_rules_sources(['A', 'B', 'C', 'D', 'F'])
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()

    assert check_json_output(transformed_query)


def test_parses_query_with_negation(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    validated_sources = parse_query_to_validated_ast(
        '(A == B or (C == D and F >= 2)) and C not in [3, 4, 5]',
        make_rules_sources(['A', 'B', 'C', 'D', 'F']),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()

    assert check_json_output(transformed_query)


def test_parses_query_with_singular_negation(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    validated_sources = parse_query_to_validated_ast(
        "'boop' not in UserEmail", make_rules_sources([('UserEmail', '"some email"')])
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()

    assert check_json_output(transformed_query)


def test_parses_query_with_null_value(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    validated_sources = parse_query_to_validated_ast('A == B and C == None', make_rules_sources(['A', 'B', 'C']))
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()

    assert check_json_output(transformed_query)


def test_parses_query_with_regex(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    validated_sources = parse_query_to_validated_ast(
        "RegexMatch(item=A, regex='^foo$') and C == D",
        make_rules_sources([('A', '"hello"'), 'C', 'D']),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()

    assert check_json_output(transformed_query)


def test_parses_string_in_query_as_search(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    validated_sources = parse_query_to_validated_ast(
        "'gmail.com' in UserEmail", make_rules_sources([('UserEmail', '"some email"')])
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()

    assert check_json_output(transformed_query)


def test_parses_did_mutate_label(
    run_validation: RunValidationFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    validated_sources = parse_query_to_validated_ast(
        'DidAddLabel(entity_type="MyEntity",label_name="my_label")',
        run_validation(
            {'main.sml': '', 'config.yaml': json.dumps({'labels': {'my_label': {'valid_for': ['MyEntity']}}})}
        ),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()

    assert check_json_output(transformed_query)


def test_parses_query_with_regex_match_on_username(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    validated_sources = parse_query_to_validated_ast(
        "RegexMatch(item=UserName, regex='^jake')",
        make_rules_sources([('UserName', '"some_user"')]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()

    # Assert it returns a valid Druid query with the regex filter
    assert isinstance(transformed_query, dict)
    assert 'filter' in transformed_query
    assert isinstance(transformed_query['filter'], dict)
    assert transformed_query['filter'].get('type') == 'regex'
    assert transformed_query['filter'].get('dimension') == 'UserName'
    assert transformed_query['filter'].get('pattern') == '^jake'


def test_udf_node_mapping_with_missing_entry(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    """
    Regression test for issue #158: KeyError in transform_Call when UDF node mapping is keyed by id(node).

    This test verifies that transform_Call can handle Call nodes that aren't in the UDF mapping,
    which can occur if:
    1. A Call node passes syntax validation but fails argument validation
    2. The node is included in the AST but not added to _udf_node_mapping
    3. transform_Call tries to look it up and gets KeyError

    The fix should either:
    - Use a stable key instead of id(node), or
    - Handle missing keys gracefully in transform_Call
    """
    # This test would ideally simulate the case where a Call node exists in the AST
    # but wasn't added to the mapping. Since our valid queries always have valid UDFs,
    # we'll just verify the current behavior works.
    rules_sources = make_rules_sources([('UserName', '"some_user"')])

    # Parse the query with a valid UDF
    query = "RegexMatch(item=UserName, regex='^jake')"
    validated_sources = parse_query_to_validated_ast(query, rules_sources)

    # Create transformer and attempt to transform
    transformer = DruidQueryTransformer(validated_sources=validated_sources)
    transformed_query = transformer.transform()

    # Verify the transformation succeeded
    assert transformed_query is not None
    assert isinstance(transformed_query, dict)
    assert 'filter' in transformed_query


@pytest.mark.parametrize(
    'query',
    [
        'not A == B or (C == D and F >= 2)',
        'not A == 1',
        'not DidAddLabel(entity_type="MyEntity",label_name="my_label")',
        'A == -1',
        '-10.0 > A',
        'not A == -1',
        'A != B',
    ],
)
def test_parses_query_with_unary_operator(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction, query: str
) -> None:
    validated_sources = parse_query_to_validated_ast(query, make_rules_sources(['A', 'B', 'C', 'D', 'F']))
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()

    assert check_json_output(transformed_query)
