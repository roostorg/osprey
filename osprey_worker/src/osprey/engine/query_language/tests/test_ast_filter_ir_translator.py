import pytest
from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes
from osprey.engine.ast_validator.validators.variables_must_be_defined import VariablesMustBeDefined
from osprey.engine.query_language import parse_query_to_validated_ast
from osprey.engine.query_language.ast_filter_ir_translator import FilterIrTransformer
from osprey.engine.query_language.filter_ir import (
    BooleanFilter,
    BooleanOperator,
    ComparisonFilter,
    ComparisonOperator,
    ContainsFilter,
    FeatureRef,
    InFilter,
    LiteralValue,
    NotFilter,
    RegexFilter,
)
from osprey.engine.query_language.tests.conftest import MakeRulesSourcesFunction

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


def test_builds_filter_ir_for_boolean_and_comparison(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    validated_sources = parse_query_to_validated_ast(
        'A == B or (C == D and F >= 2)', make_rules_sources(['A', 'B', 'C', 'D', 'F'])
    )

    filter_ir = FilterIrTransformer(validated_sources=validated_sources).transform()

    assert filter_ir == BooleanFilter(
        operator=BooleanOperator.OR,
        fields=(
            ComparisonFilter(left=FeatureRef('A'), operator=ComparisonOperator.EQUALS, right=FeatureRef('B')),
            BooleanFilter(
                operator=BooleanOperator.AND,
                fields=(
                    ComparisonFilter(left=FeatureRef('C'), operator=ComparisonOperator.EQUALS, right=FeatureRef('D')),
                    ComparisonFilter(
                        left=FeatureRef('F'),
                        operator=ComparisonOperator.GREATER_THAN_EQUALS,
                        right=LiteralValue(2),
                    ),
                ),
            ),
        ),
    )


def test_builds_filter_ir_for_contains_and_not_in(make_rules_sources: MakeRulesSourcesFunction) -> None:
    validated_sources = parse_query_to_validated_ast(
        "'gmail.com' in UserEmail and C not in [3, 4, 5]",
        make_rules_sources([('UserEmail', '"some email"'), 'C']),
    )

    filter_ir = FilterIrTransformer(validated_sources=validated_sources).transform()

    assert filter_ir == BooleanFilter(
        operator=BooleanOperator.AND,
        fields=(
            ContainsFilter(feature=FeatureRef('UserEmail'), value=LiteralValue('gmail.com')),
            NotFilter(field=InFilter(feature=FeatureRef('C'), values=(3, 4, 5))),
        ),
    )


def test_builds_filter_ir_for_regex_udf(make_rules_sources: MakeRulesSourcesFunction) -> None:
    validated_sources = parse_query_to_validated_ast(
        "RegexMatch(item=A, regex='^foo$')",
        make_rules_sources([('A', '"hello"')]),
    )

    filter_ir = FilterIrTransformer(validated_sources=validated_sources).transform()

    assert filter_ir == RegexFilter(feature=FeatureRef('A'), pattern='^foo$')


def test_preserves_literal_side_for_binary_comparison(make_rules_sources: MakeRulesSourcesFunction) -> None:
    validated_sources = parse_query_to_validated_ast('10.0 > A', make_rules_sources(['A']))

    filter_ir = FilterIrTransformer(validated_sources=validated_sources).transform()

    assert filter_ir == ComparisonFilter(
        left=LiteralValue(10.0),
        operator=ComparisonOperator.GREATER_THAN,
        right=FeatureRef('A'),
    )


@pytest.mark.parametrize(
    ('query', 'expected'),
    [
        (
            'ContainsHello == true',
            ComparisonFilter(
                left=FeatureRef('ContainsHello'),
                operator=ComparisonOperator.EQUALS,
                right=LiteralValue(True),
            ),
        ),
        (
            'ContainsHello == false',
            ComparisonFilter(
                left=FeatureRef('ContainsHello'),
                operator=ComparisonOperator.EQUALS,
                right=LiteralValue(False),
            ),
        ),
        (
            'none == OptionalValue',
            ComparisonFilter(
                left=LiteralValue(None),
                operator=ComparisonOperator.EQUALS,
                right=FeatureRef('OptionalValue'),
            ),
        ),
    ],
)
def test_builds_filter_ir_for_lowercase_literal_names(
    make_rules_sources: MakeRulesSourcesFunction, query: str, expected: ComparisonFilter
) -> None:
    validated_sources = parse_query_to_validated_ast(
        query,
        make_rules_sources(
            [
                ('ContainsHello', 'True'),
                ('OptionalValue', 'None'),
            ]
        ),
    )

    filter_ir = FilterIrTransformer(validated_sources=validated_sources).transform()

    assert filter_ir == expected
