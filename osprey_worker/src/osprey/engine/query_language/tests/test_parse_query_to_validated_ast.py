from typing import Any, Callable, List

import pytest
from osprey.engine.query_language import parse_query_to_validated_ast

from ...conftest import CheckFailureFunction, RunValidationFunction
from .conftest import MakeRulesSourcesFunction

# The validators and UDFs that the rules source validation should use, *not* the query source validation.
pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_standard_rules_validators(),
    pytest.mark.use_osprey_stdlib(),
]


def test_only_one_statement(make_rules_sources: MakeRulesSourcesFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        parse_query_to_validated_ast(
            'A == B or (C == D and F >= 2); Foo = Bar', make_rules_sources(['A', 'B', 'C', 'D', 'F'])
        )


def test_disallows_nodes_not_explicitly_allowed(
    make_rules_sources: MakeRulesSourcesFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        parse_query_to_validated_ast("A == f'{B}'", make_rules_sources([('A', '"abc"'), 'B']))


def test_validates_names_suggests_valid_name(
    make_rules_sources: MakeRulesSourcesFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        parse_query_to_validated_ast("UserNaem == 'jake'", make_rules_sources(['UserName', 'UserEmail']))


def test_validates_names_with_wildly_invalid_name(
    make_rules_sources: MakeRulesSourcesFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        parse_query_to_validated_ast("KLJHaflLasfkL == 'jake'", make_rules_sources(['UserName', 'UserEmail']))


def test_disallows_unknown_call_nodes(
    make_rules_sources: MakeRulesSourcesFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        parse_query_to_validated_ast('Foo()', make_rules_sources(['Foo']))


def test_validates_static_types(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    rules_source = run_validation(
        """
        Foo = 1
        Bar = 'hello'
        """
    )
    with check_failure():
        parse_query_to_validated_ast('(Foo > Bar) in (1 or 2)', rules_source)


def test_static_types_are_converted_post_execution(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    sources = run_validation(
        """
        Foo = Entity(type="Name", id="some name")
        Bar = Entity(type="User", id=123)
        ARule = Rule(when_all=[True], description="")
        AnOptional: Optional[bool] = True
        """
    )
    parse_query_to_validated_ast('Foo in "hello"', sources)
    parse_query_to_validated_ast('Bar > 456', sources)
    parse_query_to_validated_ast('ARule or False', sources)
    parse_query_to_validated_ast('AnOptional == None', sources)
    with check_failure():
        parse_query_to_validated_ast('Foo > Bar', sources)


@pytest.mark.parametrize(
    'query',
    ['"thing1" == "thing2"', '1 == 2', 'True == False', 'True and True', 'True and False and True'],
)
def test_binary_comparison_must_contain_identifier(
    make_rules_sources: MakeRulesSourcesFunction, check_failure: CheckFailureFunction, query: str
) -> None:
    with check_failure():
        parse_query_to_validated_ast(query, make_rules_sources(['UserName', 'UserEmail']))


def test_query_contains_more_than_literal_or_name(
    make_rules_sources: MakeRulesSourcesFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        parse_query_to_validated_ast('1', make_rules_sources(['UserName', 'UserEmail']))


@pytest.mark.parametrize(
    'query',
    ['not 1', 'not False', 'UserName == 1 and not 2'],
)
def test_unary_operator_query_validation_failure(
    make_rules_sources: MakeRulesSourcesFunction, check_failure: CheckFailureFunction, query: str
) -> None:
    with check_failure():
        parse_query_to_validated_ast(query, make_rules_sources(['UserName', 'UserEmail']))


@pytest.mark.parametrize(
    'query',
    [
        'not UserName == 1',
        'UserName == 1 and not UserEmail == 1',
        'not (UserEmail == 1 or UserEmail == 1)',
        'not not UserEmail == 1',
        '-UserName == 1',
        '-(-UserName) == -1',
        '(-UserName >= -UserEmail)',
        '-UserEmail',
    ],
)
def test_unary_operator_query_validation_success(
    make_rules_sources: MakeRulesSourcesFunction, check_failure: CheckFailureFunction, query: str
) -> None:
    parse_query_to_validated_ast(query, make_rules_sources(['UserName', 'UserEmail']))
