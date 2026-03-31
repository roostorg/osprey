from collections.abc import Callable
from typing import Any

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import CheckFailureFunction, ExecuteFunction, RunValidationFunction
from osprey.engine.stdlib.udfs.regex_match import RegexMatch, RegexMatchMap
from osprey.engine.udf.registry import UDFRegistry

pytestmark: list[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs, UniqueStoredNames]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(RegexMatch, RegexMatchMap)),
]


@pytest.mark.parametrize(
    'pattern, target, should_match',
    (
        # Anchored
        ('^[abc]+$', 'abc', True),
        ('^[abc]+$', 'abcd', False),
        ('^[abc]+$', 'zabc', False),
        # Non-anchored
        ('[abc]+', 'abcd', True),
        ('[abc]+', 'zabcd', True),
        ('(foo|bar)', 'foo', True),
        ('(foo|bar)', 'free foo', True),
        ('(foo|bar)', 'claim bar free!', True),
        ('(foo|bar)', 'claim qux free!', False),
    ),
)
def test_returns_whether_the_string_matches(
    execute: ExecuteFunction, pattern: str, target: str, should_match: bool
) -> None:
    result = execute(
        f"""
    Match = RegexMatch(pattern="{pattern}", target="{target}")
    Match2 = RegexMatchMap(pattern="{pattern}", target=["hi", "{target}"])
    """
    )
    assert result == {'Match': should_match, 'Match2': should_match}


@pytest.mark.parametrize(
    'pattern, target, should_match',
    (
        # Anchored
        ('^[abc]+$', 'abc', True),
        ('^[abc]+$', 'abcd', False),
        ('^[abc]+$', 'zabc', False),
        # Non-anchored
        ('[abc]+', 'abcd', True),
        ('[abc]+', 'zabcd', True),
        ('(foo|bar)', 'foo', True),
        ('(foo|bar)', 'free foo', True),
        ('(foo|bar)', 'claim bar free!', True),
        ('(foo|bar)', 'claim qux free!', False),
    ),
)
def test_regex_map_all_and_any(execute: ExecuteFunction, pattern: str, target: str, should_match: bool) -> None:
    result = execute(
        f"""
        Match_all_true = RegexMatchMap(pattern="{pattern}", target=["{target}", "{target}"], mode="all")
        Match_any_true = RegexMatchMap(pattern="{pattern}", target=["hi", "{target}"], mode="any")

        Match_all_false = RegexMatchMap(pattern="{pattern}", target=["xxx", "{target}"], mode="all")
        Match_any_false = RegexMatchMap(pattern="{pattern}", target=["xxx", "ho"], mode="any")
        """
    )
    assert result == {
        'Match_all_false': False,
        'Match_all_true': should_match,
        'Match_any_false': False,
        'Match_any_true': should_match,
    }, f"incorrect for /{pattern}/ and '{target}'"


@pytest.mark.parametrize(
    'pattern, target, case_insensitive, should_match',
    (
        # Default
        ('[abc]+', 'abc', None, True),
        ('[abc]+', 'ABC', None, False),
        # Sensitive
        ('[abc]+', 'abc', False, True),
        ('[abc]+', 'ABC', False, False),
        # Insensitive
        ('[abc]+', 'abc', True, True),
        ('[abc]+', 'ABC', True, True),
    ),
)
def test_can_be_case_insensitive(
    execute: ExecuteFunction, pattern: str, target: str, case_insensitive: bool | None, should_match: bool
) -> None:
    extra_args = ''
    if case_insensitive is not None:
        extra_args = f'case_insensitive={case_insensitive}'
    result = execute(f'Match = RegexMatch(pattern="{pattern}", target="{target}", {extra_args})')
    assert result == {'Match': should_match}


def test_rejects_invalid_regex(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation('Foo = RegexMatch(pattern="(", target="")')
