from typing import Any, Callable, List, Optional

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import CheckFailureFunction, ExecuteFunction, RunValidationFunction
from osprey.engine.stdlib.udfs.count_regex_matches import CountRegexMatches
from osprey.engine.udf.registry import UDFRegistry

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs, UniqueStoredNames]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(CountRegexMatches)),
]


@pytest.mark.parametrize(
    'patterns, target, expected',
    (
        # No patterns match
        (['foo', 'bar'], 'hello world', 0),
        # One of two matches
        (['foo', 'bar'], 'foo world', 1),
        # Both match
        (['foo', 'bar'], 'foo bar baz', 2),
        # Multiple hits within one pattern still count as 1
        (['foo'], 'foo foo foo', 1),
        # Distinct patterns that share a hit each contribute 1
        (['fo+', 'f.o'], 'foo', 2),
        # Anchored patterns
        (['^abc', 'xyz$'], 'abc and xyz', 2),
        (['^abc', 'xyz$'], 'zabc xyzx', 0),
        # Empty target with non-trivial patterns
        (['foo', 'bar'], '', 0),
        # Empty pattern matches anything (re.search with '' returns a match at pos 0)
        ([''], 'abc', 1),
    ),
)
def test_counts_matching_patterns(
    execute: ExecuteFunction, patterns: List[str], target: str, expected: int
) -> None:
    result = execute(
        f"""
    Count = CountRegexMatches(patterns={patterns!r}, target="{target}")
    """
    )
    assert result == {'Count': expected}


@pytest.mark.parametrize(
    'patterns, target, case_insensitive, expected',
    (
        # Default (case-sensitive)
        (['foo', 'BAR'], 'foo bar', None, 1),
        # Explicit case-sensitive
        (['foo', 'BAR'], 'foo bar', False, 1),
        # Case-insensitive applies to all patterns
        (['foo', 'BAR'], 'FOO bar', True, 2),
        (['foo', 'BAR'], 'FoO BaR', True, 2),
        # Insensitive but still no match
        (['quux'], 'FoO BaR', True, 0),
    ),
)
def test_can_be_case_insensitive(
    execute: ExecuteFunction,
    patterns: List[str],
    target: str,
    case_insensitive: Optional[bool],
    expected: int,
) -> None:
    extra_args = ''
    if case_insensitive is not None:
        extra_args = f', case_insensitive={case_insensitive}'
    result = execute(f'Count = CountRegexMatches(patterns={patterns!r}, target="{target}"{extra_args})')
    assert result == {'Count': expected}


def test_rejects_invalid_regex(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation('Foo = CountRegexMatches(patterns=["valid", "("], target="")')
