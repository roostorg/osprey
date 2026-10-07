import json
from types import SimpleNamespace

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
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.query_language import parse_query_to_validated_ast
from osprey.engine.query_language.ast_druid_translator import (
    DruidQueryTransformer,
    DruidQueryTransformException,
    DruidQueryUserError,
    get_druid_bound_query_props,
)
from osprey.engine.query_language.tests.conftest import MakeRulesSourcesFunction
from osprey.engine.query_language.udfs.registry import UDF_REGISTRY
from osprey.engine.query_language.udfs.registry import register as register_query_udf
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import QueryUdfBase, UDFBase

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
        "RegexMatch(target=A, pattern='^foo$') and C == D",
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


def test_comparing_two_features_with_unsupported_operator_raises_clear_error(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    # Regression test for https://github.com/roostorg/osprey/issues/439: comparing two features with
    # anything other than `==`/`!=` isn't caught by earlier validation, so this exception must be raised
    # with a message that will get surfaced to the user rather than propagating as an opaque 500.
    validated_sources = parse_query_to_validated_ast('A > B', make_rules_sources(['A', 'B']))

    with pytest.raises(DruidQueryUserError, match=r'`A > B` is not supported'):
        DruidQueryTransformer(validated_sources=validated_sources).transform()


def test_comparing_two_features_with_unsupported_operator_inside_a_nested_query(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    # Same as above, but confirms the error correctly identifies the one failing sub-comparison (not
    # some other node) and that message construction doesn't break when it's nested inside a larger
    # boolean expression rather than being the whole query.
    query = '(A == B and C > D) or (E != B and B < D)'
    validated_sources = parse_query_to_validated_ast(query, make_rules_sources(['A', 'B', 'C', 'D', 'E']))

    with pytest.raises(DruidQueryUserError, match=r'`C > D` is not supported'):
        DruidQueryTransformer(validated_sources=validated_sources).transform()


def test_internal_transformer_errors_are_not_user_errors() -> None:
    # Regression test: only the two genuinely user-reachable raise sites (tested above and below) raise
    # DruidQueryUserError. Everything else -- like an unrecognized comparator here, which shouldn't be
    # reachable through a query that's passed normal validation -- stays the base
    # DruidQueryTransformException, so it keeps propagating as an ordinary 500 (visible in Sentry/logs)
    # instead of being silently swallowed into a client-facing 400 that would never alert anyone.
    node = SimpleNamespace(comparator=SimpleNamespace())  # a comparator type get_druid_bound_query_props doesn't handle

    with pytest.raises(DruidQueryTransformException) as exc_info:
        get_druid_bound_query_props(node, 5)  # type: ignore[arg-type]

    assert not isinstance(exc_info.value, DruidQueryUserError)


class _BrokenQueryUdfArguments(ArgumentsBase):
    pass


class _BrokenQueryUdf(UDFBase[_BrokenQueryUdfArguments, bool]):
    """A UDF registered as queryable (e.g. by mistake) without subclassing QueryUdfBase.

    Regression fixture for https://github.com/roostorg/osprey/issues/439: this is the actual way
    `transform_Call`'s "not a QueryUdfBase" branch is reachable. Calling an unknown or rules-only
    function is *not* reachable this way — `ValidateCallKwargs` already rejects those with a clean
    `ValidationFailed` before query translation ever runs.
    """

    def execute(self, execution_context: ExecutionContext, arguments: _BrokenQueryUdfArguments) -> bool:
        return True


def test_registering_a_non_query_udf_base_udf_logs_and_skips_registration(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The `register` wrapper (not the underlying UDFRegistry.register used below) is what UDFs actually
    # use to opt into being queryable. It logs and skips a misconfigured UDF at registration time,
    # rather than either silently registering it (leaving the mistake to surface only when some query
    # happens to call it) or raising, which would crash worker startup over what's a narrow, query-only
    # feature gap and take down rule execution along with it.
    #
    # (The restore_rejected_udf_registrations autouse fixture undoes this test's contribution to
    # UDF_REGISTRY.rejected_registrations afterward, so it can't leak into test_no_udf_registrations_were_rejected.)
    with caplog.at_level('ERROR'):
        register_query_udf(_BrokenQueryUdf)

    assert '_BrokenQueryUdf' in caplog.text
    assert 'must subclass QueryUdfBase' in caplog.text
    assert UDF_REGISTRY.get('_BrokenQueryUdf') is None


def test_calling_a_udf_registered_without_query_udf_base_raises_clear_error(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    # Bypasses register()'s own guard (tested above) by writing directly into the registry's internal
    # dict, to exercise transform_Call's runtime check as a defense-in-depth backstop, in case a UDF
    # ever ends up in the registry some other way.
    UDF_REGISTRY._functions['_BrokenQueryUdf'] = _BrokenQueryUdf
    try:
        validated_sources = parse_query_to_validated_ast('_BrokenQueryUdf()', make_rules_sources([]))
        with pytest.raises(DruidQueryUserError, match='`_BrokenQueryUdf` is not a query function'):
            DruidQueryTransformer(validated_sources=validated_sources).transform()
    finally:
        del UDF_REGISTRY._functions['_BrokenQueryUdf']


class _AbstractQueryUdfArguments(ArgumentsBase):
    pass


class _AbstractQueryUdf(QueryUdfBase[_AbstractQueryUdfArguments, bool]):
    """Subclasses QueryUdfBase but never implements to_druid_query(), so it's still abstract.

    Regression fixture for https://github.com/roostorg/osprey/issues/515: `issubclass(func, QueryUdfBase)`
    alone passes for this, but ValidateCallKwargs would crash with an uncaught TypeError trying to
    instantiate it later -- the same class of bug #439 fixed, reached a different way.
    """


def test_registering_an_abstract_query_udf_base_subclass_logs_and_skips_registration(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level('ERROR'):
        # mypy already catches this mistake statically (as it should); this test is for the runtime
        # backstop, for whatever gets past static analysis.
        register_query_udf(_AbstractQueryUdf)  # type: ignore[type-abstract]

    assert '_AbstractQueryUdf' in caplog.text
    assert 'abstract' in caplog.text
    assert UDF_REGISTRY.get('_AbstractQueryUdf') is None
