"""Tests for CountOver lowering in DruidQueryTransformer."""
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
from osprey.engine.conftest import CheckJsonOutputFunction
from osprey.engine.query_language import parse_query_to_validated_ast
from osprey.engine.query_language.ast_druid_translator import DruidQueryTransformer
from osprey.engine.query_language.tests.conftest import MakeRulesSourcesFunction
from osprey.engine.query_language.udfs.count_over import CountOver
from osprey.engine.udf.registry import UDFRegistry

# Validators and UDF registry setup for CountOver translator tests
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
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(CountOver)),
]


# Snapshot tests for all 13 cases (6 ops × 2 keying variants + AND filter case)


def test_count_over_gte_with_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId) >= 10 (with key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId) >= 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'"), ('UserId', "'123'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_gt_with_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=Endpoint == '/foo', window='10m', key=UserId) > 10 (with key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=Endpoint == '/foo', window='10m', key=UserId) > 10",
        make_rules_sources([('Endpoint', "'/foo'"), ('UserId', "'123'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_eq_with_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=UserLoginIp == '1.1.1.1' and Endpoint == '/foo', window='10m', key=UserId) == 10 (with key, AND predicate)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1' and Endpoint == '/foo', window='10m', key=UserId) == 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'"), ('Endpoint', "'/foo'"), ('UserId', "'123'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_neq_with_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=Endpoint == '/foo', window='10m', key=UserId) != 5 (with key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=Endpoint == '/foo', window='10m', key=UserId) != 5",
        make_rules_sources([('Endpoint', "'/foo'"), ('UserId', "'123'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_lte_with_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId) <= 10 (with key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId) <= 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'"), ('UserId', "'123'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_lt_with_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=Endpoint == '/foo', window='10m', key=UserId) < 10 (with key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=Endpoint == '/foo', window='10m', key=UserId) < 10",
        make_rules_sources([('Endpoint', "'/foo'"), ('UserId', "'123'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_gte_no_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') >= 10 (no key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') >= 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_gt_no_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=Endpoint == '/foo', window='10m') > 10 (no key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=Endpoint == '/foo', window='10m') > 10",
        make_rules_sources([('Endpoint', "'/foo'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_eq_no_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=UserLoginIp == '1.1.1.1' or Endpoint == '/foo', window='10m') == 10 (no key, OR predicate)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1' or Endpoint == '/foo', window='10m') == 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'"), ('Endpoint', "'/foo'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_neq_no_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=Endpoint == '/foo', window='10m') != 5 (no key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=Endpoint == '/foo', window='10m') != 5",
        make_rules_sources([('Endpoint', "'/foo'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_lte_no_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') <= 10 (no key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') <= 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_lt_no_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=Endpoint == '/foo', window='10m') < 10 (no key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=Endpoint == '/foo', window='10m') < 10",
        make_rules_sources([('Endpoint', "'/foo'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_with_and_filter(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(...) >= 10 and Country != 'US' - verifies AND-conjunct folding"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId) >= 10 and Country != 'US'",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'"), ('UserId', "'123'"), ('Country', "'someCountry'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_custom_datasource_name(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    """Callers can pass a real Druid datasource name (e.g. `events`)
    and get executable SQL with that name interpolated and quoted."""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId) >= 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'"), ('UserId', "'123'")]),
    )
    transformed_query = DruidQueryTransformer(
        validated_sources=validated_sources, datasource_name='events'
    ).transform()
    sql = transformed_query['sql']
    assert 'FROM "events"' in sql
    # Placeholder must not leak through when a real name was provided.
    assert 'FROM "datasource"' not in sql
    # Inner-subquery alias should still be present regardless of datasource name.
    assert ') AS __inner WHERE' in sql


def test_count_over_scan_output_wraps_with_time_bounds(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    """`output_mode='scan'` wraps the inner CountOver SQL with the outer time
    bound (Calcite needs `AS __t` on the outer FROM subquery) so the result
    is executable directly against Druid."""
    from datetime import datetime, timezone

    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId) >= 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'"), ('UserId', "'123'")]),
    )
    start = datetime(2026, 5, 20, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 5, 20, 1, 0, 0, tzinfo=timezone.utc)

    transformed_query = DruidQueryTransformer(
        validated_sources=validated_sources,
        datasource_name='events',
        output_mode='scan',
        time_bounds=(start, end),
    ).transform()
    sql = transformed_query['sql']

    # Outer wrap with __t alias and tz-stripped TIMESTAMP literals.
    assert ') AS __t WHERE' in sql
    assert "__time >= TIMESTAMP '2026-05-20 00:00:00'" in sql
    assert "__time < TIMESTAMP '2026-05-20 01:00:00'" in sql
    assert '+00:00' not in sql  # tz suffix must be stripped


def test_count_over_timeseries_output_wraps_with_time_floor(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    """`output_mode='timeseries'` wraps the inner CountOver SQL with
    `TIME_FLOOR + COUNT(*) GROUP BY` so the result is ready for a
    timeseries-shaped consumer."""
    from datetime import datetime, timezone

    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId) >= 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'"), ('UserId', "'123'")]),
    )
    start = datetime(2026, 5, 20, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 5, 20, 2, 0, 0, tzinfo=timezone.utc)

    transformed_query = DruidQueryTransformer(
        validated_sources=validated_sources,
        datasource_name='events',
        output_mode='timeseries',
        time_bounds=(start, end),
        granularity_period='PT1H',
    ).transform()
    sql = transformed_query['sql']

    assert "TIME_FLOOR(__time, 'PT1H') AS bucket" in sql
    assert 'COUNT(*) AS cnt' in sql
    assert 'GROUP BY 1 ORDER BY 1' in sql
    assert "__time >= TIMESTAMP '2026-05-20 00:00:00'" in sql
    assert "__time < TIMESTAMP '2026-05-20 02:00:00'" in sql


def test_count_over_self_join_engine_with_key(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    """`sql_engine='self_join'` emits self-join + GROUP BY + HAVING SQL that
    works on Druid 27+ where window functions are unavailable/buggy."""
    from datetime import datetime, timezone

    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserIp == '1.1.1.1', window='5m', key=UserId) >= 10",
        make_rules_sources([('UserIp', "'1.1.1.1'"), ('UserId', "'u1'")]),
    )
    start = datetime(2026, 5, 21, 11, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 5, 21, 13, 0, 0, tzinfo=timezone.utc)

    transformed = DruidQueryTransformer(
        validated_sources=validated_sources,
        datasource_name='events',
        sql_engine='self_join',
        output_mode='scan',
        time_bounds=(start, end),
    ).transform()
    sql = transformed['sql']

    # Shape: pre-filtered subquery per side, equi-join on the key, time-window
    # WHERE, GROUP BY + HAVING.
    assert 'INNER JOIN' in sql
    assert 't1.UserId = t2.UserId' in sql
    assert 't2.__time <= t1.__time' in sql
    assert 'TIMESTAMPADD(SECOND, -300, t1.__time)' in sql
    assert 'GROUP BY t1.__action_id, t1.__time' in sql
    assert 'HAVING COUNT(*) >= 10' in sql
    # Outer SCAN wrap with the request's time bound.
    assert ') AS __t' in sql
    assert "__time >= TIMESTAMP '2026-05-21 11:00:00'" in sql


def test_count_over_self_join_engine_no_key(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    """No-key CountOver uses a synthetic constant join key so the planner sees
    an equi-join instead of a bare cross-join."""
    from datetime import datetime, timezone

    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserIp == '1.1.1.1', window='5m') == 10",
        make_rules_sources([('UserIp', "'1.1.1.1'")]),
    )
    start = datetime(2026, 5, 21, 11, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 5, 21, 13, 0, 0, tzinfo=timezone.utc)

    sql = DruidQueryTransformer(
        validated_sources=validated_sources,
        datasource_name='events',
        sql_engine='self_join',
        output_mode='scan',
        time_bounds=(start, end),
    ).transform()['sql']

    assert '1 AS __k' in sql
    assert 't1.__k = t2.__k' in sql
    assert 'HAVING COUNT(*) = 10' in sql


def test_count_over_self_join_engine_timeseries(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    """For TIMESERIES output, the self-join engine wraps the qualifying __time
    values with TIME_FLOOR + COUNT GROUP BY."""
    from datetime import datetime, timezone

    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserIp == '1.1.1.1', window='5m', key=UserId) >= 10",
        make_rules_sources([('UserIp', "'1.1.1.1'"), ('UserId', "'u1'")]),
    )
    start = datetime(2026, 5, 21, 11, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 5, 21, 13, 0, 0, tzinfo=timezone.utc)

    sql = DruidQueryTransformer(
        validated_sources=validated_sources,
        datasource_name='events',
        sql_engine='self_join',
        output_mode='timeseries',
        time_bounds=(start, end),
        granularity_period='PT1H',
    ).transform()['sql']

    # Outer TIMESERIES wrap
    assert "TIME_FLOOR(__time, 'PT1H')" in sql
    assert 'COUNT(*) AS cnt' in sql
    assert 'GROUP BY 1 ORDER BY 1' in sql
    # Inner is the self-join HAVING
    assert 'INNER JOIN' in sql
    assert 'HAVING COUNT(*) >= 10' in sql


def test_count_over_self_join_engine_all_operators(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    """Each of the six CountOver comparators maps to the right HAVING op."""
    from datetime import datetime, timezone

    start = datetime(2026, 5, 21, 11, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 5, 21, 13, 0, 0, tzinfo=timezone.utc)
    cases = [
        ('>= 10', '>='),
        ('> 5', '>'),
        ('== 10', '='),
        ('!= 10', '<>'),
        ('<= 3', '<='),
        ('< 3', '<'),
    ]
    for query_tail, expected_op in cases:
        validated_sources = parse_query_to_validated_ast(
            f"CountOver(predicate=UserIp == '1.1.1.1', window='5m') {query_tail}",
            make_rules_sources([('UserIp', "'1.1.1.1'")]),
        )
        sql = DruidQueryTransformer(
            validated_sources=validated_sources,
            datasource_name='events',
            sql_engine='self_join',
            output_mode='scan',
            time_bounds=(start, end),
        ).transform()['sql']
        threshold = query_tail.split()[-1]
        assert f'HAVING COUNT(*) {expected_op} {threshold}' in sql, f'{query_tail}: {sql}'


def test_count_over_top_n_window_engine(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    """`output_mode='top_n'` (window engine) wraps the LAG-based inner SQL
    with `GROUP BY <dim> ORDER BY COUNT(*) DESC LIMIT N` for "top dimensions
    by burst count" queries."""
    from datetime import datetime, timezone

    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserIp == '1.1.1.1', window='5m', key=UserId) == 3",
        make_rules_sources([('UserIp', "'1.1.1.1'"), ('UserId', "'u1'")]),
    )
    start = datetime(2026, 5, 21, 11, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 5, 21, 13, 0, 0, tzinfo=timezone.utc)

    sql = DruidQueryTransformer(
        validated_sources=validated_sources,
        datasource_name='events',
        output_mode='top_n',
        time_bounds=(start, end),
        topn_dimension='UserId',
        topn_limit=100,
    ).transform()['sql']

    assert 'UserId AS __dim' in sql
    assert 'COUNT(*) AS cnt' in sql
    assert 'GROUP BY UserId' in sql
    assert 'ORDER BY cnt DESC' in sql
    assert 'LIMIT 100' in sql


def test_count_over_top_n_self_join_engine(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    """`output_mode='top_n'` on the self-join engine projects the dimension
    through the inner self-join, then aggregates and orders at the outer level."""
    from datetime import datetime, timezone

    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserIp == '1.1.1.1', window='5m', key=UserId) == 3",
        make_rules_sources([('UserIp', "'1.1.1.1'"), ('UserId', "'u1'")]),
    )
    start = datetime(2026, 5, 21, 11, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 5, 21, 13, 0, 0, tzinfo=timezone.utc)

    sql = DruidQueryTransformer(
        validated_sources=validated_sources,
        datasource_name='events',
        sql_engine='self_join',
        output_mode='top_n',
        time_bounds=(start, end),
        topn_dimension='UserId',
        topn_limit=100,
    ).transform()['sql']

    # Inner: self-join + HAVING + dimension projected
    assert 't1.UserId AS __dim' in sql
    assert 'GROUP BY t1.__time, t1.UserId' in sql
    assert 'HAVING COUNT(*) = 3' in sql
    # Outer: TopN aggregation + ordering + limit
    assert 'GROUP BY __dim' in sql
    assert 'ORDER BY cnt DESC' in sql
    assert 'LIMIT 100' in sql


def test_count_over_top_n_requires_dimension_and_limit(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    """Constructor validates that `output_mode='top_n'` is paired with both
    `topn_dimension` and `topn_limit`."""
    from datetime import datetime, timezone

    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserIp == '1.1.1.1', window='5m') >= 10",
        make_rules_sources([('UserIp', "'1.1.1.1'")]),
    )
    start = datetime(2026, 5, 21, 11, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 5, 21, 13, 0, 0, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match='topn_dimension'):
        DruidQueryTransformer(
            validated_sources=validated_sources,
            output_mode='top_n',
            time_bounds=(start, end),
            topn_limit=100,
        )
    with pytest.raises(ValueError, match='topn'):
        DruidQueryTransformer(
            validated_sources=validated_sources,
            output_mode='top_n',
            time_bounds=(start, end),
            topn_dimension='UserId',
        )


def test_count_over_engine_validation(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    """Constructor rejects unknown `sql_engine` values."""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserIp == '1.1.1.1', window='5m') >= 10",
        make_rules_sources([('UserIp', "'1.1.1.1'")]),
    )
    with pytest.raises(ValueError, match='sql_engine'):
        DruidQueryTransformer(validated_sources=validated_sources, sql_engine='bogus')


def test_count_over_output_mode_validation(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    """Constructor validates output_mode and required companion args eagerly,
    so callers don't get a confusing error mid-transform."""
    from datetime import datetime, timezone

    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') >= 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'")]),
    )
    start = datetime(2026, 5, 20, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 5, 20, 1, 0, 0, tzinfo=timezone.utc)

    # Unknown output_mode
    with pytest.raises(ValueError, match='output_mode'):
        DruidQueryTransformer(validated_sources=validated_sources, output_mode='bogus')

    # 'scan' requires time_bounds
    with pytest.raises(ValueError, match='time_bounds'):
        DruidQueryTransformer(validated_sources=validated_sources, output_mode='scan')

    # 'timeseries' requires both time_bounds and granularity_period
    with pytest.raises(ValueError, match='time_bounds'):
        DruidQueryTransformer(validated_sources=validated_sources, output_mode='timeseries')
    with pytest.raises(ValueError, match='granularity_period'):
        DruidQueryTransformer(
            validated_sources=validated_sources,
            output_mode='timeseries',
            time_bounds=(start, end),
        )


def _assert_valid_count_over_sql(transformed_query: Any) -> None:
    """Assert the transformed query contains valid CountOver SQL.

    Checks:
    - Type is 'sql'
    - SQL string is present
    - No doubled FROM clauses (check by counting FROM after first SELECT *, before first WHERE in parentheses)
    - No literal {operand_str} f-string placeholders
    - Balanced parentheses
    """
    assert isinstance(transformed_query, dict), f"Expected dict, got {type(transformed_query)}"
    assert transformed_query.get('type') == 'sql', f"Expected type='sql', got {transformed_query.get('type')}"

    sql = transformed_query.get('sql', '')
    assert isinstance(sql, str) and sql, "Expected non-empty SQL string"

    # Check for doubled FROM clause bug (Critical 1)
    # The pattern should be: SELECT * FROM (SELECT *, LAG(...) FROM datasource WHERE ...) WHERE ...
    # Not: SELECT * FROM (SELECT *, LAG(...) FROM __default FROM datasource WHERE ...) WHERE ...
    # So we look for the doubling pattern specifically
    assert ' FROM __default FROM ' not in sql.upper(), f"Found doubled FROM clause (FROM __default FROM). SQL: {sql}"
    # The translator emits the datasource name double-quoted (`FROM "<name>"`)
    # so that names containing `.` parse under Calcite. The default placeholder
    # `"datasource"` shows up here; callers passing a real name get e.g.
    # `FROM "events"`.
    assert 'FROM "datasource"' in sql, f"Expected 'FROM \"datasource\"' in SQL. SQL: {sql}"

    # Check for f-string bug (Critical 2) - literal {operand_str}
    assert '{operand_str}' not in sql, f"Found literal {{operand_str}} in SQL (f-string bug). SQL: {sql}"
    assert '{' not in sql or '}' not in sql, f"Found unresolved placeholder in SQL. SQL: {sql}"

    # Check parentheses are balanced
    paren_count = 0
    for char in sql:
        if char == '(':
            paren_count += 1
        elif char == ')':
            paren_count -= 1
        assert paren_count >= 0, f"Unbalanced parentheses in SQL (more closing than opening). SQL: {sql}"
    assert paren_count == 0, f"Unbalanced parentheses in SQL (unclosed). SQL: {sql}"



