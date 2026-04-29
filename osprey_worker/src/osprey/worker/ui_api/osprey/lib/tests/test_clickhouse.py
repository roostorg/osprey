from datetime import UTC, datetime
from typing import Any, List
from unittest import mock

import pytest
from osprey.engine import shared_constants
from osprey.engine.query_language.filter_ir import (
    ArrayContainsFilter,
    ComparisonFilter,
    ComparisonOperator,
    ContainsFilter,
    FeatureRef,
    InFilter,
    LiteralValue,
)
from osprey.worker.ui_api.osprey.lib import clickhouse


def make_translator(
    monkeypatch: pytest.MonkeyPatch, feature_types: dict[str, type]
) -> tuple[clickhouse.ClickHouseFilterTranslator, clickhouse.ClickHouseSqlBuilder]:
    engine = mock.MagicMock()
    engine.get_post_execution_feature_name_to_value_type_mapping.return_value = feature_types
    engine.get_feature_name_to_entity_type_mapping.return_value = {}
    monkeypatch.setattr(clickhouse.ENGINE, 'instance', mock.MagicMock(return_value=engine))

    builder = clickhouse.ClickHouseSqlBuilder()
    return clickhouse.ClickHouseFilterTranslator(builder), builder


def make_backend(
    monkeypatch: pytest.MonkeyPatch, feature_types: dict[str, type]
) -> tuple[clickhouse.ClickHouseEventQueryBackend, Any]:
    engine = mock.MagicMock()
    engine.get_post_execution_feature_name_to_value_type_mapping.return_value = feature_types
    engine.get_feature_name_to_entity_type_mapping.return_value = {}
    monkeypatch.setattr(clickhouse.ENGINE, 'instance', mock.MagicMock(return_value=engine))

    client = mock.MagicMock()
    holder = mock.MagicMock()
    holder.client = client
    holder.database = 'osprey'
    holder.table = 'execution_results'
    holder.timeout_seconds = 30
    monkeypatch.setattr(clickhouse.CLICKHOUSE, 'instance', mock.MagicMock(return_value=holder))

    return clickhouse.ClickHouseEventQueryBackend(), client


def test_clickhouse_translates_string_contains(monkeypatch: pytest.MonkeyPatch) -> None:
    translator, builder = make_translator(monkeypatch, {'UserEmail': str})

    sql = translator.transform(ContainsFilter(feature=FeatureRef('UserEmail'), value=LiteralValue('gmail.com')))

    assert sql == (
        'positionCaseInsensitive(toString(JSONExtractString(raw_features, {param_0:String})), {param_1:String}) > 0'
    )
    assert builder.params == {'param_0': 'UserEmail', 'param_1': 'gmail.com'}


def test_clickhouse_translates_in_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    translator, builder = make_translator(monkeypatch, {'ActionName': str})

    sql = translator.transform(InFilter(feature=FeatureRef('ActionName'), values=('a', 'b')))

    assert sql == 'has({param_0:Array(String)}, action_name)'
    assert builder.params == {'param_0': ['a', 'b']}


def test_clickhouse_translates_array_udf_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    translator, builder = make_translator(monkeypatch, {shared_constants.VERDICT_DIMENSION_NAME: List[str]})

    sql = translator.transform(
        ArrayContainsFilter(
            feature=FeatureRef(shared_constants.VERDICT_DIMENSION_NAME),
            value=LiteralValue('reject'),
        )
    )

    assert sql == "has(JSONExtract(raw_features, {param_0:String}, 'Array(String)'), {param_1:String})"
    assert builder.params == {'param_0': '__verdicts', 'param_1': 'reject'}


def test_clickhouse_translates_typed_numeric_comparison(monkeypatch: pytest.MonkeyPatch) -> None:
    translator, builder = make_translator(monkeypatch, {'Score': int})

    sql = translator.transform(
        ComparisonFilter(
            left=FeatureRef('Score'),
            operator=ComparisonOperator.GREATER_THAN_EQUALS,
            right=LiteralValue(10),
        )
    )

    assert sql == (
        "(nullIf(JSONExtractRaw(raw_features, {param_0:String}), '') IS NOT NULL AND "
        'JSONExtractInt(raw_features, {param_0:String}) >= {param_1:Int64})'
    )
    assert builder.params == {'param_0': 'Score', 'param_1': 10}


def test_clickhouse_translates_typed_boolean_comparison(monkeypatch: pytest.MonkeyPatch) -> None:
    translator, builder = make_translator(monkeypatch, {'ContainsHello': bool})

    sql = translator.transform(
        ComparisonFilter(
            left=FeatureRef('ContainsHello'),
            operator=ComparisonOperator.EQUALS,
            right=LiteralValue(True),
        )
    )

    assert sql == (
        "(nullIf(JSONExtractRaw(raw_features, {param_0:String}), '') IS NOT NULL AND "
        'JSONExtractBool(raw_features, {param_0:String}) = {param_1:Bool})'
    )
    assert builder.params == {'param_0': 'ContainsHello', 'param_1': True}


def test_clickhouse_translates_typed_not_equals_to_include_missing_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    translator, builder = make_translator(monkeypatch, {'ContainsHello': bool})

    sql = translator.transform(
        ComparisonFilter(
            left=FeatureRef('ContainsHello'),
            operator=ComparisonOperator.NOT_EQUALS,
            right=LiteralValue(False),
        )
    )

    assert sql == (
        "(nullIf(JSONExtractRaw(raw_features, {param_0:String}), '') IS NULL OR "
        'JSONExtractBool(raw_features, {param_0:String}) != {param_1:Bool})'
    )
    assert builder.params == {'param_0': 'ContainsHello', 'param_1': False}


def test_clickhouse_preserves_literal_left_comparison(monkeypatch: pytest.MonkeyPatch) -> None:
    translator, builder = make_translator(monkeypatch, {'Score': int})

    sql = translator.transform(
        ComparisonFilter(
            left=LiteralValue(10),
            operator=ComparisonOperator.GREATER_THAN,
            right=FeatureRef('Score'),
        )
    )

    assert sql == (
        "(nullIf(JSONExtractRaw(raw_features, {param_0:String}), '') IS NOT NULL AND "
        '{param_1:Int64} > JSONExtractInt(raw_features, {param_0:String}))'
    )
    assert builder.params == {'param_0': 'Score', 'param_1': 10}


def test_clickhouse_rejects_unknown_feature_name(monkeypatch: pytest.MonkeyPatch) -> None:
    translator, _ = make_translator(monkeypatch, {'KnownFeature': str})

    with pytest.raises(ValueError, match='Unknown feature name'):
        translator.transform(
            ComparisonFilter(
                left=FeatureRef('BadFeature'),
                operator=ComparisonOperator.EQUALS,
                right=LiteralValue('value'),
            )
        )


def test_serialize_timestamp_normalizes_naive_datetimes_to_utc() -> None:
    assert clickhouse.serialize_timestamp(datetime(2026, 4, 8, 12, 0, 0)) == '2026-04-08T12:00:00+00:00'


def test_clickhouse_scan_encodes_cursor_with_action_id(monkeypatch: pytest.MonkeyPatch) -> None:
    backend, _ = make_backend(monkeypatch, {})
    timestamp = datetime(2026, 4, 8, 12, 0, tzinfo=UTC)

    monkeypatch.setattr(
        backend,
        '_execute_query',
        lambda sql, params: [
            {'action_id': 30, 'timestamp': timestamp},
            {'action_id': 20, 'timestamp': timestamp},
            {'action_id': 10, 'timestamp': timestamp},
        ],
    )

    result = backend.scan(
        clickhouse.PaginatedScanEventQuery(
            start=datetime(2026, 4, 8, 11, 0, tzinfo=UTC),
            end=datetime(2026, 4, 8, 13, 0, tzinfo=UTC),
            query_filter='',
            entity=None,
            limit=2,
        )
    )

    assert result.action_ids == [30, 20]
    assert result.next_page is not None
    assert clickhouse.decode_scan_cursor(result.next_page) == (timestamp, 20)


@pytest.mark.parametrize(
    ('order', 'expected_order_sql', 'expected_cursor_op'),
    [
        (clickhouse.Ordering.ASCENDING, 'ORDER BY timestamp ASC, action_id ASC', 'action_id >'),
        (clickhouse.Ordering.DESCENDING, 'ORDER BY timestamp DESC, action_id DESC', 'action_id <'),
    ],
)
def test_clickhouse_scan_uses_action_id_tiebreaker_in_cursor_predicate(
    monkeypatch: pytest.MonkeyPatch,
    order: clickhouse.Ordering,
    expected_order_sql: str,
    expected_cursor_op: str,
) -> None:
    backend, _ = make_backend(monkeypatch, {})
    timestamp = datetime(2026, 4, 8, 12, 0, tzinfo=UTC)
    captured: dict[str, Any] = {}

    def fake_execute(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        captured['sql'] = sql
        captured['params'] = params
        return [{'action_id': 10, 'timestamp': timestamp}]

    monkeypatch.setattr(backend, '_execute_query', fake_execute)

    backend.scan(
        clickhouse.PaginatedScanEventQuery(
            start=datetime(2026, 4, 8, 11, 0, tzinfo=UTC),
            end=datetime(2026, 4, 8, 13, 0, tzinfo=UTC),
            query_filter='',
            entity=None,
            limit=1,
            next_page=clickhouse.encode_scan_cursor(timestamp, 25),
            order=order,
        )
    )

    assert expected_order_sql in captured['sql']
    assert 'timestamp =' in captured['sql']
    assert expected_cursor_op in captured['sql']


def test_clickhouse_topn_rejects_non_zero_precision(monkeypatch: pytest.MonkeyPatch) -> None:
    backend, _ = make_backend(monkeypatch, {'Score': float})

    with pytest.raises(ValueError, match='do not support non-zero precision'):
        backend.topn(
            clickhouse.TopNEventQuery(
                start=datetime(2026, 4, 8, 11, 0, tzinfo=UTC),
                end=datetime(2026, 4, 8, 13, 0, tzinfo=UTC),
                query_filter='',
                entity=None,
                dimension='Score',
                limit=10,
                precision=0.1,
            )
        )
