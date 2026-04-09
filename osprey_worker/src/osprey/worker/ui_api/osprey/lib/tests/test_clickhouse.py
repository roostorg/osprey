from datetime import datetime
from typing import List
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
    monkeypatch.setattr(clickhouse.ENGINE, 'instance', mock.MagicMock(return_value=engine))

    builder = clickhouse.ClickHouseSqlBuilder()
    return clickhouse.ClickHouseFilterTranslator(builder), builder


def test_clickhouse_translates_string_contains(monkeypatch: pytest.MonkeyPatch) -> None:
    translator, builder = make_translator(monkeypatch, {'UserEmail': str})

    sql = translator.transform(ContainsFilter(feature=FeatureRef('UserEmail'), value=LiteralValue('gmail.com')))

    assert sql == (
        "positionCaseInsensitive(toString(JSONExtractString(raw_features, 'UserEmail')), {param_0:String}) > 0"
    )
    assert builder.params == {'param_0': 'gmail.com'}


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

    assert sql == "has(JSONExtract(raw_features, '__verdicts', 'Array(String)'), {param_0:String})"
    assert builder.params == {'param_0': 'reject'}


def test_clickhouse_translates_typed_numeric_comparison(monkeypatch: pytest.MonkeyPatch) -> None:
    translator, builder = make_translator(monkeypatch, {'Score': int})

    sql = translator.transform(
        ComparisonFilter(
            left=FeatureRef('Score'),
            operator=ComparisonOperator.GREATER_THAN_EQUALS,
            right=LiteralValue(10),
        )
    )

    assert sql == "JSONExtractInt(raw_features, 'Score') >= {param_0:Int64}"
    assert builder.params == {'param_0': 10}


def test_clickhouse_translates_typed_boolean_comparison(monkeypatch: pytest.MonkeyPatch) -> None:
    translator, builder = make_translator(monkeypatch, {'ContainsHello': bool})

    sql = translator.transform(
        ComparisonFilter(
            left=FeatureRef('ContainsHello'),
            operator=ComparisonOperator.EQUALS,
            right=LiteralValue(True),
        )
    )

    assert sql == "JSONExtractBool(raw_features, 'ContainsHello') = {param_0:Bool}"
    assert builder.params == {'param_0': True}


def test_clickhouse_preserves_literal_left_comparison(monkeypatch: pytest.MonkeyPatch) -> None:
    translator, builder = make_translator(monkeypatch, {'Score': int})

    sql = translator.transform(
        ComparisonFilter(
            left=LiteralValue(10),
            operator=ComparisonOperator.GREATER_THAN,
            right=FeatureRef('Score'),
        )
    )

    assert sql == "{param_0:Int64} > JSONExtractInt(raw_features, 'Score')"
    assert builder.params == {'param_0': 10}


def test_serialize_timestamp_normalizes_naive_datetimes_to_utc() -> None:
    assert clickhouse.serialize_timestamp(datetime(2026, 4, 8, 12, 0, 0)) == '2026-04-08T12:00:00+00:00'
