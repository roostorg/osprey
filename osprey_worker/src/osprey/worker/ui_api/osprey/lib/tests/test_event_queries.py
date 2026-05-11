from unittest import mock

from osprey.engine.query_language.filter_ir import BooleanFilter, BooleanOperator
from osprey.worker.ui_api.osprey.lib import event_queries


def test_entity_filter_logs_warning_when_no_matching_features(caplog) -> None:
    engine = mock.MagicMock()
    engine.get_feature_name_to_entity_type_mapping.return_value = {}

    with mock.patch.object(event_queries.ENGINE, 'instance', return_value=engine):
        with caplog.at_level('WARNING'):
            filter_ir = event_queries.EntityFilter(id='123', type='User', feature_filters=['UserId']).to_filter_ir()

    assert filter_ir == BooleanFilter(operator=BooleanOperator.OR, fields=())
    assert 'Entity filter produced no matching features' in caplog.text


def test_event_query_aliases_match_compatibility_models() -> None:
    assert event_queries.BaseEventQuery is event_queries.BaseDruidQuery
    assert event_queries.TimeseriesEventQuery is event_queries.TimeseriesDruidQuery
    assert event_queries.GroupByApproximateCountEventQuery is event_queries.GroupByApproximateCountDruidQuery
    assert event_queries.TopNEventQuery is event_queries.TopNDruidQuery
    assert event_queries.PaginatedScanEventQuery is event_queries.PaginatedScanDruidQuery
