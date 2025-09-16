import json
from datetime import datetime
from operator import itemgetter
from typing import Any, Dict, Mapping
from unittest import mock

import pytest
from flask import Flask, Response, url_for
from flask.testing import FlaskClient
from osprey.worker.ui_api.osprey.lib.druid import (
    BaseDruidQuery,
    PaginatedScanDruidQuery,
    TimeseriesDruidQuery,
    TopNDruidQuery,
)
from osprey.worker.ui_api.osprey.validators.events import BulkLabelTopNRequest
from pydruid.query import Query

_base_druid_query = BaseDruidQuery(start=datetime.now(), end=datetime.now(), query_filter='', entity=None)

config_a = {
    'main.sml': '',
    'config.yaml': json.dumps(
        {
            'acl': {
                'users': {
                    'local-dev@localhost': {'abilities': [{'name': 'CAN_VIEW_EVENTS_BY_ENTITY', 'allow_all': True}]}
                }
            }
        }
    ),
}

config_b = {
    'main.sml': '',
    'config.yaml': json.dumps(
        {
            'acl': {
                'users': {
                    'local-dev@localhost': {
                        'abilities': [
                            {'name': 'CAN_VIEW_EVENTS_BY_ENTITY', 'allow_all': True},
                            {
                                'name': 'CAN_VIEW_EVENTS_BY_ACTION',
                                'allow_specific': ['some_allowance_name', 'another_allowance_name'],
                            },
                        ]
                    },
                }
            }
        }
    ),
}


@pytest.fixture()
def model_url() -> Dict[str, Any]:
    return {'model': PaginatedScanDruidQuery(**_base_druid_query.dict()), 'url': 'events.scan_query'}


@pytest.fixture()
def fake_druid() -> Any:
    druid = mock.MagicMock()
    druid.datasource = 'osprey.execution_results'
    druid.client = mock.MagicMock()

    return druid


@pytest.fixture()
def mock_druid_client(fake_druid: Any) -> Any:
    with mock.patch('osprey.worker.ui_api.osprey.lib.druid.DRUID') as magic_mock:
        magic_mock.instance = mock.MagicMock(return_value=fake_druid)
        yield


@pytest.mark.parametrize(
    'model,url',
    [
        (TopNDruidQuery(dimension='fake', **_base_druid_query.dict()), 'events.topn_query'),
        (TimeseriesDruidQuery(granularity='fake', **_base_druid_query.dict()), 'events.timeseries_query'),
        (PaginatedScanDruidQuery(**_base_druid_query.dict()), 'events.scan_query'),
        (TopNDruidQuery(dimension='fake', **_base_druid_query.dict()), 'events.topn_query_csv'),
        (
            BulkLabelTopNRequest(
                dimension='fake',
                expected_entities=1,
                no_limit=False,
                label_name='',
                label_status='',
                label_reason='',
                **_base_druid_query.dict(),
            ),
            'events.topn_bulk_label',
        ),
    ],
)
def test_events_auth_reject_post(app: Flask, client: 'FlaskClient[Response]', model: BaseDruidQuery, url: str) -> None:
    res = client.post(url_for(url), content_type='application/json', data=model.json())
    assert res.status_code == 401, res.data


@pytest.mark.parametrize(
    'url,url_kwargs',
    [('events.get_event_data', {'event_id': 1})],
)
def test_events_auth_reject_get(
    app: Flask, client: 'FlaskClient[Response]', url: str, url_kwargs: Mapping['str', Any]
) -> None:
    res = client.get(url_for(url, **url_kwargs))
    assert res.status_code == 401, res.data


@pytest.mark.use_rules_sources(config_a)
def test_events_scan_request_missing_ability(
    app: Flask, client: 'FlaskClient[Response]', model_url: Dict[str, Any]
) -> None:
    res = client.post(url_for(model_url['url']), content_type='application/json', data=model_url['model'].json())
    assert res.status_code == 401
    assert res.data.decode('utf-8') == "User `local-dev@localhost` doesn't have ability `CAN_VIEW_EVENTS_BY_ACTION`"


# TODO: get druid local running again
@pytest.mark.use_rules_sources(config_b)
def test_events_scan_request(
    app: Flask,
    client: 'FlaskClient[Response]',
    model_url: Dict[str, Any],
    mock_druid_client: Any,
    fake_druid: Any,
) -> None:
    query = Query(query_dict={}, query_type='osprey.execution_results')  # type: ignore
    fake_druid.client._post.return_value = query
    query.result_json = '[]'

    res = client.post(url_for(model_url['url']), content_type='application/json', data=model_url['model'].json())
    args, _ = fake_druid.client._post.call_args
    sorted_fields = sorted(args[0].query_dict['filter']['fields'][0]['fields'], key=itemgetter('value'))
    assert sorted_fields == [
        {'type': 'selector', 'dimension': 'ActionName', 'value': 'another_allowance_name'},
        {'type': 'selector', 'dimension': 'ActionName', 'value': 'some_allowance_name'},
    ]
    assert res.status_code == 200
