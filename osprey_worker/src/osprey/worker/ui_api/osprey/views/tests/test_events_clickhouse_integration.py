import csv
import io
import json
import os
from datetime import UTC, datetime
from typing import Any, Callable, Dict, Iterator

import pytest
from flask import Flask, Response, url_for
from flask.testing import FlaskClient
from osprey.worker._stdlibplugin.execution_result_store_chooser import get_rules_execution_result_storage_backend
from osprey.worker.lib.storage import ExecutionResultStorageBackendType
from osprey.worker.ui_api.osprey.singletons import CLICKHOUSE

RUN_CLICKHOUSE_INTEGRATION_TESTS = os.environ.get('RUN_CLICKHOUSE_INTEGRATION_TESTS', '').lower() == 'true'
pytestmark = pytest.mark.skipif(
    not RUN_CLICKHOUSE_INTEGRATION_TESTS,
    reason='ClickHouse integration tests require docker-compose.test.clickhouse.yaml',
)

CONFIG_ALLOW_ALL = {
    'main.sml': """
        UserId: Entity[str] = EntityJson(type="User", path="$.user_id", coerce_type=True)
        EventType: Entity[str] = EntityJson(type="EventType", path="$.event_type", coerce_type=True)
        ActionName = GetActionName()
        PostText: str = JsonData(path="$.post.text")
        ContainsHello: bool = TextContains(text=PostText, phrase="hello")
    """,
    'config.yaml': json.dumps(
        {
            'acl': {
                'users': {
                    'local-dev@localhost': {
                        'abilities': [
                            {'name': 'CAN_VIEW_EVENTS_BY_ENTITY', 'allow_all': True},
                            {'name': 'CAN_VIEW_EVENTS_BY_ACTION', 'allow_all': True},
                            {'name': 'CAN_VIEW_ACTION_DATA', 'allow_all': True},
                            {'name': 'CAN_VIEW_FEATURE_DATA', 'allow_all': True},
                        ]
                    }
                }
            }
        }
    ),
}

CONFIG_ACTION_FILTERED = {
    'main.sml': CONFIG_ALLOW_ALL['main.sml'],
    'config.yaml': json.dumps(
        {
            'acl': {
                'users': {
                    'local-dev@localhost': {
                        'abilities': [
                            {'name': 'CAN_VIEW_EVENTS_BY_ENTITY', 'allow_all': True},
                            {'name': 'CAN_VIEW_EVENTS_BY_ACTION', 'allow_specific': ['create_post']},
                            {'name': 'CAN_VIEW_ACTION_DATA', 'allow_all': True},
                            {'name': 'CAN_VIEW_FEATURE_DATA', 'allow_all': True},
                        ]
                    }
                }
            }
        }
    ),
}


@pytest.fixture(autouse=True)
def clear_clickhouse_table(app: Flask) -> Iterator[None]:
    client = CLICKHOUSE.instance().client
    client.command('TRUNCATE TABLE execution_results')
    yield
    client.command('TRUNCATE TABLE execution_results')


@pytest.fixture()
def seed_event(app: Flask) -> Callable[..., Dict[str, Any]]:
    clickhouse_client = CLICKHOUSE.instance().client
    storage_backend = get_rules_execution_result_storage_backend(ExecutionResultStorageBackendType.MINIO)
    assert storage_backend is not None

    def _seed_event(
        *,
        action_id: int,
        timestamp: datetime,
        action_name: str,
        action_data: Dict[str, Any],
        extracted_features: Dict[str, Any],
    ) -> Dict[str, Any]:
        extracted_features_json = json.dumps(extracted_features)
        storage_backend.insert(
            action_id=action_id,
            extracted_features_json=extracted_features_json,
            error_traces_json='[]',
            timestamp=timestamp,
            action_data_json=json.dumps(action_data),
        )
        clickhouse_client.insert(
            table='execution_results',
            data=[[timestamp, action_id, action_name, extracted_features_json]],
            column_names=['timestamp', 'action_id', 'action_name', 'raw_features'],
            database='osprey',
        )
        return extracted_features

    return _seed_event


@pytest.mark.use_rules_sources(CONFIG_ALLOW_ALL)
def test_clickhouse_scan_and_event_detail(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    current_timestamp = datetime(2026, 4, 8, 12, 0, tzinfo=UTC)
    ignored_timestamp = datetime(2026, 4, 8, 12, 5, tzinfo=UTC)

    create_post_action = {
        'user_id': 'user_scan',
        'event_type': 'create_post',
        'post': {'text': 'hello from clickhouse integration test'},
    }
    delete_post_action = {
        'user_id': 'user_scan',
        'event_type': 'delete_post',
        'post': {'text': 'ignored'},
    }

    seed_event(
        action_id=101,
        timestamp=current_timestamp,
        action_name='create_post',
        action_data=create_post_action,
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_scan',
            'EventType': 'create_post',
            'PostText': 'hello from clickhouse integration test',
            'ContainsHello': True,
            '__action_id': 101,
            '__error_count': 0,
            '__timestamp': current_timestamp.isoformat(),
        },
    )
    seed_event(
        action_id=102,
        timestamp=ignored_timestamp,
        action_name='delete_post',
        action_data=delete_post_action,
        extracted_features={
            'ActionName': 'delete_post',
            'UserId': 'user_scan',
            'EventType': 'delete_post',
            'PostText': 'ignored',
            'ContainsHello': False,
            '__action_id': 102,
            '__error_count': 0,
            '__timestamp': ignored_timestamp.isoformat(),
        },
    )

    scan_response = client.post(
        url_for('events.scan_query'),
        json={
            'start': '2026-04-08T11:00:00+00:00',
            'end': '2026-04-08T13:00:00+00:00',
            'query_filter': 'ActionName == "create_post"',
            'entity': None,
            'limit': 10,
        },
    )

    assert scan_response.status_code == 200
    scan_payload = scan_response.get_json()
    assert scan_payload == {
        'events': [
            {
                'id': 101,
                'timestamp': current_timestamp.isoformat(),
                'extracted_features': {
                    'ActionName': 'create_post',
                    'UserId': 'user_scan',
                    'EventType': 'create_post',
                    'PostText': 'hello from clickhouse integration test',
                    'ContainsHello': True,
                    '__action_id': 101,
                    '__error_count': 0,
                    '__timestamp': current_timestamp.isoformat(),
                },
            }
        ],
        'next_page': None,
    }

    detail_response = client.get(url_for('events.get_event_data', event_id=101))

    assert detail_response.status_code == 200
    assert detail_response.get_json() == {
        'id': 101,
        'timestamp': current_timestamp.isoformat(),
        'action_data': create_post_action,
        'error_traces': [],
        'extracted_features': {
            'ActionName': 'create_post',
            'UserId': 'user_scan',
            'EventType': 'create_post',
            'PostText': 'hello from clickhouse integration test',
            'ContainsHello': True,
            '__action_id': 101,
            '__error_count': 0,
            '__timestamp': current_timestamp.isoformat(),
        },
    }


@pytest.mark.use_rules_sources(CONFIG_ALLOW_ALL)
def test_clickhouse_scan_filters_boolean_features(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    hello_timestamp = datetime(2026, 4, 8, 12, 0, tzinfo=UTC)
    non_hello_timestamp = datetime(2026, 4, 8, 12, 5, tzinfo=UTC)

    seed_event(
        action_id=111,
        timestamp=hello_timestamp,
        action_name='create_post',
        action_data={
            'user_id': 'user_bool',
            'event_type': 'create_post',
            'post': {'text': 'hello from clickhouse integration test'},
        },
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_bool',
            'EventType': 'create_post',
            'PostText': 'hello from clickhouse integration test',
            'ContainsHello': True,
            '__action_id': 111,
            '__ban_user': ['user_bool|User said "hello"'],
            '__entity_label_mutations': ['User/meow/1'],
            '__error_count': 0,
            '__timestamp': hello_timestamp.isoformat(),
        },
    )
    seed_event(
        action_id=112,
        timestamp=non_hello_timestamp,
        action_name='create_post',
        action_data={
            'user_id': 'user_bool',
            'event_type': 'create_post',
            'post': {'text': 'no greeting here'},
        },
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_bool',
            'EventType': 'create_post',
            'PostText': 'no greeting here',
            'ContainsHello': False,
            '__action_id': 112,
            '__error_count': 0,
            '__timestamp': non_hello_timestamp.isoformat(),
        },
    )

    response = client.post(
        url_for('events.scan_query'),
        json={
            'start': '2026-04-08T11:00:00+00:00',
            'end': '2026-04-08T13:00:00+00:00',
            'query_filter': 'ContainsHello == true',
            'entity': None,
            'limit': 10,
        },
    )

    assert response.status_code == 200
    assert response.get_json() == {
        'events': [
            {
                'id': 111,
                'timestamp': hello_timestamp.isoformat(),
                'extracted_features': {
                    'ActionName': 'create_post',
                    'UserId': 'user_bool',
                    'EventType': 'create_post',
                    'PostText': 'hello from clickhouse integration test',
                    'ContainsHello': True,
                    '__action_id': 111,
                    '__ban_user': ['user_bool|User said "hello"'],
                    '__entity_label_mutations': ['User/meow/1'],
                    '__error_count': 0,
                    '__timestamp': hello_timestamp.isoformat(),
                },
            }
        ],
        'next_page': None,
    }


@pytest.mark.use_rules_sources(CONFIG_ALLOW_ALL)
def test_clickhouse_scan_matches_explicit_false_without_matching_missing_feature(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    missing_timestamp = datetime(2026, 4, 8, 12, 0, tzinfo=UTC)
    false_timestamp = datetime(2026, 4, 8, 12, 5, tzinfo=UTC)
    true_timestamp = datetime(2026, 4, 8, 12, 10, tzinfo=UTC)

    seed_event(
        action_id=113,
        timestamp=missing_timestamp,
        action_name='create_post',
        action_data={
            'user_id': 'user_bool_missing',
            'event_type': 'create_post',
            'post': {'text': 'feature intentionally omitted'},
        },
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_bool_missing',
            'EventType': 'create_post',
            'PostText': 'feature intentionally omitted',
            '__action_id': 113,
            '__error_count': 0,
            '__timestamp': missing_timestamp.isoformat(),
        },
    )
    seed_event(
        action_id=114,
        timestamp=false_timestamp,
        action_name='create_post',
        action_data={
            'user_id': 'user_bool_false',
            'event_type': 'create_post',
            'post': {'text': 'no greeting here'},
        },
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_bool_false',
            'EventType': 'create_post',
            'PostText': 'no greeting here',
            'ContainsHello': False,
            '__action_id': 114,
            '__error_count': 0,
            '__timestamp': false_timestamp.isoformat(),
        },
    )
    seed_event(
        action_id=115,
        timestamp=true_timestamp,
        action_name='create_post',
        action_data={
            'user_id': 'user_bool_true',
            'event_type': 'create_post',
            'post': {'text': 'hello there'},
        },
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_bool_true',
            'EventType': 'create_post',
            'PostText': 'hello there',
            'ContainsHello': True,
            '__action_id': 115,
            '__error_count': 0,
            '__timestamp': true_timestamp.isoformat(),
        },
    )

    response = client.post(
        url_for('events.scan_query'),
        json={
            'start': '2026-04-08T11:00:00+00:00',
            'end': '2026-04-08T13:00:00+00:00',
            'query_filter': 'ContainsHello == false',
            'entity': None,
            'limit': 10,
        },
    )

    assert response.status_code == 200
    assert response.get_json() == {
        'events': [
            {
                'id': 114,
                'timestamp': false_timestamp.isoformat(),
                'extracted_features': {
                    'ActionName': 'create_post',
                    'UserId': 'user_bool_false',
                    'EventType': 'create_post',
                    'PostText': 'no greeting here',
                    'ContainsHello': False,
                    '__action_id': 114,
                    '__error_count': 0,
                    '__timestamp': false_timestamp.isoformat(),
                },
            }
        ],
        'next_page': None,
    }


@pytest.mark.use_rules_sources(CONFIG_ALLOW_ALL)
def test_clickhouse_scan_paginates_duplicate_timestamps_without_gaps_or_duplicates(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    shared_timestamp = datetime(2026, 4, 8, 12, 0, tzinfo=UTC)

    for action_id in [121, 122, 123, 124]:
        seed_event(
            action_id=action_id,
            timestamp=shared_timestamp,
            action_name='create_post',
            action_data={
                'user_id': f'user_page_{action_id}',
                'event_type': 'create_post',
                'post': {'text': f'post for {action_id}'},
            },
            extracted_features={
                'ActionName': 'create_post',
                'UserId': f'user_page_{action_id}',
                'EventType': 'create_post',
                'PostText': f'post for {action_id}',
                'ContainsHello': False,
                '__action_id': action_id,
                '__error_count': 0,
                '__timestamp': shared_timestamp.isoformat(),
            },
        )

    first_page = client.post(
        url_for('events.scan_query'),
        json={
            'start': '2026-04-08T11:00:00+00:00',
            'end': '2026-04-08T13:00:00+00:00',
            'query_filter': 'ActionName == "create_post"',
            'entity': None,
            'limit': 2,
        },
    )

    assert first_page.status_code == 200
    first_payload = first_page.get_json()
    assert first_payload is not None
    assert first_payload['next_page'] is not None
    first_ids = {event['id'] for event in first_payload['events']}
    assert len(first_ids) == 2

    second_page = client.post(
        url_for('events.scan_query'),
        json={
            'start': '2026-04-08T11:00:00+00:00',
            'end': '2026-04-08T13:00:00+00:00',
            'query_filter': 'ActionName == "create_post"',
            'entity': None,
            'limit': 2,
            'next_page': first_payload['next_page'],
        },
    )

    assert second_page.status_code == 200
    second_payload = second_page.get_json()
    assert second_payload is not None
    assert second_payload['next_page'] is None
    second_ids = {event['id'] for event in second_payload['events']}

    assert len(second_ids) == 2
    assert first_ids.isdisjoint(second_ids)
    assert first_ids | second_ids == {121, 122, 123, 124}


@pytest.mark.use_rules_sources(CONFIG_ALLOW_ALL)
def test_clickhouse_aggregate_event_endpoints(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    timestamps = [
        datetime(2026, 4, 8, 12, 0, tzinfo=UTC),
        datetime(2026, 4, 8, 12, 10, tzinfo=UTC),
        datetime(2026, 4, 8, 12, 20, tzinfo=UTC),
    ]

    for action_id, timestamp, user_id in [
        (201, timestamps[0], 'user_a'),
        (202, timestamps[1], 'user_a'),
        (203, timestamps[2], 'user_b'),
    ]:
        seed_event(
            action_id=action_id,
            timestamp=timestamp,
            action_name='create_post',
            action_data={
                'user_id': user_id,
                'event_type': 'create_post',
                'post': {'text': f'post for {user_id}'},
            },
            extracted_features={
                'ActionName': 'create_post',
                'UserId': user_id,
                'EventType': 'create_post',
                'PostText': f'post for {user_id}',
                'ContainsHello': False,
                '__action_id': action_id,
                '__error_count': 0,
                '__timestamp': timestamp.isoformat(),
            },
        )

    base_request = {
        'start': '2026-04-08T12:00:00+00:00',
        'end': '2026-04-08T13:00:00+00:00',
        'query_filter': 'ActionName == "create_post"',
        'entity': None,
    }

    timeseries_response = client.post(
        url_for('events.timeseries_query'),
        json={**base_request, 'granularity': 'hour'},
    )
    assert timeseries_response.status_code == 200
    assert timeseries_response.get_json() == [
        {
            'timestamp': '2026-04-08T12:00:00+00:00',
            'result': {'count': 3},
        }
    ]

    groupby_response = client.post(
        url_for('events.groupby_count'),
        json={**base_request, 'dimension': 'UserId'},
    )
    assert groupby_response.status_code == 200
    assert groupby_response.get_json() == {'count': 2}

    topn_response = client.post(
        url_for('events.topn_query'),
        json={**base_request, 'dimension': 'UserId', 'limit': 10, 'precision': 0},
    )
    assert topn_response.status_code == 200
    topn_payload = topn_response.get_json()
    assert topn_payload['previous_period'] is None
    assert topn_payload['comparison'] is None
    assert topn_payload['current_period'] == [
        {
            'timestamp': 'Wed, 08 Apr 2026 13:00:00 GMT',
            'result': [
                {'UserId': 'user_a', 'count': 2},
                {'UserId': 'user_b', 'count': 1},
            ],
        }
    ]


@pytest.mark.use_rules_sources(CONFIG_ALLOW_ALL)
def test_clickhouse_aggregate_event_endpoints_filter_boolean_features(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    seeded_events = [
        (211, datetime(2026, 4, 8, 12, 0, tzinfo=UTC), 'user_a', True),
        (212, datetime(2026, 4, 8, 12, 10, tzinfo=UTC), 'user_a', True),
        (213, datetime(2026, 4, 8, 12, 20, tzinfo=UTC), 'user_b', True),
        (214, datetime(2026, 4, 8, 12, 30, tzinfo=UTC), 'user_c', False),
    ]

    for action_id, timestamp, user_id, contains_hello in seeded_events:
        seed_event(
            action_id=action_id,
            timestamp=timestamp,
            action_name='create_post',
            action_data={
                'user_id': user_id,
                'event_type': 'create_post',
                'post': {'text': f'post for {user_id}'},
            },
            extracted_features={
                'ActionName': 'create_post',
                'UserId': user_id,
                'EventType': 'create_post',
                'PostText': f'post for {user_id}',
                'ContainsHello': contains_hello,
                '__action_id': action_id,
                '__error_count': 0,
                '__timestamp': timestamp.isoformat(),
            },
        )

    base_request = {
        'start': '2026-04-08T12:00:00+00:00',
        'end': '2026-04-08T13:00:00+00:00',
        'query_filter': 'ContainsHello == true',
        'entity': None,
    }

    timeseries_response = client.post(
        url_for('events.timeseries_query'),
        json={**base_request, 'granularity': 'hour'},
    )
    assert timeseries_response.status_code == 200
    assert timeseries_response.get_json() == [
        {
            'timestamp': '2026-04-08T12:00:00+00:00',
            'result': {'count': 3},
        }
    ]

    groupby_response = client.post(
        url_for('events.groupby_count'),
        json={**base_request, 'dimension': 'UserId'},
    )
    assert groupby_response.status_code == 200
    assert groupby_response.get_json() == {'count': 2}

    topn_response = client.post(
        url_for('events.topn_query'),
        json={**base_request, 'dimension': 'UserId', 'limit': 10, 'precision': 0},
    )
    assert topn_response.status_code == 200
    topn_payload = topn_response.get_json()
    assert topn_payload['previous_period'] is None
    assert topn_payload['comparison'] is None
    assert topn_payload['current_period'] == [
        {
            'timestamp': 'Wed, 08 Apr 2026 13:00:00 GMT',
            'result': [
                {'UserId': 'user_a', 'count': 2},
                {'UserId': 'user_b', 'count': 1},
            ],
        }
    ]


@pytest.mark.use_rules_sources(CONFIG_ACTION_FILTERED)
def test_clickhouse_scan_respects_action_acl_filter(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    current_timestamp = datetime(2026, 4, 8, 14, 0, tzinfo=UTC)
    filtered_timestamp = datetime(2026, 4, 8, 14, 5, tzinfo=UTC)

    seed_event(
        action_id=301,
        timestamp=current_timestamp,
        action_name='create_post',
        action_data={'user_id': 'user_acl', 'event_type': 'create_post', 'post': {'text': 'allowed'}},
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_acl',
            'EventType': 'create_post',
            'PostText': 'allowed',
            'ContainsHello': False,
            '__action_id': 301,
            '__error_count': 0,
            '__timestamp': current_timestamp.isoformat(),
        },
    )
    seed_event(
        action_id=302,
        timestamp=filtered_timestamp,
        action_name='delete_post',
        action_data={'user_id': 'user_acl', 'event_type': 'delete_post', 'post': {'text': 'filtered'}},
        extracted_features={
            'ActionName': 'delete_post',
            'UserId': 'user_acl',
            'EventType': 'delete_post',
            'PostText': 'filtered',
            'ContainsHello': False,
            '__action_id': 302,
            '__error_count': 0,
            '__timestamp': filtered_timestamp.isoformat(),
        },
    )

    response = client.post(
        url_for('events.scan_query'),
        json={
            'start': '2026-04-08T13:00:00+00:00',
            'end': '2026-04-08T15:00:00+00:00',
            'query_filter': '',
            'entity': None,
            'limit': 10,
        },
    )

    assert response.status_code == 200
    assert response.get_json() == {
        'events': [
            {
                'id': 301,
                'timestamp': current_timestamp.isoformat(),
                'extracted_features': {
                    'ActionName': 'create_post',
                    'UserId': 'user_acl',
                    'EventType': 'create_post',
                    'PostText': 'allowed',
                    'ContainsHello': False,
                    '__action_id': 301,
                    '__error_count': 0,
                    '__timestamp': current_timestamp.isoformat(),
                },
            }
        ],
        'next_page': None,
    }


@pytest.mark.use_rules_sources(CONFIG_ACTION_FILTERED)
def test_clickhouse_scan_combines_boolean_query_with_action_acl_filter(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    allowed_timestamp = datetime(2026, 4, 8, 15, 0, tzinfo=UTC)
    query_filtered_timestamp = datetime(2026, 4, 8, 15, 5, tzinfo=UTC)
    acl_filtered_timestamp = datetime(2026, 4, 8, 15, 10, tzinfo=UTC)

    seed_event(
        action_id=311,
        timestamp=allowed_timestamp,
        action_name='create_post',
        action_data={'user_id': 'user_acl_bool', 'event_type': 'create_post', 'post': {'text': 'hello allowed'}},
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_acl_bool',
            'EventType': 'create_post',
            'PostText': 'hello allowed',
            'ContainsHello': True,
            '__action_id': 311,
            '__ban_user': ['user_acl_bool|User said "hello"'],
            '__entity_label_mutations': ['User/meow/1'],
            '__error_count': 0,
            '__timestamp': allowed_timestamp.isoformat(),
        },
    )
    seed_event(
        action_id=312,
        timestamp=query_filtered_timestamp,
        action_name='create_post',
        action_data={'user_id': 'user_acl_bool', 'event_type': 'create_post', 'post': {'text': 'no greeting'}},
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_acl_bool',
            'EventType': 'create_post',
            'PostText': 'no greeting',
            'ContainsHello': False,
            '__action_id': 312,
            '__error_count': 0,
            '__timestamp': query_filtered_timestamp.isoformat(),
        },
    )
    seed_event(
        action_id=313,
        timestamp=acl_filtered_timestamp,
        action_name='delete_post',
        action_data={'user_id': 'user_acl_bool', 'event_type': 'delete_post', 'post': {'text': 'hello filtered'}},
        extracted_features={
            'ActionName': 'delete_post',
            'UserId': 'user_acl_bool',
            'EventType': 'delete_post',
            'PostText': 'hello filtered',
            'ContainsHello': True,
            '__action_id': 313,
            '__error_count': 0,
            '__timestamp': acl_filtered_timestamp.isoformat(),
        },
    )

    response = client.post(
        url_for('events.scan_query'),
        json={
            'start': '2026-04-08T14:00:00+00:00',
            'end': '2026-04-08T16:00:00+00:00',
            'query_filter': 'ContainsHello == true',
            'entity': None,
            'limit': 10,
        },
    )

    assert response.status_code == 200
    assert response.get_json() == {
        'events': [
            {
                'id': 311,
                'timestamp': allowed_timestamp.isoformat(),
                'extracted_features': {
                    'ActionName': 'create_post',
                    'UserId': 'user_acl_bool',
                    'EventType': 'create_post',
                    'PostText': 'hello allowed',
                    'ContainsHello': True,
                    '__action_id': 311,
                    '__ban_user': ['user_acl_bool|User said "hello"'],
                    '__entity_label_mutations': ['User/meow/1'],
                    '__error_count': 0,
                    '__timestamp': allowed_timestamp.isoformat(),
                },
            }
        ],
        'next_page': None,
    }


@pytest.mark.use_rules_sources(CONFIG_ACTION_FILTERED)
def test_clickhouse_aggregate_event_endpoints_combine_boolean_query_with_action_acl_filter(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    seeded_events = [
        (321, datetime(2026, 4, 8, 16, 0, tzinfo=UTC), 'user_a_acl', 'create_post', True),
        (322, datetime(2026, 4, 8, 16, 10, tzinfo=UTC), 'user_a_acl', 'create_post', True),
        (323, datetime(2026, 4, 8, 16, 20, tzinfo=UTC), 'user_b_acl', 'create_post', True),
        (324, datetime(2026, 4, 8, 16, 30, tzinfo=UTC), 'user_c_acl', 'create_post', False),
        (325, datetime(2026, 4, 8, 16, 40, tzinfo=UTC), 'user_d_acl', 'delete_post', True),
    ]

    for action_id, timestamp, user_id, action_name, contains_hello in seeded_events:
        seed_event(
            action_id=action_id,
            timestamp=timestamp,
            action_name=action_name,
            action_data={
                'user_id': user_id,
                'event_type': action_name,
                'post': {'text': f'post for {user_id}'},
            },
            extracted_features={
                'ActionName': action_name,
                'UserId': user_id,
                'EventType': action_name,
                'PostText': f'post for {user_id}',
                'ContainsHello': contains_hello,
                '__action_id': action_id,
                '__error_count': 0,
                '__timestamp': timestamp.isoformat(),
            },
        )

    base_request = {
        'start': '2026-04-08T16:00:00+00:00',
        'end': '2026-04-08T17:00:00+00:00',
        'query_filter': 'ContainsHello == true',
        'entity': None,
    }

    timeseries_response = client.post(
        url_for('events.timeseries_query'),
        json={**base_request, 'granularity': 'hour'},
    )
    assert timeseries_response.status_code == 200
    assert timeseries_response.get_json() == [
        {
            'timestamp': '2026-04-08T16:00:00+00:00',
            'result': {'count': 3},
        }
    ]

    groupby_response = client.post(
        url_for('events.groupby_count'),
        json={**base_request, 'dimension': 'UserId'},
    )
    assert groupby_response.status_code == 200
    assert groupby_response.get_json() == {'count': 2}

    topn_response = client.post(
        url_for('events.topn_query'),
        json={**base_request, 'dimension': 'UserId', 'limit': 10, 'precision': 0},
    )
    assert topn_response.status_code == 200
    topn_payload = topn_response.get_json()
    assert topn_payload['previous_period'] is None
    assert topn_payload['comparison'] is None
    assert {item['UserId']: item['count'] for item in topn_payload['current_period'][0]['result']} == {
        'user_a_acl': 2,
        'user_b_acl': 1,
    }


@pytest.mark.use_rules_sources(CONFIG_ACTION_FILTERED)
def test_clickhouse_topn_csv_combines_boolean_query_with_action_acl_filter(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    seeded_events = [
        (331, datetime(2026, 4, 8, 17, 0, tzinfo=UTC), 'user_a_csv', 'create_post', True),
        (332, datetime(2026, 4, 8, 17, 10, tzinfo=UTC), 'user_a_csv', 'create_post', True),
        (333, datetime(2026, 4, 8, 17, 20, tzinfo=UTC), 'user_b_csv', 'create_post', True),
        (334, datetime(2026, 4, 8, 17, 30, tzinfo=UTC), 'user_c_csv', 'create_post', False),
        (335, datetime(2026, 4, 8, 17, 40, tzinfo=UTC), 'user_d_csv', 'delete_post', True),
    ]

    for action_id, timestamp, user_id, action_name, contains_hello in seeded_events:
        seed_event(
            action_id=action_id,
            timestamp=timestamp,
            action_name=action_name,
            action_data={
                'user_id': user_id,
                'event_type': action_name,
                'post': {'text': f'post for {user_id}'},
            },
            extracted_features={
                'ActionName': action_name,
                'UserId': user_id,
                'EventType': action_name,
                'PostText': f'post for {user_id}',
                'ContainsHello': contains_hello,
                '__action_id': action_id,
                '__error_count': 0,
                '__timestamp': timestamp.isoformat(),
            },
        )

    response = client.post(
        url_for('events.topn_query_csv'),
        json={
            'start': '2026-04-08T17:00:00+00:00',
            'end': '2026-04-08T18:00:00+00:00',
            'query_filter': 'ContainsHello == true',
            'entity': None,
            'dimension': 'UserId',
            'limit': 10,
            'precision': 0,
        },
    )

    assert response.status_code == 200
    rows = list(csv.DictReader(io.StringIO(response.get_data(as_text=True))))
    assert {row['UserId']: row['current_count'] for row in rows} == {
        'user_a_csv': '2',
        'user_b_csv': '1',
    }


@pytest.mark.use_rules_sources(CONFIG_ALLOW_ALL)
@pytest.mark.parametrize(
    ('endpoint', 'payload'),
    [
        (
            'events.groupby_count',
            {
                'start': '2026-04-08T17:00:00+00:00',
                'end': '2026-04-08T18:00:00+00:00',
                'query_filter': '',
                'entity': None,
                'dimension': 'UnknownFeature',
            },
        ),
        (
            'events.topn_query',
            {
                'start': '2026-04-08T17:00:00+00:00',
                'end': '2026-04-08T18:00:00+00:00',
                'query_filter': '',
                'entity': None,
                'dimension': 'UnknownFeature',
                'limit': 10,
                'precision': 0,
            },
        ),
    ],
)
def test_clickhouse_event_endpoints_reject_unknown_feature_names_with_bad_request(
    app: Flask,
    client: 'FlaskClient[Response]',
    endpoint: str,
    payload: Dict[str, Any],
) -> None:
    response = client.post(url_for(endpoint), json=payload)

    assert response.status_code == 400
    assert response.get_data(as_text=True) == 'Unknown feature name for ClickHouse query rendering: UnknownFeature'


@pytest.mark.use_rules_sources(CONFIG_ALLOW_ALL)
def test_clickhouse_topn_rejects_non_zero_precision_with_bad_request(
    app: Flask,
    client: 'FlaskClient[Response]',
) -> None:
    response = client.post(
        url_for('events.topn_query'),
        json={
            'start': '2026-04-08T17:00:00+00:00',
            'end': '2026-04-08T18:00:00+00:00',
            'query_filter': '',
            'entity': None,
            'dimension': 'UserId',
            'limit': 10,
            'precision': 0.1,
        },
    )

    assert response.status_code == 400
    assert response.get_data(as_text=True) == 'ClickHouse topn queries do not support non-zero precision'
