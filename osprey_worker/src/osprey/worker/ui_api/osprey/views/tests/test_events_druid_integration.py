import csv
import io
import json
import os
import time
from datetime import UTC, datetime
from typing import Any, Callable, Dict, Iterator, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import pytest
from flask import Flask, Response, url_for
from flask.testing import FlaskClient
from kafka import KafkaProducer
from osprey.worker._stdlibplugin.execution_result_store_chooser import get_rules_execution_result_storage_backend
from osprey.worker.lib.storage import ExecutionResultStorageBackendType

RUN_DRUID_INTEGRATION_TESTS = os.environ.get('RUN_DRUID_INTEGRATION_TESTS', '').lower() == 'true'
pytestmark = pytest.mark.skipif(
    not RUN_DRUID_INTEGRATION_TESTS,
    reason='Druid integration tests require docker-compose.test.yaml with Druid services',
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

_DRUID_INGESTION_PIPELINE_READY = False
DRUID_SUPERVISOR_ID = 'osprey.execution_results'


@pytest.fixture(scope='session')
def kafka_output_producer() -> Iterator[KafkaProducer]:
    producer = KafkaProducer(bootstrap_servers=['osprey-kafka:29092'])
    try:
        yield producer
    finally:
        producer.flush()
        producer.close()


@pytest.fixture()
def seed_event(app: Flask, kafka_output_producer: KafkaProducer) -> Callable[..., Dict[str, Any]]:
    storage_backend = get_rules_execution_result_storage_backend(ExecutionResultStorageBackendType.MINIO)
    assert storage_backend is not None

    def _seed_event(
        *,
        action_id: int,
        timestamp: datetime,
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
        kafka_output_producer.send('osprey.execution_results', value=extracted_features_json.encode('utf-8')).get(
            timeout=10
        )
        kafka_output_producer.flush()
        return extracted_features

    return _seed_event


@pytest.fixture(autouse=True)
def warm_druid_ingestion_pipeline() -> Iterator[None]:
    global _DRUID_INGESTION_PIPELINE_READY

    if not _DRUID_INGESTION_PIPELINE_READY:
        wait_for_druid_supervisor_ready(timeout_seconds=180)
        _DRUID_INGESTION_PIPELINE_READY = True

    yield


def wait_for_druid_supervisor_ready(*, timeout_seconds: int = 90) -> Dict[str, Any]:
    status_url = f'http://druid-coordinator:8081/druid/indexer/v1/supervisor/{DRUID_SUPERVISOR_ID}/status'

    def fetch() -> Tuple[int, Any]:
        try:
            with urlopen(status_url, timeout=5) as response:
                return response.status, json.loads(response.read().decode('utf-8'))
        except HTTPError as exc:
            try:
                payload: Any = json.loads(exc.read().decode('utf-8'))
            except (OSError, json.JSONDecodeError):
                payload = None
            return exc.code, payload
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            return 0, str(exc)

    return wait_for_json(fetch, _druid_supervisor_is_ready, timeout_seconds=timeout_seconds)


def _druid_supervisor_is_ready(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False

    supervisor_status = payload.get('payload', payload)
    if not isinstance(supervisor_status, dict):
        return False

    return supervisor_status.get('healthy') is True and supervisor_status.get('state') == 'RUNNING'


def wait_for_json(
    fetch: Callable[[], Tuple[int, Any]],
    predicate: Callable[[Any], bool],
    *,
    timeout_seconds: int = 90,
) -> Any:
    deadline = time.monotonic() + timeout_seconds
    last_status = None
    last_payload = None

    while time.monotonic() < deadline:
        status_code, payload = fetch()
        if status_code == 200 and predicate(payload):
            return payload

        last_status = status_code
        last_payload = payload
        time.sleep(1)

    pytest.fail(f'Druid integration query did not converge in {timeout_seconds}s: {last_status=} {last_payload=}')


def post_json(
    client: 'FlaskClient[Response]',
    endpoint: str,
    payload: Dict[str, Any],
) -> Tuple[int, Any]:
    response = client.post(url_for(endpoint), json=payload)
    return response.status_code, response.get_json(silent=True)


def wait_for_csv_rows(
    fetch: Callable[[], Response],
    predicate: Callable[[list[dict[str, str]]], bool],
    *,
    timeout_seconds: int = 90,
) -> list[dict[str, str]]:
    deadline = time.monotonic() + timeout_seconds
    last_status = None
    last_payload = None

    while time.monotonic() < deadline:
        response = fetch()
        rows = list(csv.DictReader(io.StringIO(response.get_data(as_text=True))))
        if response.status_code == 200 and predicate(rows):
            return rows

        last_status = response.status_code
        last_payload = rows
        time.sleep(1)

    pytest.fail(f'Druid integration CSV did not converge in {timeout_seconds}s: {last_status=} {last_payload=}')


def wait_for_druid_ingestion(
    client: 'FlaskClient[Response]',
    *,
    start: str,
    end: str,
    expected_count: int,
    timeout_seconds: int = 90,
) -> Any:
    return wait_for_json(
        lambda: post_json(
            client,
            'events.timeseries_query',
            {
                'start': start,
                'end': end,
                'query_filter': '',
                'entity': None,
                'granularity': 'hour',
            },
        ),
        lambda payload: isinstance(payload, list)
        and len(payload) == 1
        and payload[0]['result']['count'] == expected_count,
        timeout_seconds=timeout_seconds,
    )


@pytest.mark.use_rules_sources(CONFIG_ALLOW_ALL)
def test_druid_scan_and_event_detail(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    current_timestamp = datetime(2026, 4, 8, 12, 0, tzinfo=UTC)
    ignored_timestamp = datetime(2026, 4, 8, 12, 5, tzinfo=UTC)

    create_post_action = {
        'user_id': 'user_scan_druid',
        'event_type': 'create_post',
        'post': {'text': 'hello from druid integration test'},
    }
    delete_post_action = {
        'user_id': 'user_scan_druid',
        'event_type': 'delete_post',
        'post': {'text': 'ignored'},
    }

    seed_event(
        action_id=401,
        timestamp=current_timestamp,
        action_data=create_post_action,
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_scan_druid',
            'EventType': 'create_post',
            'PostText': 'hello from druid integration test',
            'ContainsHello': True,
            '__action_id': 401,
            '__ban_user': ['user_scan_druid|User said "hello"'],
            '__entity_label_mutations': ['User/meow/1'],
            '__error_count': 0,
            '__timestamp': current_timestamp.isoformat(),
        },
    )
    seed_event(
        action_id=402,
        timestamp=ignored_timestamp,
        action_data=delete_post_action,
        extracted_features={
            'ActionName': 'delete_post',
            'UserId': 'user_scan_druid',
            'EventType': 'delete_post',
            'PostText': 'ignored',
            'ContainsHello': False,
            '__action_id': 402,
            '__error_count': 0,
            '__timestamp': ignored_timestamp.isoformat(),
        },
    )

    expected_scan_payload = {
        'events': [
            {
                'id': 401,
                'timestamp': current_timestamp.isoformat(),
                'extracted_features': {
                    'ActionName': 'create_post',
                    'UserId': 'user_scan_druid',
                    'EventType': 'create_post',
                    'PostText': 'hello from druid integration test',
                    'ContainsHello': True,
                    '__action_id': 401,
                    '__ban_user': ['user_scan_druid|User said "hello"'],
                    '__entity_label_mutations': ['User/meow/1'],
                    '__error_count': 0,
                    '__timestamp': current_timestamp.isoformat(),
                },
            }
        ],
        'next_page': None,
    }

    wait_for_druid_ingestion(
        client,
        start='2026-04-08T11:00:00+00:00',
        end='2026-04-08T13:00:00+00:00',
        expected_count=2,
    )

    scan_payload = wait_for_json(
        lambda: post_json(
            client,
            'events.scan_query',
            {
                'start': '2026-04-08T11:00:00+00:00',
                'end': '2026-04-08T13:00:00+00:00',
                'query_filter': 'ActionName == "create_post"',
                'entity': None,
                'limit': 10,
            },
        ),
        lambda payload: payload == expected_scan_payload,
    )

    assert scan_payload == expected_scan_payload

    detail_response = client.get(url_for('events.get_event_data', event_id=401))

    assert detail_response.status_code == 200
    assert detail_response.get_json() == {
        'id': 401,
        'timestamp': current_timestamp.isoformat(),
        'action_data': create_post_action,
        'error_traces': [],
        'extracted_features': {
            'ActionName': 'create_post',
            'UserId': 'user_scan_druid',
            'EventType': 'create_post',
            'PostText': 'hello from druid integration test',
            'ContainsHello': True,
            '__action_id': 401,
            '__ban_user': ['user_scan_druid|User said "hello"'],
            '__entity_label_mutations': ['User/meow/1'],
            '__error_count': 0,
            '__timestamp': current_timestamp.isoformat(),
        },
    }


@pytest.mark.use_rules_sources(CONFIG_ALLOW_ALL)
def test_druid_scan_filters_boolean_features(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    hello_timestamp = datetime(2026, 4, 8, 16, 0, tzinfo=UTC)
    non_hello_timestamp = datetime(2026, 4, 8, 16, 5, tzinfo=UTC)

    seed_event(
        action_id=410,
        timestamp=hello_timestamp,
        action_data={
            'user_id': 'user_bool_druid',
            'event_type': 'create_post',
            'post': {'text': 'hello from druid integration test'},
        },
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_bool_druid',
            'EventType': 'create_post',
            'PostText': 'hello from druid integration test',
            'ContainsHello': True,
            '__action_id': 410,
            '__ban_user': ['user_bool_druid|User said "hello"'],
            '__entity_label_mutations': ['User/meow/1'],
            '__error_count': 0,
            '__timestamp': hello_timestamp.isoformat(),
        },
    )
    seed_event(
        action_id=415,
        timestamp=non_hello_timestamp,
        action_data={
            'user_id': 'user_bool_druid',
            'event_type': 'create_post',
            'post': {'text': 'no greeting here'},
        },
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_bool_druid',
            'EventType': 'create_post',
            'PostText': 'no greeting here',
            'ContainsHello': False,
            '__action_id': 415,
            '__error_count': 0,
            '__timestamp': non_hello_timestamp.isoformat(),
        },
    )

    wait_for_druid_ingestion(
        client,
        start='2026-04-08T15:00:00+00:00',
        end='2026-04-08T17:00:00+00:00',
        expected_count=2,
    )

    scan_payload = wait_for_json(
        lambda: post_json(
            client,
            'events.scan_query',
            {
                'start': '2026-04-08T15:00:00+00:00',
                'end': '2026-04-08T17:00:00+00:00',
                'query_filter': 'ContainsHello == true',
                'entity': None,
                'limit': 10,
            },
        ),
        lambda payload: payload
        == {
            'events': [
                {
                    'id': 410,
                    'timestamp': hello_timestamp.isoformat(),
                    'extracted_features': {
                        'ActionName': 'create_post',
                        'UserId': 'user_bool_druid',
                        'EventType': 'create_post',
                        'PostText': 'hello from druid integration test',
                        'ContainsHello': True,
                        '__action_id': 410,
                        '__ban_user': ['user_bool_druid|User said "hello"'],
                        '__entity_label_mutations': ['User/meow/1'],
                        '__error_count': 0,
                        '__timestamp': hello_timestamp.isoformat(),
                    },
                }
            ],
            'next_page': None,
        },
    )

    assert scan_payload == {
        'events': [
            {
                'id': 410,
                'timestamp': hello_timestamp.isoformat(),
                'extracted_features': {
                    'ActionName': 'create_post',
                    'UserId': 'user_bool_druid',
                    'EventType': 'create_post',
                    'PostText': 'hello from druid integration test',
                    'ContainsHello': True,
                    '__action_id': 410,
                    '__ban_user': ['user_bool_druid|User said "hello"'],
                    '__entity_label_mutations': ['User/meow/1'],
                    '__error_count': 0,
                    '__timestamp': hello_timestamp.isoformat(),
                },
            }
        ],
        'next_page': None,
    }


@pytest.mark.use_rules_sources(CONFIG_ALLOW_ALL)
def test_druid_scan_matches_explicit_false_without_matching_missing_feature(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    missing_timestamp = datetime(2026, 4, 8, 9, 0, tzinfo=UTC)
    false_timestamp = datetime(2026, 4, 8, 9, 5, tzinfo=UTC)
    true_timestamp = datetime(2026, 4, 8, 9, 10, tzinfo=UTC)

    seed_event(
        action_id=416,
        timestamp=missing_timestamp,
        action_data={
            'user_id': 'user_bool_missing_druid',
            'event_type': 'create_post',
            'post': {'text': 'feature intentionally omitted'},
        },
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_bool_missing_druid',
            'EventType': 'create_post',
            'PostText': 'feature intentionally omitted',
            '__action_id': 416,
            '__error_count': 0,
            '__timestamp': missing_timestamp.isoformat(),
        },
    )
    seed_event(
        action_id=417,
        timestamp=false_timestamp,
        action_data={
            'user_id': 'user_bool_false_druid',
            'event_type': 'create_post',
            'post': {'text': 'no greeting here'},
        },
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_bool_false_druid',
            'EventType': 'create_post',
            'PostText': 'no greeting here',
            'ContainsHello': False,
            '__action_id': 417,
            '__error_count': 0,
            '__timestamp': false_timestamp.isoformat(),
        },
    )
    seed_event(
        action_id=418,
        timestamp=true_timestamp,
        action_data={
            'user_id': 'user_bool_true_druid',
            'event_type': 'create_post',
            'post': {'text': 'hello there'},
        },
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_bool_true_druid',
            'EventType': 'create_post',
            'PostText': 'hello there',
            'ContainsHello': True,
            '__action_id': 418,
            '__error_count': 0,
            '__timestamp': true_timestamp.isoformat(),
        },
    )

    wait_for_druid_ingestion(
        client,
        start='2026-04-08T08:00:00+00:00',
        end='2026-04-08T10:00:00+00:00',
        expected_count=3,
    )

    expected_payload = {
        'events': [
            {
                'id': 417,
                'timestamp': false_timestamp.isoformat(),
                'extracted_features': {
                    'ActionName': 'create_post',
                    'UserId': 'user_bool_false_druid',
                    'EventType': 'create_post',
                    'PostText': 'no greeting here',
                    'ContainsHello': False,
                    '__action_id': 417,
                    '__error_count': 0,
                    '__timestamp': false_timestamp.isoformat(),
                },
            }
        ],
        'next_page': None,
    }

    scan_payload = wait_for_json(
        lambda: post_json(
            client,
            'events.scan_query',
            {
                'start': '2026-04-08T08:00:00+00:00',
                'end': '2026-04-08T10:00:00+00:00',
                'query_filter': 'ContainsHello == false',
                'entity': None,
                'limit': 10,
            },
        ),
        lambda payload: payload == expected_payload,
    )

    assert scan_payload == expected_payload


@pytest.mark.use_rules_sources(CONFIG_ALLOW_ALL)
def test_druid_aggregate_event_endpoints_filter_boolean_features(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    seeded_events = [
        (411, datetime(2026, 4, 8, 18, 0, tzinfo=UTC), 'user_a_druid', True),
        (412, datetime(2026, 4, 8, 18, 10, tzinfo=UTC), 'user_a_druid', True),
        (413, datetime(2026, 4, 8, 18, 20, tzinfo=UTC), 'user_b_druid', True),
        (414, datetime(2026, 4, 8, 18, 30, tzinfo=UTC), 'user_c_druid', False),
    ]

    for action_id, timestamp, user_id, contains_hello in seeded_events:
        seed_event(
            action_id=action_id,
            timestamp=timestamp,
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

    wait_for_json(
        lambda: post_json(
            client,
            'events.timeseries_query',
            {
                'start': '2026-04-08T18:00:00+00:00',
                'end': '2026-04-08T19:00:00+00:00',
                'query_filter': '',
                'entity': None,
                'granularity': 'hour',
            },
        ),
        lambda payload: isinstance(payload, list) and len(payload) == 1 and payload[0]['result']['count'] == 4,
    )

    base_request = {
        'start': '2026-04-08T18:00:00+00:00',
        'end': '2026-04-08T19:00:00+00:00',
        'query_filter': 'ContainsHello == true',
        'entity': None,
    }

    timeseries_payload = wait_for_json(
        lambda: post_json(client, 'events.timeseries_query', {**base_request, 'granularity': 'hour'}),
        lambda payload: isinstance(payload, list) and len(payload) == 1 and payload[0]['result']['count'] == 3,
    )
    assert len(timeseries_payload) == 1
    assert timeseries_payload[0]['result']['count'] == 3

    groupby_payload = wait_for_json(
        lambda: post_json(client, 'events.groupby_count', {**base_request, 'dimension': 'UserId'}),
        lambda payload: payload == {'count': 2},
    )
    assert groupby_payload == {'count': 2}

    topn_payload = wait_for_json(
        lambda: post_json(
            client, 'events.topn_query', {**base_request, 'dimension': 'UserId', 'limit': 10, 'precision': 0}
        ),
        lambda payload: payload is not None
        and payload.get('previous_period') is None
        and payload.get('comparison') is None
        and payload.get('current_period')
        and {item['UserId']: item['count'] for item in payload['current_period'][0]['result']}
        == {'user_a_druid': 2, 'user_b_druid': 1},
    )

    assert topn_payload['previous_period'] is None
    assert topn_payload['comparison'] is None
    assert {item['UserId']: item['count'] for item in topn_payload['current_period'][0]['result']} == {
        'user_a_druid': 2,
        'user_b_druid': 1,
    }


@pytest.mark.use_rules_sources(CONFIG_ACTION_FILTERED)
def test_druid_scan_respects_action_acl_filter(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    current_timestamp = datetime(2026, 4, 8, 14, 0, tzinfo=UTC)
    filtered_timestamp = datetime(2026, 4, 8, 14, 5, tzinfo=UTC)

    seed_event(
        action_id=421,
        timestamp=current_timestamp,
        action_data={'user_id': 'user_acl_druid', 'event_type': 'create_post', 'post': {'text': 'allowed'}},
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_acl_druid',
            'EventType': 'create_post',
            'PostText': 'allowed',
            'ContainsHello': False,
            '__action_id': 421,
            '__error_count': 0,
            '__timestamp': current_timestamp.isoformat(),
        },
    )
    seed_event(
        action_id=422,
        timestamp=filtered_timestamp,
        action_data={'user_id': 'user_acl_druid', 'event_type': 'delete_post', 'post': {'text': 'filtered'}},
        extracted_features={
            'ActionName': 'delete_post',
            'UserId': 'user_acl_druid',
            'EventType': 'delete_post',
            'PostText': 'filtered',
            'ContainsHello': False,
            '__action_id': 422,
            '__error_count': 0,
            '__timestamp': filtered_timestamp.isoformat(),
        },
    )

    wait_for_druid_ingestion(
        client,
        start='2026-04-08T14:00:00+00:00',
        end='2026-04-08T15:00:00+00:00',
        expected_count=1,
    )

    expected_payload = {
        'events': [
            {
                'id': 421,
                'timestamp': current_timestamp.isoformat(),
                'extracted_features': {
                    'ActionName': 'create_post',
                    'UserId': 'user_acl_druid',
                    'EventType': 'create_post',
                    'PostText': 'allowed',
                    'ContainsHello': False,
                    '__action_id': 421,
                    '__error_count': 0,
                    '__timestamp': current_timestamp.isoformat(),
                },
            }
        ],
        'next_page': None,
    }

    payload = wait_for_json(
        lambda: post_json(
            client,
            'events.scan_query',
            {
                'start': '2026-04-08T14:00:00+00:00',
                'end': '2026-04-08T15:00:00+00:00',
                'query_filter': '',
                'entity': None,
                'limit': 10,
            },
        ),
        lambda response_payload: response_payload == expected_payload,
    )

    assert payload == expected_payload


@pytest.mark.use_rules_sources(CONFIG_ACTION_FILTERED)
def test_druid_scan_combines_boolean_query_with_action_acl_filter(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    allowed_timestamp = datetime(2026, 4, 8, 20, 0, tzinfo=UTC)
    query_filtered_timestamp = datetime(2026, 4, 8, 20, 5, tzinfo=UTC)
    acl_filtered_timestamp = datetime(2026, 4, 8, 20, 10, tzinfo=UTC)

    seed_event(
        action_id=431,
        timestamp=allowed_timestamp,
        action_data={'user_id': 'user_acl_bool_druid', 'event_type': 'create_post', 'post': {'text': 'hello allowed'}},
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_acl_bool_druid',
            'EventType': 'create_post',
            'PostText': 'hello allowed',
            'ContainsHello': True,
            '__action_id': 431,
            '__ban_user': ['user_acl_bool_druid|User said "hello"'],
            '__entity_label_mutations': ['User/meow/1'],
            '__error_count': 0,
            '__timestamp': allowed_timestamp.isoformat(),
        },
    )
    seed_event(
        action_id=432,
        timestamp=query_filtered_timestamp,
        action_data={'user_id': 'user_acl_bool_druid', 'event_type': 'create_post', 'post': {'text': 'no greeting'}},
        extracted_features={
            'ActionName': 'create_post',
            'UserId': 'user_acl_bool_druid',
            'EventType': 'create_post',
            'PostText': 'no greeting',
            'ContainsHello': False,
            '__action_id': 432,
            '__error_count': 0,
            '__timestamp': query_filtered_timestamp.isoformat(),
        },
    )
    seed_event(
        action_id=433,
        timestamp=acl_filtered_timestamp,
        action_data={'user_id': 'user_acl_bool_druid', 'event_type': 'delete_post', 'post': {'text': 'hello filtered'}},
        extracted_features={
            'ActionName': 'delete_post',
            'UserId': 'user_acl_bool_druid',
            'EventType': 'delete_post',
            'PostText': 'hello filtered',
            'ContainsHello': True,
            '__action_id': 433,
            '__error_count': 0,
            '__timestamp': acl_filtered_timestamp.isoformat(),
        },
    )

    wait_for_druid_ingestion(
        client,
        start='2026-04-08T19:00:00+00:00',
        end='2026-04-08T21:00:00+00:00',
        expected_count=2,
    )

    expected_payload = {
        'events': [
            {
                'id': 431,
                'timestamp': allowed_timestamp.isoformat(),
                'extracted_features': {
                    'ActionName': 'create_post',
                    'UserId': 'user_acl_bool_druid',
                    'EventType': 'create_post',
                    'PostText': 'hello allowed',
                    'ContainsHello': True,
                    '__action_id': 431,
                    '__ban_user': ['user_acl_bool_druid|User said "hello"'],
                    '__entity_label_mutations': ['User/meow/1'],
                    '__error_count': 0,
                    '__timestamp': allowed_timestamp.isoformat(),
                },
            }
        ],
        'next_page': None,
    }

    payload = wait_for_json(
        lambda: post_json(
            client,
            'events.scan_query',
            {
                'start': '2026-04-08T19:00:00+00:00',
                'end': '2026-04-08T21:00:00+00:00',
                'query_filter': 'ContainsHello == true',
                'entity': None,
                'limit': 10,
            },
        ),
        lambda response_payload: response_payload == expected_payload,
    )

    assert payload == expected_payload


@pytest.mark.use_rules_sources(CONFIG_ACTION_FILTERED)
def test_druid_aggregate_event_endpoints_combine_boolean_query_with_action_acl_filter(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    seeded_events = [
        (441, datetime(2026, 4, 8, 22, 0, tzinfo=UTC), 'user_a_acl_druid', 'create_post', True),
        (442, datetime(2026, 4, 8, 22, 10, tzinfo=UTC), 'user_a_acl_druid', 'create_post', True),
        (443, datetime(2026, 4, 8, 22, 20, tzinfo=UTC), 'user_b_acl_druid', 'create_post', True),
        (444, datetime(2026, 4, 8, 22, 30, tzinfo=UTC), 'user_c_acl_druid', 'create_post', False),
        (445, datetime(2026, 4, 8, 22, 40, tzinfo=UTC), 'user_d_acl_druid', 'delete_post', True),
    ]

    for action_id, timestamp, user_id, action_name, contains_hello in seeded_events:
        seed_event(
            action_id=action_id,
            timestamp=timestamp,
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

    wait_for_json(
        lambda: post_json(
            client,
            'events.timeseries_query',
            {
                'start': '2026-04-08T22:00:00+00:00',
                'end': '2026-04-08T23:00:00+00:00',
                'query_filter': '',
                'entity': None,
                'granularity': 'hour',
            },
        ),
        lambda payload: isinstance(payload, list) and len(payload) == 1 and payload[0]['result']['count'] == 4,
    )

    base_request = {
        'start': '2026-04-08T22:00:00+00:00',
        'end': '2026-04-08T23:00:00+00:00',
        'query_filter': 'ContainsHello == true',
        'entity': None,
    }

    timeseries_payload = wait_for_json(
        lambda: post_json(client, 'events.timeseries_query', {**base_request, 'granularity': 'hour'}),
        lambda payload: isinstance(payload, list) and len(payload) == 1 and payload[0]['result']['count'] == 3,
    )
    assert len(timeseries_payload) == 1
    assert timeseries_payload[0]['result']['count'] == 3

    groupby_payload = wait_for_json(
        lambda: post_json(client, 'events.groupby_count', {**base_request, 'dimension': 'UserId'}),
        lambda payload: payload == {'count': 2},
    )
    assert groupby_payload == {'count': 2}

    topn_payload = wait_for_json(
        lambda: post_json(
            client, 'events.topn_query', {**base_request, 'dimension': 'UserId', 'limit': 10, 'precision': 0}
        ),
        lambda payload: payload is not None
        and payload.get('previous_period') is None
        and payload.get('comparison') is None
        and payload.get('current_period')
        and {item['UserId']: item['count'] for item in payload['current_period'][0]['result']}
        == {'user_a_acl_druid': 2, 'user_b_acl_druid': 1},
    )

    assert topn_payload['previous_period'] is None
    assert topn_payload['comparison'] is None
    assert {item['UserId']: item['count'] for item in topn_payload['current_period'][0]['result']} == {
        'user_a_acl_druid': 2,
        'user_b_acl_druid': 1,
    }


@pytest.mark.use_rules_sources(CONFIG_ACTION_FILTERED)
def test_druid_topn_csv_combines_boolean_query_with_action_acl_filter(
    app: Flask,
    client: 'FlaskClient[Response]',
    seed_event: Callable[..., Dict[str, Any]],
) -> None:
    seeded_events = [
        (451, datetime(2026, 4, 8, 23, 0, tzinfo=UTC), 'user_a_csv_druid', 'create_post', True),
        (452, datetime(2026, 4, 8, 23, 10, tzinfo=UTC), 'user_a_csv_druid', 'create_post', True),
        (453, datetime(2026, 4, 8, 23, 20, tzinfo=UTC), 'user_b_csv_druid', 'create_post', True),
        (454, datetime(2026, 4, 8, 23, 30, tzinfo=UTC), 'user_c_csv_druid', 'create_post', False),
        (455, datetime(2026, 4, 8, 23, 40, tzinfo=UTC), 'user_d_csv_druid', 'delete_post', True),
    ]

    for action_id, timestamp, user_id, action_name, contains_hello in seeded_events:
        seed_event(
            action_id=action_id,
            timestamp=timestamp,
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

    wait_for_json(
        lambda: post_json(
            client,
            'events.timeseries_query',
            {
                'start': '2026-04-08T23:00:00+00:00',
                'end': '2026-04-09T00:00:00+00:00',
                'query_filter': '',
                'entity': None,
                'granularity': 'hour',
            },
        ),
        lambda payload: isinstance(payload, list) and len(payload) == 1 and payload[0]['result']['count'] == 4,
    )

    csv_rows = wait_for_csv_rows(
        lambda: client.post(
            url_for('events.topn_query_csv'),
            json={
                'start': '2026-04-08T23:00:00+00:00',
                'end': '2026-04-09T00:00:00+00:00',
                'query_filter': 'ContainsHello == true',
                'entity': None,
                'dimension': 'UserId',
                'limit': 10,
                'precision': 0,
            },
        ),
        predicate=lambda rows: {row['UserId']: row['current_count'] for row in rows}
        == {'user_a_csv_druid': '2', 'user_b_csv_druid': '1'},
    )

    assert {row['UserId']: row['current_count'] for row in csv_rows} == {
        'user_a_csv_druid': '2',
        'user_b_csv_druid': '1',
    }
