from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Sequence
from unittest.mock import MagicMock, call

import pytest
from osprey.engine.ast.sources import Sources
from osprey.rpc.labels.v1.service_pb2 import EntityKey, LabelStatus
from osprey.worker.lib.bulk_label import TaskStatus
from osprey.worker.lib.osprey_engine import bootstrap_engine
from osprey.worker.lib.sources_provider import StaticSourcesProvider
from osprey.worker.lib.storage.bulk_label_task import MAX_ATTEMPTS, BulkLabelTask
from osprey.worker.sinks.sink.bulk_label_sink import (
    BULK_LABEL_NO_LIMIT_SIZE,
    DEFAULT_BULK_LABEL_COLLECTING_HEARTBEAT,
    NO_LIMIT_BULK_LABEL_COLLECTING_HEARTBEAT,
    NO_LIMIT_TOP_N_QUERY_TIME_DELTA_MAX,
)
from osprey.worker.ui_api.osprey.lib.druid import TopNDruidQuery
from pytest_mock import MockFixture

from ..bulk_label_sink import BulkLabelSink, UnretryableTaskException
from ..input_stream import StaticInputStream

# Druid might also return null/empty values, we need to make sure we handle those in our sink.
_TASK_NULLISH_ENTITIES: List[Dict[str, Optional[str]]] = [{'UserId': None}, {'UserId': ''}]
_TASK_TOTAL_VALID_ENTITIES = 10
_TASK_TOTAL_ENTITIES_RETURNED = _TASK_TOTAL_VALID_ENTITIES + len(_TASK_NULLISH_ENTITIES)


@pytest.fixture(autouse=True)
def mock_top_n_druid_query(mocker: MockFixture) -> None:
    execute = mocker.patch('osprey.worker.sinks.sink.bulk_label_sink.TopNDruidQuery.execute')
    fake_result: List[Dict[str, Optional[str]]] = [{'UserId': str(x)} for x in range(_TASK_TOTAL_VALID_ENTITIES)]
    fake_result += _TASK_NULLISH_ENTITIES

    execute.return_value = ({'result': fake_result},)


@dataclass(frozen=True)
class BulkLabelSinkAndMocks:
    sink: BulkLabelSink
    task: BulkLabelTask
    heartbeat_mock: MagicMock
    release_mock: MagicMock
    event_effects_output_sink_mock: MagicMock
    analytics_mock: MagicMock


def create_bulk_label_sink_with_single_task(
    excluded_entities: Sequence[str] = (),
    attempts: int = 1,
    expected_total_entities_to_label: int = _TASK_TOTAL_VALID_ENTITIES,
    no_limit: bool = False,
    multiple_queries: bool = False,
) -> BulkLabelSinkAndMocks:
    """Constructs a BulkLabelSink with a single task in its input stream.

    Returns the sink and mocks for various aspects of the sink.
    """
    start_timestamp = round(datetime.now().timestamp())
    end_timestamp = start_timestamp + NO_LIMIT_TOP_N_QUERY_TIME_DELTA_MAX + 1 if multiple_queries else start_timestamp
    task = BulkLabelTask(
        query={
            'query_filter': 'fake',
            # this is dumb but basically it rounds off the millis and adds a timezone to the
            # datetime object.
            'start': start_timestamp,
            'end': end_timestamp,
        },
        initiated_by='test@test.com',
        dimension='UserId',
        excluded_entities=excluded_entities,
        expected_total_entities_to_label=expected_total_entities_to_label,
        no_limit=no_limit,
        label_name='fake',
        label_reason='fake',
        label_status=LabelStatus.MANUALLY_ADDED,
        entities_labeled=0,
        claim_until=datetime.now(),
        created_at=datetime.now(),
        updated_at=datetime.now(),
        attempts=attempts,
    )
    heartbeat_mock = MagicMock()
    release_mock = MagicMock()
    analytics_mock = MagicMock()
    setattr(task, 'heartbeat', heartbeat_mock)
    setattr(task, 'release', release_mock)
    setattr(BulkLabelSink, '_send_bulk_job_analytics', analytics_mock)

    sources_provider = StaticSourcesProvider(
        sources=Sources.from_dict({'main.sml': "UserId = Entity(type='User', id='1')"})
    )

    engine = bootstrap_engine(sources_provider=sources_provider)

    event_effects_output_sink_mock = MagicMock()
    bulk_label_sink = BulkLabelSink(
        StaticInputStream([task]),
        event_effects_output_sink=event_effects_output_sink_mock,
        analytics_publisher=MagicMock(),
        engine=engine,
    )

    return BulkLabelSinkAndMocks(
        sink=bulk_label_sink,
        task=task,
        heartbeat_mock=heartbeat_mock,
        release_mock=release_mock,
        event_effects_output_sink_mock=event_effects_output_sink_mock,
        analytics_mock=analytics_mock,
    )


def test_bulk_label_golden_path() -> None:
    sink_and_mocks = create_bulk_label_sink_with_single_task()

    sink_and_mocks.sink.run()

    assert (
        sink_and_mocks.event_effects_output_sink_mock.apply_label_mutations_pb2.call_count == _TASK_TOTAL_VALID_ENTITIES
    )
    event_keys = [
        kwargs['entity_key']
        for args, kwargs in sink_and_mocks.event_effects_output_sink_mock.apply_label_mutations_pb2.call_args_list
    ]
    expected_entity_keys = [
        EntityKey(type='User', id='0'),
        EntityKey(type='User', id='1'),
        EntityKey(type='User', id='2'),
        EntityKey(type='User', id='3'),
        EntityKey(type='User', id='4'),
        EntityKey(type='User', id='5'),
        EntityKey(type='User', id='6'),
        EntityKey(type='User', id='7'),
        EntityKey(type='User', id='8'),
        EntityKey(type='User', id='9'),
    ]
    # We have to check that the lists are equal, but unordered.
    # We cant use a set because the proto EntityKey object is not
    # hashable :(
    expected_keys_as_tuples = {(key.type, key.id) for key in expected_entity_keys}
    actual_keys_as_tuples = {(key.type, key.id) for key in event_keys}
    assert actual_keys_as_tuples == expected_keys_as_tuples

    sink_and_mocks.release_mock.assert_called_once_with(status=TaskStatus.COMPLETE)
    sink_and_mocks.heartbeat_mock.assert_has_calls(
        [
            call(
                status=TaskStatus.COLLECTING,
                new_entity_count=0,
                claim_until_seconds=DEFAULT_BULK_LABEL_COLLECTING_HEARTBEAT,
            ),
            call(status=TaskStatus.LABELLING, new_entity_count=_TASK_TOTAL_VALID_ENTITIES),
        ]
    )
    sink_and_mocks.analytics_mock.assert_called_once()


def test_bulk_label_retries() -> None:
    sink_and_mocks = create_bulk_label_sink_with_single_task()
    exc = Exception('fake')
    sink_and_mocks.event_effects_output_sink_mock.apply_label_mutations_pb2.side_effect = exc

    sink_and_mocks.sink.run()

    sink_and_mocks.heartbeat_mock.assert_called_once_with(
        status=TaskStatus.COLLECTING, new_entity_count=0, claim_until_seconds=DEFAULT_BULK_LABEL_COLLECTING_HEARTBEAT
    )
    sink_and_mocks.release_mock.assert_called_once_with(status=TaskStatus.RETRYING, result=repr(exc))
    sink_and_mocks.analytics_mock.assert_not_called()


def test_bulk_label_fails() -> None:
    sink_and_mocks = create_bulk_label_sink_with_single_task(attempts=MAX_ATTEMPTS + 1)
    exc = Exception('fake')
    sink_and_mocks.event_effects_output_sink_mock.apply_label_mutations_pb2.side_effect = exc

    sink_and_mocks.sink.run()

    sink_and_mocks.heartbeat_mock.assert_called_once_with(
        status=TaskStatus.COLLECTING, new_entity_count=0, claim_until_seconds=DEFAULT_BULK_LABEL_COLLECTING_HEARTBEAT
    )
    sink_and_mocks.release_mock.assert_called_once_with(status=TaskStatus.FAILED, result=repr(exc))
    sink_and_mocks.analytics_mock.assert_not_called()


def test_bulk_label_golden_path_exclude_ids() -> None:
    excluded_entities = ['0', '2', '4', '6', '8']
    sink_and_mocks = create_bulk_label_sink_with_single_task(excluded_entities=excluded_entities)

    sink_and_mocks.sink.run()

    apply_label_mutations = sink_and_mocks.event_effects_output_sink_mock.apply_label_mutations_pb2

    assert apply_label_mutations.call_count == _TASK_TOTAL_VALID_ENTITIES - len(excluded_entities)

    included_entities_set = {'1', '3', '5', '7', '9'}
    entities_labeled = {k['entity_key'].id for _, k in apply_label_mutations.call_args_list}
    assert included_entities_set == entities_labeled

    sink_and_mocks.heartbeat_mock.assert_has_calls(
        [
            call(
                status=TaskStatus.COLLECTING,
                new_entity_count=0,
                claim_until_seconds=DEFAULT_BULK_LABEL_COLLECTING_HEARTBEAT,
            ),
            call(status=TaskStatus.LABELLING, new_entity_count=_TASK_TOTAL_VALID_ENTITIES - len(excluded_entities)),
        ]
    )
    sink_and_mocks.release_mock.assert_called_once_with(status=TaskStatus.COMPLETE)
    sink_and_mocks.analytics_mock.assert_called_once()


def test_bulk_label_entity_mismatch() -> None:
    sink_and_mocks = create_bulk_label_sink_with_single_task(expected_total_entities_to_label=5)

    sink_and_mocks.sink.run()

    exc = UnretryableTaskException(f'Expected 5 entities, got {_TASK_TOTAL_VALID_ENTITIES} (margin of error: 100%)')
    sink_and_mocks.heartbeat_mock.assert_called_once_with(
        status=TaskStatus.COLLECTING, new_entity_count=0, claim_until_seconds=DEFAULT_BULK_LABEL_COLLECTING_HEARTBEAT
    )
    sink_and_mocks.release_mock.assert_called_once_with(status=TaskStatus.FAILED, result=repr(exc))
    sink_and_mocks.analytics_mock.assert_not_called()


# Test bulk label bypasses entity mismatch check for 0 expected entities and builds correct query limit
def test_bulk_label_no_limit() -> None:
    sink_and_mocks = create_bulk_label_sink_with_single_task(expected_total_entities_to_label=0, no_limit=True)

    query = sink_and_mocks.task.query
    assert isinstance(query, dict)

    expected_topN_query = TopNDruidQuery(
        start=datetime.fromtimestamp(query['start']),
        end=datetime.fromtimestamp(query['end']),
        query_filter=query['query_filter'],
        dimension=sink_and_mocks.task.dimension,
        limit=BULK_LABEL_NO_LIMIT_SIZE,
    )

    assert sink_and_mocks.sink._build_top_n_queries(sink_and_mocks.task) == [expected_topN_query]
    sink_and_mocks.sink.run()

    sink_and_mocks.heartbeat_mock.assert_has_calls(
        [
            call(
                status=TaskStatus.COLLECTING,
                new_entity_count=0,
                claim_until_seconds=NO_LIMIT_BULK_LABEL_COLLECTING_HEARTBEAT,
            ),
            call(status=TaskStatus.LABELLING, new_entity_count=_TASK_TOTAL_VALID_ENTITIES),
        ]
    )
    sink_and_mocks.release_mock.assert_called_once_with(status=TaskStatus.COMPLETE)
    sink_and_mocks.analytics_mock.assert_called_once()


def test_bulk_label_no_limit_multiple_queries() -> None:
    sink_and_mocks = create_bulk_label_sink_with_single_task(
        expected_total_entities_to_label=0, no_limit=True, multiple_queries=True
    )

    query = sink_and_mocks.task.query
    assert isinstance(query, dict)

    expected_topN_query_one = TopNDruidQuery(
        start=datetime.fromtimestamp(query['start']),
        end=datetime.fromtimestamp(query['start'] + NO_LIMIT_TOP_N_QUERY_TIME_DELTA_MAX),
        query_filter=query['query_filter'],
        dimension=sink_and_mocks.task.dimension,
        limit=BULK_LABEL_NO_LIMIT_SIZE,
    )

    expected_topN_query_two = TopNDruidQuery(
        start=datetime.fromtimestamp(query['start'] + NO_LIMIT_TOP_N_QUERY_TIME_DELTA_MAX),
        end=datetime.fromtimestamp(query['end']),
        query_filter=query['query_filter'],
        dimension=sink_and_mocks.task.dimension,
        limit=BULK_LABEL_NO_LIMIT_SIZE,
    )

    assert sink_and_mocks.sink._build_top_n_queries(sink_and_mocks.task) == [
        expected_topN_query_one,
        expected_topN_query_two,
    ]
    sink_and_mocks.sink.run()

    sink_and_mocks.heartbeat_mock.assert_has_calls(
        [
            call(  # The first collecting call is from the _build_top_n_queries test
                status=TaskStatus.COLLECTING,
                new_entity_count=0,
                claim_until_seconds=NO_LIMIT_BULK_LABEL_COLLECTING_HEARTBEAT,
            ),
            call(  # The second is from the _build_top_n_queries within run()
                status=TaskStatus.COLLECTING,
                new_entity_count=0,
                claim_until_seconds=NO_LIMIT_BULK_LABEL_COLLECTING_HEARTBEAT,
            ),
            call(status=TaskStatus.COLLECTING, new_entity_count=_TASK_TOTAL_VALID_ENTITIES),
            call(status=TaskStatus.LABELLING, new_entity_count=_TASK_TOTAL_VALID_ENTITIES),
        ]
    )
    sink_and_mocks.release_mock.assert_called_once_with(status=TaskStatus.COMPLETE)
    sink_and_mocks.analytics_mock.assert_called_once()
