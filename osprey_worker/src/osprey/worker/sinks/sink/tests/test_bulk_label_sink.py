from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from unittest.mock import MagicMock, call

import pytest
from osprey.engine.ast.sources import Sources
from osprey.worker.adaptor.plugin_manager import bootstrap_ast_validators, bootstrap_udfs
from osprey.worker.lib.bulk_label import TaskStatus
from osprey.worker.lib.osprey_engine import OspreyEngine
from osprey.worker.lib.osprey_shared.labels import LabelStatus
from osprey.worker.lib.sources_provider import StaticSourcesProvider
from osprey.worker.lib.storage.bulk_label_task import MAX_ATTEMPTS, BulkLabelTask
from osprey.worker.sinks.sink.bulk_label_sink import (
    BULK_LABEL_NO_LIMIT_SIZE,
    DEFAULT_BULK_LABEL_COLLECTING_HEARTBEAT,
    NO_LIMIT_BULK_LABEL_COLLECTING_HEARTBEAT,
    NO_LIMIT_TOP_N_QUERY_TIME_DELTA_MAX,
    BulkLabelSink,
    UnretryableTaskException,
)
from osprey.worker.ui_api.osprey.lib.druid import TopNDruidQuery
from pytest_mock import MockFixture

from ..input_stream import StaticInputStream

# Druid might also return null/empty values, we need to make sure we handle those in our sink.
_TASK_NULLISH_ENTITIES: list[dict[str, str | None]] = [{'UserId': None}, {'UserId': ''}]
_TASK_TOTAL_VALID_ENTITIES = 10
_TASK_TOTAL_ENTITIES_RETURNED = _TASK_TOTAL_VALID_ENTITIES + len(_TASK_NULLISH_ENTITIES)


@pytest.fixture(autouse=True)
def mock_top_n_druid_query(mocker: MockFixture) -> None:
    execute = mocker.patch('osprey.worker.sinks.sink.bulk_label_sink.TopNDruidQuery.execute')
    fake_result: list[dict[str, str | None]] = [{'UserId': str(x)} for x in range(_TASK_TOTAL_VALID_ENTITIES)]
    fake_result += _TASK_NULLISH_ENTITIES

    execute.return_value = ({'result': fake_result},)


@dataclass(frozen=True)
class BulkLabelSinkAndMocks:
    sink: BulkLabelSink
    task: BulkLabelTask
    heartbeat_mock: MagicMock
    release_mock: MagicMock
    labels_provider_mock: MagicMock
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

    provider = StaticSourcesProvider(sources=Sources.from_dict({'main.sml': "UserId = Entity(type='User', id='1')"}))
    udf_registry, _ = bootstrap_udfs()
    bootstrap_ast_validators()

    engine = OspreyEngine(sources_provider=provider, udf_registry=udf_registry)

    labels_provider_mock = MagicMock()
    bulk_label_sink = BulkLabelSink(
        StaticInputStream([task]),
        labels_provider=labels_provider_mock,
        analytics_publisher=MagicMock(),
        engine=engine,
    )

    return BulkLabelSinkAndMocks(
        sink=bulk_label_sink,
        task=task,
        heartbeat_mock=heartbeat_mock,
        release_mock=release_mock,
        labels_provider_mock=labels_provider_mock,
        analytics_mock=analytics_mock,
    )


def test_bulk_label_golden_path() -> None:
    sink_and_mocks = create_bulk_label_sink_with_single_task()

    sink_and_mocks.sink.run()

    assert sink_and_mocks.labels_provider_mock.apply_entity_label_mutations.call_count == _TASK_TOTAL_VALID_ENTITIES
    # Extract entity keys from the mock calls
    entity_keys = []
    for call_args in sink_and_mocks.labels_provider_mock.apply_entity_label_mutations.call_args_list:
        entity = call_args.kwargs['entity']
        entity_keys.append((entity.type, entity.id))

    expected_entity_keys = [
        ('User', '0'),
        ('User', '1'),
        ('User', '2'),
        ('User', '3'),
        ('User', '4'),
        ('User', '5'),
        ('User', '6'),
        ('User', '7'),
        ('User', '8'),
        ('User', '9'),
    ]
    # We have to check that the lists are equal, but unordered.
    expected_keys_as_tuples = set(expected_entity_keys)
    actual_keys_as_tuples = set(entity_keys)
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
    sink_and_mocks.labels_provider_mock.apply_entity_label_mutations.side_effect = exc

    sink_and_mocks.sink.run()

    sink_and_mocks.heartbeat_mock.assert_called_once_with(
        status=TaskStatus.COLLECTING, new_entity_count=0, claim_until_seconds=DEFAULT_BULK_LABEL_COLLECTING_HEARTBEAT
    )
    sink_and_mocks.release_mock.assert_called_once_with(status=TaskStatus.RETRYING, result=repr(exc))
    sink_and_mocks.analytics_mock.assert_not_called()


def test_bulk_label_fails() -> None:
    sink_and_mocks = create_bulk_label_sink_with_single_task(attempts=MAX_ATTEMPTS + 1)
    exc = Exception('fake')
    sink_and_mocks.labels_provider_mock.apply_entity_label_mutations.side_effect = exc

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

    assert (
        sink_and_mocks.labels_provider_mock.apply_entity_label_mutations.call_count
        == _TASK_TOTAL_VALID_ENTITIES - len(excluded_entities)
    )

    # Extract entity keys from the mock calls
    entity_keys = []
    for call_args in sink_and_mocks.labels_provider_mock.apply_entity_label_mutations.call_args_list:
        entity = call_args.kwargs['entity']
        entity_keys.append(entity.id)

    included_entities_set = {'1', '3', '5', '7', '9'}
    entities_labeled = set(entity_keys)
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
        entity=None,
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
        entity=None,
    )

    expected_topN_query_two = TopNDruidQuery(
        start=datetime.fromtimestamp(query['start'] + NO_LIMIT_TOP_N_QUERY_TIME_DELTA_MAX),
        end=datetime.fromtimestamp(query['end']),
        query_filter=query['query_filter'],
        dimension=sink_and_mocks.task.dimension,
        limit=BULK_LABEL_NO_LIMIT_SIZE,
        entity=None,
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
