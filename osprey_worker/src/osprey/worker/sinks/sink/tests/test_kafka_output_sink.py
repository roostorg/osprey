from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest
from osprey.worker.sinks.sink.kafka_output_sink import (
    EmptyBootstrapServersException,
    InvalidOutputTopicException,
    KafkaOutputSink,
)

MODULE = 'osprey.worker.sinks.sink.kafka_output_sink'
BOOTSTRAP_SERVERS = ['localhost:9092']
OUTPUT_TOPIC = 'osprey.execution_results'


@pytest.fixture(autouse=True)
def no_real_producer() -> Iterator[None]:
    """Keep the tests off the network, and off the real OS thread ThreadedKafkaProducer spawns."""
    with patch(f'{MODULE}.Producer'), patch(f'{MODULE}.ThreadedKafkaProducer'):
        yield


@pytest.fixture
def admin_client_cls() -> Iterator[MagicMock]:
    """Patches AdminClient. Defaults to a cluster with no topics on it."""
    with patch(f'{MODULE}.AdminClient') as cls:
        cls.return_value.list_topics.return_value.topics = {}
        yield cls


def make_sink(
    auto_create_topic: bool = True,
    num_partitions: int = 1,
    replication_factor: int = 1,
) -> KafkaOutputSink:
    return KafkaOutputSink(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        output_topic=OUTPUT_TOPIC,
        client_id='test-client',
        auto_create_topic=auto_create_topic,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )


def test_auto_create_topic_disabled_never_talks_to_the_admin_api(admin_client_cls: MagicMock) -> None:
    """Deployments that manage topics via terraform shouldn't need kafka admin rights at all."""
    sink = make_sink(auto_create_topic=False)

    admin_client_cls.assert_not_called()
    assert sink.topic_ensured is False


def test_missing_topic_is_created_with_the_configured_partitions(admin_client_cls: MagicMock) -> None:
    sink = make_sink(auto_create_topic=True, num_partitions=6, replication_factor=3)

    (created,) = admin_client_cls.return_value.create_topics.call_args.args[0]
    assert created.topic == OUTPUT_TOPIC
    assert created.num_partitions == 6
    assert created.replication_factor == 3
    assert sink.topic_ensured is True


def test_existing_topic_is_left_alone(admin_client_cls: MagicMock) -> None:
    admin_client_cls.return_value.list_topics.return_value.topics = {OUTPUT_TOPIC: MagicMock()}

    sink = make_sink(auto_create_topic=True)

    admin_client_cls.return_value.create_topics.assert_not_called()
    assert sink.topic_ensured is True


def test_unreachable_broker_does_not_break_startup(admin_client_cls: MagicMock) -> None:
    admin_client_cls.return_value.list_topics.side_effect = Exception('no brokers available')

    sink = make_sink(auto_create_topic=True)

    admin_client_cls.return_value.create_topics.assert_not_called()
    assert sink.topic_ensured is False


def test_failed_topic_creation_does_not_break_startup(admin_client_cls: MagicMock) -> None:
    future = MagicMock()
    future.result.side_effect = Exception('topic authorization failed')
    admin_client_cls.return_value.create_topics.return_value = {OUTPUT_TOPIC: future}

    sink = make_sink(auto_create_topic=True)

    assert sink.topic_ensured is False


def test_empty_bootstrap_servers_is_rejected() -> None:
    with pytest.raises(EmptyBootstrapServersException):
        KafkaOutputSink(
            bootstrap_servers=[],
            output_topic=OUTPUT_TOPIC,
            client_id='test-client',
            auto_create_topic=False,
            num_partitions=1,
            replication_factor=1,
        )


def test_empty_output_topic_is_rejected() -> None:
    with pytest.raises(InvalidOutputTopicException):
        KafkaOutputSink(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            output_topic='',
            client_id='test-client',
            auto_create_topic=False,
            num_partitions=1,
            replication_factor=1,
        )
