import platform
from typing import Any

import sentry_sdk
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.sinks.sink.output_sink import BaseOutputSink

logger = get_logger()


class EmptyBootstrapServersException(Exception):
    """Exception that is raised whenever the server list provided to KafkaOutputSink is empty."""


class InvalidOutputTopicException(Exception):
    """Exception that is raised whenever the output topic passed to KafkaOutputSink is empty."""


class KafkaOutputSink(BaseOutputSink):
    """An output sink that sends the extracted features to a given kafka topic."""

    def __init__(
        self,
        bootstrap_servers: list[str],
        output_topic: str,
        client_id: str | None,
        poll_every: int = 20,
    ) -> None:
        if len(bootstrap_servers) == 0:
            raise EmptyBootstrapServersException()

        if output_topic == '':
            raise InvalidOutputTopicException()

        self.logger = get_logger('KafkaOutputSink')

        self._bootstrap_servers = bootstrap_servers
        self._output_topic = output_topic
        self._poll_every = poll_every
        self._message_count = 0

        # NOTE(haileyok): this is...not necessary probably
        self.topic_ensured = False

        if client_id is None:
            client_hostname = platform.node()
            if client_hostname != '':
                client_id = f'{client_hostname};host_override={bootstrap_servers[0]}'
            else:
                client_id = f'osprey-output-sink;host_override={bootstrap_servers[0]}'

        self.logger.info(f'Creating Kafka producer with client id {client_id}')

        config = {
            'bootstrap.servers': ','.join(bootstrap_servers),
            'client.id': client_id,
            'queue.buffering.max.messages': 1_000_000,
            'linger.ms': 10,
            'retries': 10,
            'request.timeout.ms': 30_000,
            'socket.timeout.ms': 30_000,
            'delivery.timeout.ms': 120_000,
            'statistics.interval.ms': 10_000,
            'log.connection.close': False,
            'enable.idempotence': True,
            'acks': 'all',
            'max.in.flight.requests.per.connection': 5,
            'max.message.bytes': 20_000_000,
        }

        self.producer = Producer(config)
        self.ensure_topic()

        super().__init__()

    def ensure_topic(self) -> None:
        """Create the Kafka topic if it does not yet exist."""
        admin_client = AdminClient({'bootstrap.servers': ','.join(self._bootstrap_servers)})

        try:
            metadata = admin_client.list_topics(timeout=10)
        except Exception as e:
            self.logger.error(f'Error listing topics, unable to ensure topic: {e}')
            return

        if self._output_topic in metadata.topics:
            self.topic_ensured = True
            return

        self.logger.info(f'Creating topic {self._output_topic}')
        try:
            # TODO: num_partitions and replication_factor should not be hard coded...
            topic = NewTopic(self._output_topic, num_partitions=3, replication_factor=3)
            fs = admin_client.create_topics([topic])
            fs[self._output_topic].result()
            self.topic_ensured = True
        except Exception as e:
            self.logger.error(f'Error creating topic, unable to ensure topic: {e}')

    def _handle_polling(self) -> None:
        """Poll periodically based on message count"""
        self._message_count += 1
        if self._message_count % self._poll_every == 0:
            self.producer.poll(0)

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    def push(self, result: ExecutionResult) -> None:
        try:
            self.producer.produce(
                self._output_topic,
                value=result.extracted_features_json.encode('utf-8'),
                on_delivery=self._on_delivery,
            )
        except BufferError:
            self.logger.warning('Producer queue full, flushing before retry')
            self.producer.flush(timeout=5)
            self.producer.produce(
                self._output_topic,
                value=result.extracted_features_json.encode('utf-8'),
                on_delivery=self._on_delivery,
            )

        self._handle_polling()

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.push_err_to_sentry(err)

    def flush(self, timeout: float = 30) -> int:
        return self.producer.flush(timeout)

    @classmethod
    def push_err_to_sentry(cls, e: Exception) -> None:
        logger.error(f'exception raised when pushing event to kafka: {str(e)}')
        sentry_sdk.capture_exception(error=e)

    def stop(self) -> None:
        remaining = self.flush(timeout=10)
        if remaining > 0:
            logger.warning(f'{remaining} messages were not delivered')
