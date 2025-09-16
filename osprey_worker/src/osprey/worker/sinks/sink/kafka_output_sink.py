from typing import Any

import sentry_sdk
from kafka import KafkaProducer

from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.sinks.sink.output_sink import BaseOutputSink

logger = get_logger()


class KafkaOutputSink(BaseOutputSink):
    """An output sink that sends the extracted features to a given kafka topic."""

    def __init__(self, kafka_topic: str, kafka_producer: KafkaProducer):
        self._kafka_topic = kafka_topic
        self._kafka_producer = kafka_producer

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    def push(self, result: ExecutionResult) -> None:
        kafka_future: Any = self._kafka_producer.send(
            topic=self._kafka_topic, value=result.extracted_features_json.encode('utf-8')
        )
        kafka_future.add_errback(self.push_err_to_sentry)

    @classmethod
    def push_err_to_sentry(cls, e: Exception) -> None:
        logger.error(f'exception raised when pushing event to kafka: {str(e)}')
        sentry_sdk.capture_exception(error=e)

    def stop(self) -> None:
        pass
