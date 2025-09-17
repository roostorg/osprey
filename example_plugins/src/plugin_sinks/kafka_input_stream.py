import json
from typing import Iterator

import sentry_sdk
from kafka.consumer.fetcher import ConsumerRecord
from kafka.consumer.group import RoundRobinPartitionAssignor
from osprey.engine.executor.execution_context import Action
from osprey.worker.lib.config import Config
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.utils.dates import parse_go_timestamp
from osprey.worker.sinks.sink.input_stream import BaseInputStream
from osprey.worker.sinks.utils.acking_contexts import (
    BaseAckingContext,
    NoopAckingContext,
)
from osprey.worker.sinks.utils.kafka import PatchedKafkaConsumer

logger = get_logger()


class KafkaInputStream(BaseInputStream[BaseAckingContext[Action]]):
    """An input stream that consumes messages from a Kafka topic and yields Action objects wrapped in an AckingContext."""

    def __init__(self, config: Config):
        super().__init__()

        client_id = config.get_str('OSPREY_KAFKA_INPUT_STREAM_CLIENT_ID', 'localhost')
        input_topic: str = config.get_str('OSPREY_KAFKA_INPUT_STREAM_TOPIC', 'osprey.actions_input')
        input_bootstrap_servers: list[str] = config.get_str_list('OSPREY_KAFKA_BOOTSTRAP_SERVERS', ['localhost'])
        group_id = config.get_optional_str('OSPREY_KAFKA_GROUP_ID')

        self._consumer = PatchedKafkaConsumer(
            input_topic,
            bootstrap_servers=input_bootstrap_servers,
            client_id=client_id,
            group_id=group_id,
            partition_assignment_strategy=(RoundRobinPartitionAssignor,),
        )

    def _gen(self) -> Iterator[BaseAckingContext[Action]]:
        while True:
            try:
                with metrics.timed('kafka_consumer.lock_time'):
                    with metrics.timed('kafka_consumer.poll_time'):
                        record: ConsumerRecord = next(self._consumer)
                data = json.loads(record.value)
                timestamp = parse_go_timestamp(data['send_time'])
                action_data = data['data']
                # this was here for when this was protobuf. If its json by default, we should just assume its all in one
                # json blob.
                # action_data = json.loads(action_data_json)

                action = Action(
                    action_id=int(action_data['action_id']),
                    action_name=action_data['action_name'],
                    data=action_data['data'],
                    timestamp=timestamp,
                )
                # Wrap in NoopAckingContext for now, or implement a KafkaAckingContext if needed
                yield NoopAckingContext(action)
            except Exception as e:
                logger.exception('Error while consuming from Kafka')
                sentry_sdk.capture_exception(e)
                continue
