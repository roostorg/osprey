from typing import List, Sequence

from kafka import KafkaProducer
from osprey.worker.adaptor.plugin_manager import hookimpl_osprey
from osprey.worker.lib.config import Config
from osprey.worker.lib.storage import ExecutionResultStorageBackendType
from osprey.worker.lib.storage.stored_execution_result import get_rules_execution_result_storage_backend
from osprey.worker.sinks.sink.kafka_output_sink import KafkaOutputSink
from osprey.worker.sinks.sink.output_sink import BaseOutputSink, StdoutOutputSink
from osprey.worker.sinks.sink.stored_execution_result_output_sink import StoredExecutionResultOutputSink


@hookimpl_osprey
def register_output_sinks(config: Config) -> Sequence[BaseOutputSink]:
    sinks: List[BaseOutputSink] = []
    if config.get_bool('OSPREY_STDOUT_OUTPUT_SINK', False):
        sinks.append(StdoutOutputSink())
    if config.get_bool('OSPREY_KAFKA_OUTPUT_SINK', False):
        output_topic = config.expect_str('OSPREY_KAFKA_OUTPUT_TOPIC')
        bootstrap_servers = config.expect_str_list('OSPREY_KAFKA_BOOTSTRAP_SERVERS')
        client_id = config.expect_str('OSPREY_KAFKA_OUTPUT_CLIENT_ID')
        sinks.append(
            KafkaOutputSink(
                kafka_topic=output_topic,
                kafka_producer=KafkaProducer(bootstrap_servers=bootstrap_servers, client_id=client_id),
            )
        )

    storage_backend_type = ExecutionResultStorageBackendType(
        config.get_str('OSPREY_EXECUTION_RESULT_STORAGE_BACKEND', 'none')
    )
    storage_backend = get_rules_execution_result_storage_backend(backend_type=storage_backend_type)

    # There may not be an execution result store configured, so check before adding the output sink
    if storage_backend is not None:
        sinks.append(StoredExecutionResultOutputSink())

    return sinks
