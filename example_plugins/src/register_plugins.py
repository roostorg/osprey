from typing import Any, Sequence, Type

from osprey.engine.udf.base import UDFBase
from osprey.worker.adaptor.plugin_manager import hookimpl_osprey
from osprey.worker.lib.config import Config
from osprey.worker.lib.storage.labels import LabelsServiceBase
from osprey.worker.sinks.sink.output_sink import BaseOutputSink, StdoutOutputSink
from output_sinks.clickhouse_execution_results_sink import ClickhouseExecutionResultsSink
from services.labels_service import PostgresLabelsService
from udfs.ban_user import BanUser
from udfs.text_contains import TextContains


@hookimpl_osprey
def register_udfs() -> Sequence[Type[UDFBase[Any, Any]]]:
    return [TextContains, BanUser]


@hookimpl_osprey
def register_output_sinks(config: Config) -> Sequence[BaseOutputSink]:
    return [StdoutOutputSink(log_sampler=None), ClickhouseExecutionResultsSink(config=config)]


@hookimpl_osprey
def register_labels_service_or_provider(config: Config) -> LabelsServiceBase:
    """Register a PostgreSQL-backed labels service."""
    return PostgresLabelsService()
