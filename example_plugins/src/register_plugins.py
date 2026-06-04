from typing import Any, Sequence, Type

from osprey.engine.udf.base import UDFBase
from osprey.worker.adaptor.plugin_manager import hookimpl_osprey
from osprey.worker.lib.config import Config
from osprey.worker.lib.storage.labels import LabelsServiceBase
from osprey.worker.sinks.sink.output_sink import BaseOutputSink, StdoutOutputSink
from services.labels_service import PostgresLabelsService
from udfs.ban_user import BanUser
from udfs.text_contains import TextContains


@hookimpl_osprey
def register_udfs() -> Sequence[Type[UDFBase[Any, Any]]]:
    return [TextContains, BanUser]


@hookimpl_osprey
def register_output_sinks(config: Config) -> Sequence[BaseOutputSink]:
    return [StdoutOutputSink(log_sampler=None)]


@hookimpl_osprey
def register_labels_service_or_provider(config: Config) -> LabelsServiceBase:
    """Register a PostgreSQL-backed labels service."""
    return PostgresLabelsService()


# NOTE: the `register_llm_provider` hook is intentionally NOT implemented here.
# An LLM provider is optional, and we don't want one registered by default. See
# `llm.anthropic_provider.AnthropicLLMProvider` for a reference implementation; a
# deployment that wants it can add its own `register_llm_provider` hookimpl.
