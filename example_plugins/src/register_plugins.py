from collections.abc import Sequence
from typing import Any, Type

from llm.anthropic_provider import AnthropicLLMProvider
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


@hookimpl_osprey
def register_llm_provider(config: Config) -> AnthropicLLMProvider:
    """Register the example Anthropic-backed LLM provider.

    Construction is cheap and does not import the ``anthropic`` SDK or require an API
    key; readiness is deferred to the first ``chat()`` call.
    """
    return AnthropicLLMProvider(config)
