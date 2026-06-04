from typing import Any, Sequence, Type

from llm.anthropic_provider import AnthropicLLMProvider
from osprey.engine.udf.base import UDFBase
from osprey.worker.adaptor.plugin_manager import hookimpl_osprey
from osprey.worker.lib.config import Config
from osprey.worker.lib.llm.base import BaseLLMProvider
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
def register_llm_provider(config: Config) -> BaseLLMProvider:
    """Register a direct Anthropic API LLM provider.

    Requires the ``anthropic`` SDK (installed manually, see
    :mod:`llm.anthropic_provider`) and an API key, but only when the provider is
    actually invoked.
    """
    return AnthropicLLMProvider(config)
