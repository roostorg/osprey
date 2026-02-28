from typing import Any, Sequence, Type

from osprey.engine.udf.base import UDFBase
from osprey.worker.adaptor.plugin_manager import hookimpl_osprey
from osprey.worker.lib.config import Config
from osprey.worker.lib.storage.labels import LabelsServiceBase
from osprey.worker.sinks.sink.output_sink import BaseOutputSink, StdoutOutputSink
from services.labels_service import PostgresLabelsService
from services.relay_manager_sink import RelayManagerSink
from services.zendesk_sink import ZendeskSink
from udfs.ban_nostr_event import BanNostrEvent
from udfs.check_moderation_result import CheckModerationResult
from udfs.nostr_account_age import NostrAccountAge


@hookimpl_osprey
def register_udfs() -> Sequence[Type[UDFBase[Any, Any]]]:
    return [BanNostrEvent, NostrAccountAge, CheckModerationResult]


@hookimpl_osprey
def register_output_sinks(config: Config) -> Sequence[BaseOutputSink]:
    return [
        StdoutOutputSink(log_sampler=None),
        RelayManagerSink(),
        ZendeskSink(),
    ]


@hookimpl_osprey
def register_labels_service_or_provider(config: Config) -> LabelsServiceBase:
    """Register a PostgreSQL-backed labels service."""
    return PostgresLabelsService()
