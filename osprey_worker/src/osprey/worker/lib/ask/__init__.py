"""Host-neutral Ask AI turn service.

This package drives a validated user turn through an LLM provider, a principal-scoped
tool registry, host persistence/locking, redaction, audit, and evidence adapters,
emitting a deterministic, bounded stream of versioned events. All product- and
vendor-specific behavior is supplied by the host through the Protocols in
:mod:`osprey.worker.lib.ask.ports`; nothing here imports host identity, product stores,
product query types, or a concrete vendor SDK.
"""

from osprey.worker.lib.ask.contracts import (
    ASK_EVENT_VERSION,
    AskEvent,
    AskEventType,
    AskLimits,
    AskRequest,
    ContextSnapshot,
    Conversation,
    Principal,
    ResolvedModel,
)
from osprey.worker.lib.ask.errors import (
    AskError,
    AskErrorCode,
    BudgetExceeded,
    Cancelled,
    Forbidden,
    InternalError,
    InvalidContext,
    InvalidModel,
    InvalidRequest,
    LockUnavailable,
    ProviderError,
    SerializationError,
    ToolExecutionError,
    UnavailableProvider,
    to_public_payload,
)
from osprey.worker.lib.ask.ports import (
    AskConfig,
    AuditSink,
    ContextSnapshotProvider,
    ConversationLock,
    ConversationStore,
    EvidenceNormalizer,
    ModelPolicy,
    Redactor,
    ToolRegistryFactory,
)
from osprey.worker.lib.ask.service import AskService

__all__ = [
    'ASK_EVENT_VERSION',
    'AskEvent',
    'AskEventType',
    'AskLimits',
    'AskRequest',
    'ContextSnapshot',
    'Conversation',
    'Principal',
    'ResolvedModel',
    'AskError',
    'AskErrorCode',
    'BudgetExceeded',
    'Cancelled',
    'Forbidden',
    'InternalError',
    'InvalidContext',
    'InvalidModel',
    'InvalidRequest',
    'LockUnavailable',
    'ProviderError',
    'SerializationError',
    'ToolExecutionError',
    'UnavailableProvider',
    'to_public_payload',
    'AskConfig',
    'AuditSink',
    'ContextSnapshotProvider',
    'ConversationLock',
    'ConversationStore',
    'EvidenceNormalizer',
    'ModelPolicy',
    'Redactor',
    'ToolRegistryFactory',
    'AskService',
]
