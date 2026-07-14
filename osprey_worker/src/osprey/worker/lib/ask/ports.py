"""Host adapter Protocols and the AskConfig ports bundle.

The host supplies concrete implementations of these Protocols; the generic package
defines only the interfaces (and test fakes). See README section 3.4 for the invariant
each port must uphold. Nothing here imports host identity, product stores, product
query types, or a concrete vendor SDK.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Protocol, Sequence, runtime_checkable

from osprey.worker.lib.ask.contracts import (
    AskLimits,
    AskRequest,
    ContextSnapshot,
    Conversation,
    Principal,
    ResolvedModel,
)
from osprey.worker.lib.llm import LLMResponse, ToolCall, ToolRegistry, ToolResult


@runtime_checkable
class ModelPolicy(Protocol):
    def resolve(self, requested_model: Optional[str], principal: Principal) -> ResolvedModel:
        """Validate the requested model and return a ready provider + limits.

        Raise ``InvalidModel`` for a rejected model. Return ``provider=None`` (or
        raise ``UnavailableProvider``) when no provider is available or it cannot
        initialize -- both map to a pre-flight ``unavailable_provider``.
        """
        ...


@runtime_checkable
class ConversationStore(Protocol):
    def load(self, conversation_id: str, principal: Principal) -> Optional[Conversation]:
        """Load a conversation, enforcing principal ownership.

        Return ``None`` when the id is unknown. Raise ``Forbidden`` when the
        conversation is owned by another principal.
        """
        ...

    def create(self, principal: Principal) -> Conversation:
        """Create and return a new conversation owned by ``principal``."""
        ...

    def append_assistant_turn(
        self,
        conversation: Conversation,
        request: AskRequest,
        response: LLMResponse,
        evidence: Sequence[Mapping[str, Any]],
    ) -> None:
        """Persist a successful assistant turn. Called only on the success path."""
        ...


@runtime_checkable
class ConversationLock(Protocol):
    def acquire(self, conversation_id: str, principal: Principal) -> AbstractContextManager[None]:
        """Return a context manager serializing turns for this conversation.

        Raise ``LockUnavailable`` when a turn is already in flight. The manager is
        released on every terminal path.
        """
        ...


@runtime_checkable
class ContextSnapshotProvider(Protocol):
    def resolve(self, context_ref: str, principal: Principal) -> Optional[ContextSnapshot]:
        """Validate a ``context_ref`` and return a snapshot, or ``None`` if invalid."""
        ...

    def render(self, snapshot: ContextSnapshot) -> str:
        """Render a snapshot to grounding text (truncated to limits by the service)."""
        ...


@runtime_checkable
class ToolRegistryFactory(Protocol):
    def build(self, principal: Principal, snapshot: Optional[ContextSnapshot]) -> ToolRegistry:
        """Build a principal-scoped tool registry for a turn."""
        ...


@runtime_checkable
class Redactor(Protocol):
    def redact_text(self, text: str) -> str:
        """Redact a plain string before it crosses the transport boundary."""
        ...

    def redact_payload(self, value: Any) -> Any:
        """Recursively redact a JSON-compatible payload (dict/list/str/scalar)."""
        ...


@runtime_checkable
class AuditSink(Protocol):
    def record(
        self,
        principal: Principal,
        conversation_id: str,
        turn_id: str,
        outcome: str,
        detail: Mapping[str, Any],
    ) -> None:
        """Record a terminal outcome. Best-effort; failures must not break the stream."""
        ...


@runtime_checkable
class EvidenceNormalizer(Protocol):
    def normalize(self, tool_call: ToolCall, tool_result: ToolResult) -> dict[str, Any]:
        """Map a (tool_call, tool_result) pair to a ``query_result`` payload."""
        ...


@dataclass
class AskConfig:
    """The ports + limits bundle an :class:`AskService` is constructed with."""

    policy: ModelPolicy
    conversations: ConversationStore
    lock: ConversationLock
    tools: ToolRegistryFactory
    limits: AskLimits = AskLimits()
    context: Optional[ContextSnapshotProvider] = None
    redactor: Optional[Redactor] = None
    audit: Optional[AuditSink] = None
    evidence: Optional[EvidenceNormalizer] = None
    system_prompt: Optional[str] = None
