"""Vendor-neutral Ask AI domain contracts.

Request, event, principal, conversation, and limit types shared across the Ask
service and its transports. No host, product, or vendor specifics live here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Literal, Mapping, Optional

from osprey.worker.lib.llm import BaseLLMProvider, LLMMessage

ASK_EVENT_VERSION = 1

AskEventType = Literal[
    'conversation_started',
    'tool_call',
    'query_result',
    'assistant_message',
    'done',
    'error',
]


@dataclass(frozen=True)
class Principal:
    """The server-authenticated actor a turn is scoped to.

    Derived by the transport from server-side identity; it is never read from the
    client request body. Ownership, permissions, and tool authorization are all
    keyed off this principal.
    """

    id: str
    email: str
    display_name: Optional[str] = None
    attributes: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class AskRequest:
    """A single user turn request. ``message`` is required and non-empty."""

    message: str
    conversation_id: Optional[str] = None
    model: Optional[str] = None
    context_ref: Optional[str] = None


@dataclass
class AskEvent:
    """A versioned event emitted while a turn streams.

    ``type`` selects the event; ``payload`` carries type-specific data. Every event
    carries the schema ``version`` so clients and hosts can evolve independently.
    """

    type: AskEventType
    payload: dict[str, Any] = field(default_factory=dict)
    conversation_id: Optional[str] = None
    turn_id: Optional[str] = None
    version: int = ASK_EVENT_VERSION


@dataclass(frozen=True)
class AskLimits:
    """Host-configurable bounds the service enforces on every turn."""

    max_history_messages: int = 40
    max_context_chars: int = 20_000
    max_tool_iterations: int = 8
    max_output_tokens: int = 1024
    max_output_chars: int = 40_000
    max_evidence_chars: int = 4_000


@dataclass
class Conversation:
    """A conversation owned by a principal. ``messages`` are oldest-first."""

    id: str
    principal_id: str
    messages: List[LLMMessage] = field(default_factory=list)


@dataclass
class ContextSnapshot:
    """Validated grounding state referenced by a request's ``context_ref``.

    Grounding input only: it never grants authorization. The host renders it to a
    string via :meth:`ContextSnapshotProvider.render`.
    """

    ref: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class ResolvedModel:
    """The outcome of :meth:`ModelPolicy.resolve`.

    ``provider`` is a *ready* provider (SDK/credentials validated) or ``None`` when
    no provider is available (mapped to ``unavailable_provider``). ``limits``
    optionally overrides the service default for this turn.
    """

    model: Optional[str]
    provider: Optional[BaseLLMProvider]
    limits: Optional[AskLimits] = None
