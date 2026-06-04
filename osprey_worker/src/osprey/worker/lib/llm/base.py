"""Vendor-neutral interface for LLM API providers used by AI-assisted Osprey features.

This module defines a small, provider-agnostic surface for chat-style LLM calls,
designed for tool calling from day one: tool definitions go in, tool-call requests
come back out, and tool results are fed back in on a subsequent call. Concrete
providers (e.g. a direct Anthropic implementation) translate these dataclasses to
and from their vendor SDKs and are registered via the ``register_llm_provider``
plugin hook.

Streaming is intentionally out of scope here; a future ``chat_stream`` can be added
without breaking this interface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Literal, Optional, Sequence

Role = Literal['system', 'user', 'assistant', 'tool']


@dataclass
class ToolDefinition:
    """A tool the model may call, described in a vendor-neutral way."""

    name: str
    description: str
    input_schema: dict[str, Any]
    """JSON Schema describing the tool's input arguments."""


@dataclass
class ToolCall:
    """A request, emitted by the model, to invoke a tool with given arguments."""

    id: str
    name: str
    arguments: dict[str, Any]


@dataclass
class ToolResult:
    """The result of executing a tool, fed back to the model on the next call."""

    tool_call_id: str
    content: str
    is_error: bool = False


@dataclass
class CacheControl:
    """Optional prompt-caching hint.

    Vendor-neutral; providers map this onto their own caching primitives (e.g.
    Anthropic's ephemeral cache breakpoints). A ``None`` field means "use the
    provider default".
    """

    ttl: Optional[str] = None
    """Cache time-to-live, e.g. ``'5m'`` or ``'1h'``. ``None`` = provider default."""


@dataclass
class LLMMessage:
    """A single message in a conversation.

    ``tool_calls`` carries assistant-emitted tool invocation requests, while
    ``tool_results`` carries the outputs of previously requested tools being fed
    back in. A given message typically uses one or the other depending on ``role``.
    """

    role: Role
    content: Optional[str] = None
    tool_calls: Sequence[ToolCall] = field(default_factory=list)
    tool_results: Sequence[ToolResult] = field(default_factory=list)
    cache_control: Optional[CacheControl] = None


@dataclass
class LLMUsage:
    """Token accounting for a single response, where the provider reports it."""

    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0


@dataclass
class LLMResponse:
    """A single non-streaming response from a provider.

    ``text`` is the concatenated assistant text. ``tool_calls`` is non-empty when
    the model is requesting tool execution (typically with ``stop_reason``
    indicating a tool-use stop). ``raw`` holds the untouched vendor response for
    callers that need provider-specific details.
    """

    text: str
    tool_calls: Sequence[ToolCall] = field(default_factory=list)
    stop_reason: Optional[str] = None
    usage: Optional[LLMUsage] = None
    raw: Any = None


class BaseLLMProvider(ABC):
    """Interface implemented by LLM API providers.

    Only a single provider may be registered per Osprey deployment (the
    ``register_llm_provider`` hook uses ``firstresult=True``).
    """

    @abstractmethod
    def chat(
        self,
        *,
        messages: Sequence[LLMMessage],
        system: Optional[str] = None,
        tools: Optional[Sequence[ToolDefinition]] = None,
        model: Optional[str] = None,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **params: Any,
    ) -> LLMResponse:
        """Send a chat completion request and return a single response.

        Args:
            messages: The conversation so far, oldest first.
            system: Optional system prompt. Providers may also accept a leading
                ``system`` message; prefer this argument for clarity.
            tools: Optional tool definitions the model is allowed to call.
            model: Provider model identifier. ``None`` uses the provider default.
            max_tokens: Maximum tokens to generate. ``None`` uses the provider default.
            temperature: Sampling temperature. ``None`` uses the provider default.
            **params: Provider-specific passthrough parameters.

        Returns:
            An :class:`LLMResponse`. When the model requests tool execution,
            ``tool_calls`` is populated and the caller is expected to run the
            tools and call ``chat`` again with the results appended.
        """
        ...
