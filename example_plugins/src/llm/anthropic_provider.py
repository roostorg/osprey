"""Example LLM provider backed directly by the Anthropic Messages API.

This demonstrates implementing :class:`osprey.worker.lib.llm.base.BaseLLMProvider`,
including tool calling: it translates the vendor-neutral ``LLMMessage`` /
``ToolDefinition`` types into Anthropic's request format, and maps the response
(including ``tool_use`` blocks) back into ``LLMResponse`` / ``ToolCall``.

The ``anthropic`` SDK is an optional dependency (``example_plugins[llm]``). It is
imported lazily so the base example package, and Osprey's CI, do not require the
SDK, an API key, or network access unless this provider is actually used.

Configuration (via Osprey ``Config`` or environment):

- API key: ``LLM_ANTHROPIC_API_KEY`` config key, else the ``ANTHROPIC_API_KEY``
  environment variable (read by the SDK itself if neither is set explicitly).
- Default model: ``LLM_ANTHROPIC_MODEL`` config key
  (default: ``claude-3-5-sonnet-latest``).
- Default max tokens: ``LLM_ANTHROPIC_MAX_TOKENS`` config key (default: ``1024``).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple, Union

from osprey.worker.lib.config import Config
from osprey.worker.lib.llm.base import (
    BaseLLMProvider,
    CacheControl,
    LLMMessage,
    LLMResponse,
    LLMUsage,
    ToolCall,
    ToolDefinition,
)

if TYPE_CHECKING:
    from anthropic import Anthropic

DEFAULT_MODEL = 'claude-3-5-sonnet-latest'
DEFAULT_MAX_TOKENS = 1024


class AnthropicLLMProvider(BaseLLMProvider):
    """A :class:`BaseLLMProvider` that calls the Anthropic Messages API directly."""

    def __init__(self, config: Config, client: Optional['Anthropic'] = None) -> None:
        self._config = config
        self._default_model = config.get_str('LLM_ANTHROPIC_MODEL', DEFAULT_MODEL)
        self._default_max_tokens = config.get_int('LLM_ANTHROPIC_MAX_TOKENS', DEFAULT_MAX_TOKENS)
        # Allow injecting a client (used in tests); otherwise build lazily on first use.
        self._client = client

    def _get_client(self) -> 'Anthropic':
        if self._client is not None:
            return self._client

        try:
            import anthropic
        except ImportError as exc:  # pragma: no cover - exercised only without the optional dep
            raise RuntimeError(
                "The 'anthropic' package is required to use AnthropicLLMProvider. "
                'It is not a declared workspace dependency (it conflicts with the pinned '
                'typing-extensions), so install it manually, e.g. `uv pip install anthropic`.'
            ) from exc

        api_key = self._config.get_optional_str('LLM_ANTHROPIC_API_KEY') or os.environ.get('ANTHROPIC_API_KEY')
        # If api_key is None the SDK still reads ANTHROPIC_API_KEY from the environment itself.
        self._client = anthropic.Anthropic(api_key=api_key) if api_key else anthropic.Anthropic()
        return self._client

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
        request: Dict[str, Any] = {
            # Use `is not None` rather than `or` so an explicit max_tokens=0 or
            # model='' is passed through (and rejected by the API) instead of
            # silently falling back to the default.
            'model': model if model is not None else self._default_model,
            'max_tokens': max_tokens if max_tokens is not None else self._default_max_tokens,
            'messages': self._to_anthropic_messages(messages),
        }

        system_value = self._build_system(system, messages)
        if system_value is not None:
            request['system'] = system_value

        if tools:
            request['tools'] = [self._to_anthropic_tool(tool) for tool in tools]

        if temperature is not None:
            request['temperature'] = temperature

        # Provider-specific passthrough (e.g. top_p, stop_sequences, tool_choice).
        request.update(params)

        response = self._get_client().messages.create(**request)
        return self._from_anthropic_response(response)

    # --- request translation ------------------------------------------------

    @classmethod
    def _build_system(
        cls, system: Optional[str], messages: Sequence[LLMMessage]
    ) -> Union[str, List[Dict[str, Any]], None]:
        # Anthropic carries the system prompt as a top-level field, not a message,
        # so the `system` argument and any role='system' messages are folded here.
        # Each part keeps its own optional cache_control breakpoint.
        parts: List[Tuple[str, Optional[CacheControl]]] = []
        if system:
            parts.append((system, None))
        for message in messages:
            if message.role == 'system' and message.content:
                parts.append((message.content, message.cache_control))

        if not parts:
            return None

        # When nothing needs a cache breakpoint, the simple string form suffices.
        if all(cache_control is None for _, cache_control in parts):
            return '\n\n'.join(text for text, _ in parts)

        # Otherwise emit the structured block form so per-part cache_control is
        # preserved (a string `system` cannot carry cache breakpoints).
        blocks: List[Dict[str, Any]] = []
        for text, cache_control in parts:
            block: Dict[str, Any] = {'type': 'text', 'text': text}
            if cache_control is not None:
                block['cache_control'] = cls._cache_control_dict(cache_control)
            blocks.append(block)
        return blocks

    @staticmethod
    def _cache_control_dict(cache_control: CacheControl) -> Dict[str, Any]:
        result: Dict[str, Any] = {'type': 'ephemeral'}
        if cache_control.ttl is not None:
            # A non-default ttl (e.g. '1h') requires the Anthropic extended-cache-ttl
            # beta header on the client, or the API rejects the request; the default
            # 5m ephemeral cache needs no header.
            result['ttl'] = cache_control.ttl
        return result

    @staticmethod
    def _to_anthropic_tool(tool: ToolDefinition) -> Dict[str, Any]:
        return {
            'name': tool.name,
            'description': tool.description,
            'input_schema': tool.input_schema,
        }

    @classmethod
    def _to_anthropic_messages(cls, messages: Sequence[LLMMessage]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for message in messages:
            # System messages are handled separately via the top-level `system` field.
            if message.role == 'system':
                continue

            blocks = cls._message_content_blocks(message)
            if not blocks:
                continue

            if message.cache_control is not None:
                blocks[-1]['cache_control'] = cls._cache_control_dict(message.cache_control)

            # Tool results are surfaced to Anthropic as a user-role message.
            role = 'user' if message.role == 'tool' else message.role
            out.append({'role': role, 'content': blocks})
        return out

    @staticmethod
    def _message_content_blocks(message: LLMMessage) -> List[Dict[str, Any]]:
        blocks: List[Dict[str, Any]] = []

        if message.content:
            blocks.append({'type': 'text', 'text': message.content})

        for tool_call in message.tool_calls:
            blocks.append(
                {
                    'type': 'tool_use',
                    'id': tool_call.id,
                    'name': tool_call.name,
                    'input': tool_call.arguments,
                }
            )

        for tool_result in message.tool_results:
            blocks.append(
                {
                    'type': 'tool_result',
                    'tool_use_id': tool_result.tool_call_id,
                    'content': tool_result.content,
                    'is_error': tool_result.is_error,
                }
            )

        return blocks

    # --- response translation ------------------------------------------------

    @staticmethod
    def _from_anthropic_response(response: Any) -> LLMResponse:
        text_parts: List[str] = []
        tool_calls: List[ToolCall] = []

        for block in getattr(response, 'content', None) or []:
            block_type = getattr(block, 'type', None)
            if block_type == 'text':
                text_parts.append(getattr(block, 'text', '') or '')
            elif block_type == 'tool_use':
                raw_input = getattr(block, 'input', None)
                tool_calls.append(
                    ToolCall(
                        id=getattr(block, 'id', ''),
                        name=getattr(block, 'name', ''),
                        # Degrade gracefully (like the other fields) if the SDK ever
                        # hands back a non-dict input rather than crashing.
                        arguments=dict(raw_input) if isinstance(raw_input, dict) else {},
                    )
                )

        usage: Optional[LLMUsage] = None
        raw_usage = getattr(response, 'usage', None)
        if raw_usage is not None:
            usage = LLMUsage(
                input_tokens=getattr(raw_usage, 'input_tokens', 0) or 0,
                output_tokens=getattr(raw_usage, 'output_tokens', 0) or 0,
                cache_read_tokens=getattr(raw_usage, 'cache_read_input_tokens', 0) or 0,
                cache_write_tokens=getattr(raw_usage, 'cache_creation_input_tokens', 0) or 0,
            )

        return LLMResponse(
            text=''.join(text_parts),
            tool_calls=tool_calls,
            stop_reason=getattr(response, 'stop_reason', None),
            usage=usage,
            raw=response,
        )
