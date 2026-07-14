"""Unit tests for the vendor-neutral LLM provider dataclasses and interface."""

from typing import Any, Optional, Sequence

from osprey.worker.lib.llm import (
    BaseLLMProvider,
    CacheControl,
    LLMMessage,
    LLMResponse,
    LLMUsage,
    ToolCall,
    ToolDefinition,
    ToolResult,
)


def test_message_defaults_are_independent() -> None:
    a = LLMMessage(role='user', content='hi')
    b = LLMMessage(role='user', content='bye')

    # Mutable default factories must not be shared across instances.
    assert a.tool_calls is not b.tool_calls
    assert a.tool_results is not b.tool_results
    assert list(a.tool_calls) == []
    assert a.cache_control is None


def test_tool_definition_round_trip() -> None:
    schema = {'type': 'object', 'properties': {'q': {'type': 'string'}}}
    tool = ToolDefinition(name='search', description='Search the docs', input_schema=schema)

    assert tool.name == 'search'
    assert tool.description == 'Search the docs'
    assert tool.input_schema == schema


def test_tool_call_and_result_shapes() -> None:
    call = ToolCall(id='call_1', name='search', arguments={'q': 'osprey'})
    result = ToolResult(tool_call_id='call_1', content='found it')

    assert call.arguments == {'q': 'osprey'}
    assert result.tool_call_id == call.id
    assert result.is_error is False

    err = ToolResult(tool_call_id='call_2', content='boom', is_error=True)
    assert err.is_error is True


def test_assistant_message_with_tool_calls() -> None:
    call = ToolCall(id='c1', name='lookup', arguments={'id': 5})
    message = LLMMessage(role='assistant', content=None, tool_calls=[call])

    assert message.content is None
    assert list(message.tool_calls) == [call]


def test_cache_control_and_usage_defaults() -> None:
    assert CacheControl().ttl is None
    assert CacheControl(ttl='1h').ttl == '1h'

    usage = LLMUsage()
    assert usage.input_tokens == 0
    assert usage.output_tokens == 0
    assert usage.cache_read_tokens == 0
    assert usage.cache_write_tokens == 0


def test_response_defaults() -> None:
    response = LLMResponse(text='hello')
    assert response.text == 'hello'
    assert list(response.tool_calls) == []
    assert response.stop_reason is None
    assert response.usage is None
    assert response.raw is None


def test_base_provider_is_abstract_and_subclassable() -> None:
    # BaseLLMProvider cannot be instantiated directly.
    try:
        BaseLLMProvider()  # type: ignore[abstract]
    except TypeError:
        pass
    else:
        raise AssertionError('expected BaseLLMProvider to be abstract')

    class EchoProvider(BaseLLMProvider):
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
            last = messages[-1]
            return LLMResponse(text=last.content or '')

    provider = EchoProvider()
    response = provider.chat(messages=[LLMMessage(role='user', content='ping')])
    assert response.text == 'ping'
