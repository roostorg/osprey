"""Tests for the example Anthropic LLM provider.

These use a fake Anthropic client (no network, no SDK, no API key) to verify the
vendor-neutral <-> Anthropic translation, including a full tool-call cycle.
"""

from typing import Any, Dict, List

from osprey.worker.lib.config import Config
from osprey.worker.lib.llm.base import (
    CacheControl,
    LLMMessage,
    ToolCall,
    ToolDefinition,
    ToolResult,
)

from llm.anthropic_provider import DEFAULT_MAX_TOKENS, DEFAULT_MODEL, AnthropicLLMProvider


class _Block:
    """Mimics an Anthropic content block (text or tool_use)."""

    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)


class _Usage:
    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)


class _Response:
    def __init__(self, content: List[_Block], stop_reason: str, usage: _Usage) -> None:
        self.content = content
        self.stop_reason = stop_reason
        self.usage = usage


class _FakeMessages:
    def __init__(self, responses: List[_Response]) -> None:
        self._responses = responses
        self.calls: List[Dict[str, Any]] = []

    def create(self, **kwargs: Any) -> _Response:
        self.calls.append(kwargs)
        return self._responses[len(self.calls) - 1]


class _FakeClient:
    def __init__(self, responses: List[_Response]) -> None:
        self.messages = _FakeMessages(responses)


def _text_response(text: str) -> _Response:
    return _Response(
        content=[_Block(type='text', text=text)],
        stop_reason='end_turn',
        usage=_Usage(input_tokens=10, output_tokens=5, cache_read_input_tokens=0, cache_creation_input_tokens=0),
    )


def test_defaults_used_when_not_overridden() -> None:
    client = _FakeClient([_text_response('hello')])
    provider = AnthropicLLMProvider(Config({}), client=client)

    provider.chat(messages=[LLMMessage(role='user', content='hi')])

    request = client.messages.calls[0]
    assert request['model'] == DEFAULT_MODEL
    assert request['max_tokens'] == DEFAULT_MAX_TOKENS
    assert request['messages'] == [{'role': 'user', 'content': [{'type': 'text', 'text': 'hi'}]}]
    assert 'tools' not in request
    assert 'temperature' not in request


def test_config_overrides_model_and_max_tokens() -> None:
    client = _FakeClient([_text_response('hello')])
    config = Config({'LLM_ANTHROPIC_MODEL': 'claude-test', 'LLM_ANTHROPIC_MAX_TOKENS': 256})
    provider = AnthropicLLMProvider(config, client=client)

    provider.chat(messages=[LLMMessage(role='user', content='hi')])

    request = client.messages.calls[0]
    assert request['model'] == 'claude-test'
    assert request['max_tokens'] == 256


def test_system_prompt_and_role_system_messages_folded() -> None:
    client = _FakeClient([_text_response('ok')])
    provider = AnthropicLLMProvider(Config({}), client=client)

    provider.chat(
        messages=[
            LLMMessage(role='system', content='from message'),
            LLMMessage(role='user', content='hi'),
        ],
        system='from arg',
    )

    request = client.messages.calls[0]
    assert request['system'] == 'from arg\n\nfrom message'
    # system messages are not surfaced as conversation messages
    assert request['messages'] == [{'role': 'user', 'content': [{'type': 'text', 'text': 'hi'}]}]


def test_per_call_overrides_and_passthrough_params() -> None:
    client = _FakeClient([_text_response('ok')])
    provider = AnthropicLLMProvider(Config({}), client=client)

    provider.chat(
        messages=[LLMMessage(role='user', content='hi')],
        model='claude-override',
        max_tokens=42,
        temperature=0.3,
        top_p=0.9,
    )

    request = client.messages.calls[0]
    assert request['model'] == 'claude-override'
    assert request['max_tokens'] == 42
    assert request['temperature'] == 0.3
    assert request['top_p'] == 0.9


def test_tools_and_cache_control_translation() -> None:
    client = _FakeClient([_text_response('ok')])
    provider = AnthropicLLMProvider(Config({}), client=client)

    tool = ToolDefinition(
        name='lookup_user',
        description='Look up a user by id',
        input_schema={'type': 'object', 'properties': {'id': {'type': 'integer'}}},
    )
    provider.chat(
        messages=[LLMMessage(role='user', content='hi', cache_control=CacheControl(ttl='1h'))],
        tools=[tool],
    )

    request = client.messages.calls[0]
    assert request['tools'] == [
        {
            'name': 'lookup_user',
            'description': 'Look up a user by id',
            'input_schema': {'type': 'object', 'properties': {'id': {'type': 'integer'}}},
        }
    ]
    # cache_control is attached to the last content block of the message.
    block = request['messages'][0]['content'][-1]
    assert block['cache_control'] == {'type': 'ephemeral', 'ttl': '1h'}


def test_response_with_tool_use_is_parsed() -> None:
    response = _Response(
        content=[
            _Block(type='text', text='let me check'),
            _Block(type='tool_use', id='call_1', name='lookup_user', input={'id': 7}),
        ],
        stop_reason='tool_use',
        usage=_Usage(
            input_tokens=20,
            output_tokens=8,
            cache_read_input_tokens=3,
            cache_creation_input_tokens=4,
        ),
    )
    client = _FakeClient([response])
    provider = AnthropicLLMProvider(Config({}), client=client)

    result = provider.chat(messages=[LLMMessage(role='user', content='who is user 7?')])

    assert result.text == 'let me check'
    assert result.stop_reason == 'tool_use'
    assert result.tool_calls == [ToolCall(id='call_1', name='lookup_user', arguments={'id': 7})]
    assert result.usage is not None
    assert result.usage.input_tokens == 20
    assert result.usage.output_tokens == 8
    assert result.usage.cache_read_tokens == 3
    assert result.usage.cache_write_tokens == 4
    assert result.raw is response


def test_full_tool_call_cycle() -> None:
    first = _Response(
        content=[_Block(type='tool_use', id='call_1', name='lookup_user', input={'id': 7})],
        stop_reason='tool_use',
        usage=_Usage(input_tokens=1, output_tokens=1, cache_read_input_tokens=0, cache_creation_input_tokens=0),
    )
    second = _text_response('User 7 is Ada.')
    client = _FakeClient([first, second])
    provider = AnthropicLLMProvider(Config({}), client=client)

    # Round 1: model requests a tool call.
    first_result = provider.chat(messages=[LLMMessage(role='user', content='who is user 7?')])
    assert first_result.tool_calls[0].id == 'call_1'

    # Round 2: feed the tool result back and get a final answer.
    second_result = provider.chat(
        messages=[
            LLMMessage(role='user', content='who is user 7?'),
            LLMMessage(role='assistant', tool_calls=list(first_result.tool_calls)),
            LLMMessage(
                role='tool',
                tool_results=[ToolResult(tool_call_id='call_1', content='Ada')],
            ),
        ]
    )
    assert second_result.text == 'User 7 is Ada.'

    # Verify the tool_use and tool_result blocks were serialized as Anthropic expects.
    second_request = client.messages.calls[1]
    roles = [m['role'] for m in second_request['messages']]
    assert roles == ['user', 'assistant', 'user']

    assistant_block = second_request['messages'][1]['content'][0]
    assert assistant_block == {
        'type': 'tool_use',
        'id': 'call_1',
        'name': 'lookup_user',
        'input': {'id': 7},
    }
    tool_result_block = second_request['messages'][2]['content'][0]
    assert tool_result_block == {
        'type': 'tool_result',
        'tool_use_id': 'call_1',
        'content': 'Ada',
        'is_error': False,
    }


def test_missing_sdk_raises_clear_error() -> None:
    # No client injected and the `anthropic` package is not installed in the workspace,
    # so building the client should fail with a helpful message.
    provider = AnthropicLLMProvider(Config({}))
    try:
        provider.chat(messages=[LLMMessage(role='user', content='hi')])
    except RuntimeError as exc:
        assert 'anthropic' in str(exc)
    else:
        raise AssertionError('expected a RuntimeError when the anthropic SDK is missing')
