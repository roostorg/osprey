"""Unit tests for the vendor-neutral tool-call loop."""

from typing import Any, List, Optional, Sequence

from osprey.worker.lib.llm import (
    BaseLLMProvider,
    LLMMessage,
    LLMResponse,
    ToolCall,
    ToolDefinition,
    ToolLoopLimitExceeded,
    ToolParameter,
    ToolRegistry,
    run_tool_loop,
)


class _ScriptedProvider(BaseLLMProvider):
    """A provider that returns a fixed list of responses, recording each call."""

    def __init__(self, responses: Sequence[LLMResponse]) -> None:
        self._responses = list(responses)
        self.calls: List[List[LLMMessage]] = []

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
        # Snapshot the conversation as seen on this call.
        self.calls.append(list(messages))
        return self._responses[len(self.calls) - 1]


def _registry() -> ToolRegistry:
    registry = ToolRegistry()

    @registry.tool(
        name='lookup_user',
        description='Look up a user by id.',
        parameters=[ToolParameter(name='id', type='integer', description='user id')],
    )
    def lookup_user(id: int) -> dict:
        return {'id': id, 'name': 'Ada'}

    return registry


def test_loop_returns_immediately_without_tool_calls() -> None:
    provider = _ScriptedProvider([LLMResponse(text='just an answer')])

    result = run_tool_loop(
        provider,
        messages=[LLMMessage(role='user', content='hi')],
        registry=_registry(),
    )

    assert result.text == 'just an answer'
    assert len(provider.calls) == 1


def test_loop_runs_tool_then_returns_final() -> None:
    provider = _ScriptedProvider(
        [
            LLMResponse(
                text='let me check',
                tool_calls=[ToolCall(id='call_1', name='lookup_user', arguments={'id': 7})],
                stop_reason='tool_use',
            ),
            LLMResponse(text='User 7 is Ada.'),
        ]
    )

    result = run_tool_loop(
        provider,
        messages=[LLMMessage(role='user', content='who is user 7?')],
        registry=_registry(),
    )

    assert result.text == 'User 7 is Ada.'
    assert len(provider.calls) == 2

    # The second call must see: original user, the assistant tool-use turn, then the
    # tool results — in that order.
    second_call = provider.calls[1]
    assert [m.role for m in second_call] == ['user', 'assistant', 'tool']

    assistant_msg = second_call[1]
    assert assistant_msg.content == 'let me check'
    assert list(assistant_msg.tool_calls) == [ToolCall(id='call_1', name='lookup_user', arguments={'id': 7})]

    tool_msg = second_call[2]
    assert len(tool_msg.tool_results) == 1
    tool_result = tool_msg.tool_results[0]
    assert tool_result.tool_call_id == 'call_1'
    assert tool_result.is_error is False
    assert tool_result.content == '{"id": 7, "name": "Ada"}'


def test_loop_does_not_mutate_input_messages() -> None:
    provider = _ScriptedProvider(
        [
            LLMResponse(tool_calls=[ToolCall(id='c1', name='lookup_user', arguments={'id': 1})], text=''),
            LLMResponse(text='done'),
        ]
    )
    messages = [LLMMessage(role='user', content='go')]

    run_tool_loop(provider, messages=messages, registry=_registry())

    assert len(messages) == 1


def test_loop_raises_when_limit_exceeded() -> None:
    # Always asks for a tool, never converges.
    looping = LLMResponse(tool_calls=[ToolCall(id='c', name='lookup_user', arguments={'id': 1})], text='')
    provider = _ScriptedProvider([looping, looping, looping])

    try:
        run_tool_loop(
            provider,
            messages=[LLMMessage(role='user', content='go')],
            registry=_registry(),
            max_iterations=3,
        )
    except ToolLoopLimitExceeded as exc:
        assert exc.max_iterations == 3
    else:
        raise AssertionError('expected ToolLoopLimitExceeded')

    assert len(provider.calls) == 3


def test_loop_does_not_dispatch_tools_on_the_final_iteration() -> None:
    # A registry whose tool records each invocation, so we can prove side effects
    # don't run on the limit-tripping call.
    dispatched: List[int] = []
    registry = ToolRegistry()

    @registry.tool(
        name='lookup_user',
        description='Look up a user by id.',
        parameters=[ToolParameter(name='id', type='integer', description='user id')],
    )
    def lookup_user(id: int) -> dict:
        dispatched.append(id)
        return {'id': id}

    looping = LLMResponse(tool_calls=[ToolCall(id='c', name='lookup_user', arguments={'id': 1})], text='')
    provider = _ScriptedProvider([looping, looping])

    try:
        run_tool_loop(
            provider,
            messages=[LLMMessage(role='user', content='go')],
            registry=registry,
            max_iterations=2,
        )
    except ToolLoopLimitExceeded:
        pass
    else:
        raise AssertionError('expected ToolLoopLimitExceeded')

    # Two chat calls were made, but the tool ran only on the first (non-final) one.
    assert len(provider.calls) == 2
    assert dispatched == [1]


def test_loop_rejects_invalid_max_iterations() -> None:
    provider = _ScriptedProvider([LLMResponse(text='x')])
    try:
        run_tool_loop(
            provider,
            messages=[LLMMessage(role='user', content='go')],
            registry=_registry(),
            max_iterations=0,
        )
    except ValueError:
        pass
    else:
        raise AssertionError('expected ValueError for max_iterations=0')
