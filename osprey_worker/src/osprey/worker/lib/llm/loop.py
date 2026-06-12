"""A vendor-neutral tool-call loop.

``run_tool_loop`` drives the standard agentic exchange: call the provider, and while
it requests tool calls, dispatch them through a :class:`~osprey.worker.lib.llm.tools.ToolRegistry`
and feed the results back, until the model returns a final answer (or an iteration
cap is hit). This is pure mechanism — *which* tools exist and any domain prompting
live with the caller, not here.
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence

from osprey.worker.lib.llm.base import BaseLLMProvider, LLMMessage, LLMResponse
from osprey.worker.lib.llm.tools import ToolRegistry

DEFAULT_MAX_ITERATIONS = 10


class ToolLoopLimitExceeded(RuntimeError):
    """Raised when the model keeps requesting tools past ``max_iterations``."""

    def __init__(self, max_iterations: int) -> None:
        super().__init__(f'tool-call loop did not converge within {max_iterations} iteration(s)')
        self.max_iterations = max_iterations


def run_tool_loop(
    provider: BaseLLMProvider,
    *,
    messages: Sequence[LLMMessage],
    registry: ToolRegistry,
    system: Optional[str] = None,
    max_iterations: int = DEFAULT_MAX_ITERATIONS,
    **chat_params: Any,
) -> LLMResponse:
    """Run a chat/tool-execution loop and return the model's final response.

    Args:
        provider: The LLM provider to call.
        messages: The initial conversation, oldest first. Not mutated.
        registry: Tools available to the model; its definitions are sent on every
            turn and its :meth:`~osprey.worker.lib.llm.tools.ToolRegistry.dispatch`
            runs any requested calls.
        system: Optional system prompt forwarded to the provider.
        max_iterations: Maximum number of provider calls before giving up.
        **chat_params: Extra provider params (``model``, ``max_tokens``,
            ``temperature``, ...) forwarded to every ``chat`` call.

    Returns:
        The first :class:`LLMResponse` that requests no further tool calls.

    Raises:
        ToolLoopLimitExceeded: If the model is still requesting tools after
            ``max_iterations`` provider calls.
    """
    if max_iterations < 1:
        raise ValueError('max_iterations must be >= 1')

    conversation: List[LLMMessage] = list(messages)
    tools = registry.definitions()

    for iteration in range(max_iterations):
        response = provider.chat(messages=conversation, system=system, tools=tools, **chat_params)
        if not response.tool_calls:
            return response

        # The model still wants tools but this was our last allowed call. Don't run
        # the tools (they may have side effects) when their results could never be
        # sent back to the model — just give up.
        if iteration == max_iterations - 1:
            break

        # Echo the assistant's tool-use turn, then feed back the results so the next
        # call sees the full exchange (providers require tool results to follow the
        # matching tool-use request).
        conversation.append(
            LLMMessage(
                role='assistant',
                content=response.text or None,
                tool_calls=list(response.tool_calls),
            )
        )
        results = [registry.dispatch(tool_call) for tool_call in response.tool_calls]
        conversation.append(LLMMessage(role='tool', tool_results=results))

    raise ToolLoopLimitExceeded(max_iterations)
