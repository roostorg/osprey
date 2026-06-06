# LLM Provider & Tool Calling

Osprey exposes an optional, vendor-neutral interface for LLM API access, used by
AI-assisted features such as natural-language query building. It lives in
`osprey.worker.lib.llm` and is **tool-calling aware**: you pass `ToolDefinition`s
in, the model may return `ToolCall`s, and you feed `ToolResult`s back on the next
`chat()` call.

## The `register_llm_provider` hook

A plugin supplies the LLM API client by implementing the `register_llm_provider`
hook:

```python
from osprey.worker.lib.config import Config
from osprey.worker.lib.llm.base import BaseLLMProvider

@hookimpl_osprey
def register_llm_provider(config: Config) -> BaseLLMProvider:
    return MyLLMProvider(config)
```

Only one provider may be registered (`firstresult=True`). Retrieve it with
`bootstrap_llm_provider(config)` from `osprey.worker.adaptor.plugin_manager`, which
returns `None` when no plugin registers one — so callers should null-check.

A direct Anthropic implementation is provided as a reference in
`example_plugins/src/llm/anthropic_provider.py`, including the request/response and
`tool_use` translation. The `anthropic` SDK is a dependency of `example_plugins`
(installed by `uv sync`); set `ANTHROPIC_API_KEY` (or the
`OSPREY_LLM_ANTHROPIC_API_KEY` config key) to use it. The example plugins do **not**
register it by default — add your own `register_llm_provider` hookimpl to enable it.

## Declaring tools and running a tool loop

`osprey.worker.lib.llm` includes an optional, vendor-neutral tool-calling layer.
Declare tools with the `@tool` decorator on a `ToolRegistry` (it compiles
`ToolParameter`s into the `ToolDefinition` JSON Schema the provider consumes), then
let `run_tool_loop` drive the call/dispatch/feed-back exchange until the model
returns a final answer. Handlers are plain synchronous callables.

```python
from osprey.worker.lib.llm import ToolParameter, ToolRegistry, run_tool_loop, LLMMessage

registry = ToolRegistry()

@registry.tool(
    name='lookup_user',
    description='Look up a user by id.',
    parameters=[ToolParameter(name='id', type='integer', description='User id')],
)
def lookup_user(id: int) -> dict:
    return {'id': id, 'name': '...'}

response = run_tool_loop(
    provider,                       # any BaseLLMProvider
    messages=[LLMMessage(role='user', content='Who is user 7?')],
    registry=registry,
)
```

`registry.dispatch(tool_call)` runs a single tool and captures errors as a
`ToolResult` with `is_error=True` (fed back to the model rather than aborting).
`run_tool_loop` raises `ToolLoopLimitExceeded` if the model keeps requesting tools
past `max_iterations`.
