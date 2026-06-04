"""Vendor-neutral helpers for declaring and dispatching LLM tools.

This is sugar on top of the provider interface in :mod:`osprey.worker.lib.llm.base`:
tools are declared with :class:`ToolParameter` + the :meth:`ToolRegistry.tool`
decorator, which compiles to the same :class:`~osprey.worker.lib.llm.base.ToolDefinition`
the provider already consumes. The registry also binds a handler per tool so a
caller (e.g. the tool-call loop in :mod:`osprey.worker.lib.llm.loop`) can execute a
:class:`~osprey.worker.lib.llm.base.ToolCall` and feed the result back to the model.

Handlers are plain **synchronous** callables (Osprey's worker is gevent/sync). They
receive the tool arguments as keyword arguments and return any JSON-serialisable
value (a non-string return is serialised to JSON for the model). Dependency
injection is intentionally left to the caller — bind state with ``functools.partial``
or a closure rather than a framework-imposed context object.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence

from osprey.worker.lib.llm.base import ToolCall, ToolDefinition, ToolResult

# JSON Schema primitive types a tool parameter may declare.
ParameterType = Literal['string', 'integer', 'number', 'boolean', 'object', 'array']

ToolHandler = Callable[..., Any]


@dataclass
class ToolParameter:
    """A single tool argument, declared in a vendor-neutral way.

    Compiles to one entry of a JSON Schema ``properties`` map (plus ``required``).
    """

    name: str
    type: ParameterType
    description: str
    required: bool = True
    enum: Optional[Sequence[Any]] = None
    # ``None`` is the "no default" sentinel; a JSON Schema ``default`` is only
    # emitted for non-``None`` values.
    default: Any = None


def build_input_schema(parameters: Sequence[ToolParameter]) -> Dict[str, Any]:
    """Compile a list of :class:`ToolParameter` into a JSON Schema object."""
    properties: Dict[str, Any] = {}
    required: List[str] = []
    for parameter in parameters:
        prop: Dict[str, Any] = {'type': parameter.type, 'description': parameter.description}
        if parameter.enum is not None:
            prop['enum'] = list(parameter.enum)
        if parameter.default is not None:
            prop['default'] = parameter.default
        properties[parameter.name] = prop
        if parameter.required:
            required.append(parameter.name)

    schema: Dict[str, Any] = {'type': 'object', 'properties': properties}
    if required:
        schema['required'] = required
    return schema


@dataclass
class Tool:
    """A declared tool: its model-facing schema plus the handler that runs it."""

    name: str
    description: str
    handler: ToolHandler
    parameters: Sequence[ToolParameter] = field(default_factory=list)

    def definition(self) -> ToolDefinition:
        """The provider-facing :class:`ToolDefinition` for this tool."""
        return ToolDefinition(
            name=self.name,
            description=self.description,
            input_schema=build_input_schema(self.parameters),
        )


def _result_to_content(value: Any) -> str:
    """Serialise a handler's return value to the string content the model receives."""
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str)


class ToolRegistry:
    """A collection of named tools, declared via the :meth:`tool` decorator.

    Pass :meth:`definitions` to ``provider.chat(tools=...)`` and run a returned
    :class:`ToolCall` with :meth:`dispatch`.
    """

    def __init__(self) -> None:
        self._tools: Dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        if tool.name in self._tools:
            raise ValueError(f'a tool named {tool.name!r} is already registered')
        self._tools[tool.name] = tool

    def tool(
        self,
        name: str,
        description: str,
        parameters: Optional[Sequence[ToolParameter]] = None,
    ) -> Callable[[ToolHandler], ToolHandler]:
        """Decorator that registers the wrapped callable as a tool.

        The function is returned unchanged, so it remains directly callable::

            registry = ToolRegistry()

            @registry.tool(
                name='lookup_user',
                description='Look up a user by id.',
                parameters=[ToolParameter(name='id', type='integer', description='User id')],
            )
            def lookup_user(id: int) -> dict:
                ...
        """

        def decorator(handler: ToolHandler) -> ToolHandler:
            self.register(
                Tool(
                    name=name,
                    description=description,
                    handler=handler,
                    parameters=parameters or [],
                )
            )
            return handler

        return decorator

    def get(self, name: str) -> Optional[Tool]:
        return self._tools.get(name)

    def all_tools(self) -> List[Tool]:
        return list(self._tools.values())

    def definitions(self) -> List[ToolDefinition]:
        """Tool definitions to hand to ``provider.chat(tools=...)``."""
        return [tool.definition() for tool in self._tools.values()]

    def dispatch(self, tool_call: ToolCall) -> ToolResult:
        """Execute the handler for a model-requested :class:`ToolCall`.

        Errors (unknown tool, bad arguments, handler exceptions, or a result that
        can't be serialised) are returned as a :class:`ToolResult` with
        ``is_error=True`` so the caller can feed them back to the model rather than
        aborting the conversation.

        Note: the error text is sent to the model, so handlers should not embed
        secrets (credentials, connection strings, internal ids) in exceptions.
        """
        tool = self._tools.get(tool_call.name)
        if tool is None:
            return ToolResult(
                tool_call_id=tool_call.id,
                content=f'Unknown tool: {tool_call.name!r}',
                is_error=True,
            )

        try:
            # Serialise inside the try so a non-serialisable result (e.g. a circular
            # reference) becomes an error result rather than propagating to the caller.
            result = tool.handler(**tool_call.arguments)
            content = _result_to_content(result)
        except Exception as exc:  # noqa: BLE001 - surfaced to the model, not swallowed
            return ToolResult(
                tool_call_id=tool_call.id,
                content=f'{type(exc).__name__}: {exc}',
                is_error=True,
            )

        return ToolResult(tool_call_id=tool_call.id, content=content)
