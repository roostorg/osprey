"""Vendor-neutral LLM provider interface for AI-assisted Osprey features.

See :mod:`osprey.worker.lib.llm.base` for the interface and dataclasses. Concrete
providers are registered through the ``register_llm_provider`` plugin hook and
obtained via ``osprey.worker.adaptor.plugin_manager.bootstrap_llm_provider``.

:mod:`osprey.worker.lib.llm.tools` and :mod:`osprey.worker.lib.llm.loop` add an
optional, vendor-neutral tool-calling layer: declare tools with the ``@tool``
decorator and run the standard tool-execution exchange with ``run_tool_loop``.
"""

from osprey.worker.lib.llm.base import (
    BaseLLMProvider,
    CacheControl,
    LLMMessage,
    LLMResponse,
    LLMUsage,
    Role,
    ToolCall,
    ToolDefinition,
    ToolResult,
)
from osprey.worker.lib.llm.loop import ToolLoopLimitExceeded, run_tool_loop
from osprey.worker.lib.llm.tools import Tool, ToolParameter, ToolRegistry, build_input_schema

__all__ = [
    'BaseLLMProvider',
    'CacheControl',
    'LLMMessage',
    'LLMResponse',
    'LLMUsage',
    'Role',
    'ToolCall',
    'ToolDefinition',
    'ToolResult',
    'Tool',
    'ToolParameter',
    'ToolRegistry',
    'build_input_schema',
    'run_tool_loop',
    'ToolLoopLimitExceeded',
]
