"""Vendor-neutral LLM provider interface for AI-assisted Osprey features.

See :mod:`osprey.worker.lib.llm.base` for the interface and dataclasses. Concrete
providers are registered through the ``register_llm_provider`` plugin hook and
obtained via ``osprey.worker.adaptor.plugin_manager.bootstrap_llm_provider``.
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
]
