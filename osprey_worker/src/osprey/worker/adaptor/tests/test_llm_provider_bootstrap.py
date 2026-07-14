"""Discovery and absence behavior for the ``register_llm_provider`` plugin hook.

Covers osprey-ask-ai.AC1.4 (a provider is discoverable through the existing pluggy
hookspec + bootstrap path) and AC1.6 (provider absence returns ``None`` without
requiring a vendor SDK).

These tests do not touch a database and avoid loading real setuptools entrypoints:
discovery is exercised by registering an in-memory fake plugin on the manager, and
absence is exercised with a fresh, empty manager that only has the hookspecs added.
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

import pluggy
from osprey.worker.adaptor import plugin_manager as pm
from osprey.worker.adaptor.constants import OSPREY_ADAPTOR
from osprey.worker.adaptor.hookspecs import osprey_hooks
from osprey.worker.lib.llm.base import (
    BaseLLMProvider,
    LLMMessage,
    LLMResponse,
    ToolDefinition,
)


class _FakeProvider(BaseLLMProvider):
    """A minimal provider used only to prove discovery; ``chat`` must never be called."""

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
        raise AssertionError('discovery must not invoke chat()')


class _FakeLLMPlugin:
    def __init__(self, provider: BaseLLMProvider) -> None:
        self._provider = provider

    @pm.hookimpl_osprey
    def register_llm_provider(self, config: Any) -> BaseLLMProvider:
        return self._provider


def test_provider_discovered_through_hook() -> None:
    provider = _FakeProvider()
    plugin = _FakeLLMPlugin(provider)
    pm.plugin_manager.register(plugin)
    try:
        # firstresult=True => a single provider is returned (not a list).
        result = pm.plugin_manager.hook.register_llm_provider(config=None)
        assert result is provider
    finally:
        pm.plugin_manager.unregister(plugin)


def test_absence_returns_none_with_fresh_manager() -> None:
    fresh = pluggy.PluginManager(OSPREY_ADAPTOR)
    fresh.add_hookspecs(osprey_hooks)
    # No implementations registered: a firstresult hook yields None (provider absence).
    assert fresh.hook.register_llm_provider(config=None) is None


def test_bootstrap_returns_registered_provider(monkeypatch: Any) -> None:
    # Keep hermetic: do not load real setuptools entrypoints for this assertion.
    monkeypatch.setattr(pm, 'load_all_osprey_plugins', lambda: None)
    provider = _FakeProvider()
    plugin = _FakeLLMPlugin(provider)
    pm.plugin_manager.register(plugin)
    try:
        assert pm.bootstrap_llm_provider(config=None) is provider
    finally:
        pm.plugin_manager.unregister(plugin)


def test_bootstrap_returns_none_when_unregistered(monkeypatch: Any) -> None:
    # A fresh, empty manager stands in for "no llm provider plugin installed"; bootstrap
    # must return None (not raise) and must not require any vendor SDK.
    empty = pluggy.PluginManager(OSPREY_ADAPTOR)
    empty.add_hookspecs(osprey_hooks)
    monkeypatch.setattr(pm, 'load_all_osprey_plugins', lambda: None)
    monkeypatch.setattr(pm, 'plugin_manager', empty)
    assert pm.bootstrap_llm_provider(config=None) is None
