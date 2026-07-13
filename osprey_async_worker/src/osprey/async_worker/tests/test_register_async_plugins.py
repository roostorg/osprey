"""Tests for the example async plugin registrations.

Mirrors register_plugins (sync) for the experimental asyncio worker: verifies
the osprey_async_plugin hooks return the expected UDFs and async output sink,
and that the entry point is discoverable so the async worker can load it.
"""

from importlib.metadata import entry_points
from typing import cast

import register_async_plugins
from async_sinks.example_async_output_sink import ExampleAsyncOutputSink
from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink
from osprey.worker.lib.config import Config
from udfs.text_contains import TextContains


def test_register_udfs_returns_text_contains() -> None:
    assert TextContains in register_async_plugins.register_udfs()


def test_register_async_output_sinks_returns_example_sink() -> None:
    sinks = register_async_plugins.register_async_output_sinks(config=cast(Config, None))
    assert len(sinks) == 1
    assert isinstance(sinks[0], ExampleAsyncOutputSink)
    assert isinstance(sinks[0], AsyncBaseOutputSink)


def test_example_async_plugin_entry_point_is_registered() -> None:
    eps = entry_points(group='osprey_async_plugin')
    assert any(ep.value == 'register_async_plugins' for ep in eps), (
        'example async plugin must be discoverable via the osprey_async_plugin entry-point group'
    )
