"""Lifecycle tests for the async worker CLI."""

import asyncio
import signal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from osprey.async_worker.cli import main as cli_main
from osprey.async_worker.engine import AsyncOspreyEngine


class _SignalLoop:
    def __init__(self, trigger_signal: bool) -> None:
        self._trigger_signal = trigger_signal

    def add_signal_handler(self, sig, handler) -> None:
        if self._trigger_signal and sig == signal.SIGTERM:
            handler()


@pytest.mark.parametrize('trigger_signal', [False, True], ids=['normal-completion', 'signal'])
def test_run_stops_sink_then_awaits_async_engine_shutdown(monkeypatch, tmp_path, trigger_signal):
    order = []
    engine = object.__new__(AsyncOspreyEngine)

    async def shutdown_async() -> None:
        order.append('engine.shutdown_async')

    engine.shutdown_async = shutdown_async
    engine.shutdown = lambda: order.append('engine.shutdown.sync')

    class _RulesSink:
        async def run(self) -> None:
            if trigger_signal:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    order.append('sink.cancelled')
                    raise
            order.append('sink.completed')

        async def stop(self) -> None:
            order.append('sink.stop')

    rules_dir = tmp_path / 'rules'
    rules_dir.mkdir()
    monkeypatch.setattr(cli_main, 'init_config', lambda: None)
    monkeypatch.setattr(cli_main, 'bootstrap_stdlib_engine', lambda path: (engine, SimpleNamespace()))
    monkeypatch.setattr(cli_main, 'AsyncRulesSink', lambda **kwargs: _RulesSink())
    monkeypatch.setattr(cli_main.asyncio, 'get_running_loop', lambda: _SignalLoop(trigger_signal))

    with patch.object(cli_main.logger, 'info'):
        cli_main.run.callback(str(rules_dir), None, 12, False)

    expected = ['sink.cancelled'] if trigger_signal else ['sink.completed']
    expected.extend(['sink.stop', 'engine.shutdown_async'])
    assert order == expected


def test_run_awaits_async_engine_shutdown_when_sink_stop_fails(monkeypatch, tmp_path):
    order = []
    engine = object.__new__(AsyncOspreyEngine)

    async def shutdown_async() -> None:
        order.append('engine.shutdown_async')

    engine.shutdown_async = shutdown_async
    engine.shutdown = lambda: order.append('engine.shutdown.sync')

    class _RulesSink:
        async def run(self) -> None:
            order.append('sink.completed')

        async def stop(self) -> None:
            order.append('sink.stop')
            raise RuntimeError('stop failed')

    rules_dir = tmp_path / 'rules'
    rules_dir.mkdir()
    monkeypatch.setattr(cli_main, 'init_config', lambda: None)
    monkeypatch.setattr(cli_main, 'bootstrap_stdlib_engine', lambda path: (engine, SimpleNamespace()))
    monkeypatch.setattr(cli_main, 'AsyncRulesSink', lambda **kwargs: _RulesSink())
    monkeypatch.setattr(cli_main.asyncio, 'get_running_loop', lambda: _SignalLoop(False))

    with patch.object(cli_main.logger, 'info'), pytest.raises(RuntimeError, match='stop failed'):
        cli_main.run.callback(str(rules_dir), None, 12, False)

    assert order == ['sink.completed', 'sink.stop', 'engine.shutdown_async']


def test_run_does_not_send_sync_plugin_engine_through_async_shutdown(monkeypatch, tmp_path):
    order = []
    sync_engine = SimpleNamespace()

    async def unexpected_shutdown() -> None:
        order.append('sync_engine.shutdown_async')

    sync_engine.shutdown_async = unexpected_shutdown

    class _RulesSink:
        async def run(self) -> None:
            order.append('sink.completed')

        async def stop(self) -> None:
            order.append('sink.stop')

    rules_dir = tmp_path / 'rules'
    rules_dir.mkdir()
    monkeypatch.setattr(cli_main, 'init_config', lambda: None)
    monkeypatch.setattr(cli_main.Sources, 'from_path', lambda path: MagicMock())
    monkeypatch.setattr(cli_main, 'AsyncRulesSink', lambda **kwargs: _RulesSink())
    monkeypatch.setattr(cli_main.asyncio, 'get_running_loop', lambda: _SignalLoop(False))

    with (
        patch(
            'osprey.worker.lib.osprey_engine.bootstrap_engine_with_helpers',
            return_value=(sync_engine, SimpleNamespace()),
        ),
        patch.object(cli_main.logger, 'info'),
    ):
        cli_main.run.callback(str(rules_dir), None, 12, True)

    assert order == ['sink.completed', 'sink.stop']
