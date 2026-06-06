"""Tests for ``EtcdSourcesProvider``'s self-healing dedup of etcd source updates.

Context: etcd re-delivers the current value as a full snapshot on every watcher
reconnect / session refresh, so the provider dedups to avoid the memory-doubling
recompile on no-op events. The dedup must key off what the engine has *applied*,
not merely what the provider last *received* — otherwise a recompile that fails
(the engine swallows the error and keeps the old graph) or is dropped (the async
fire-and-forget bridge in the gevent config consumers) advances the dedup
baseline while the engine stays on the old graph. Every subsequent identical
re-delivery is then suppressed and the engine is wedged on stale rules until the
pod restarts.
"""

from typing import Dict
from unittest.mock import patch

from osprey.engine.ast.sources import Sources
from osprey.engine.udf.registry import UDFRegistry
from osprey.worker.lib import sources_provider as sp_module
from osprey.worker.lib.osprey_engine import OspreyEngine
from osprey.worker.lib.sources_provider import EtcdSourcesProvider

# Trivial but distinct, validly-compiling rule sources (no UDFs needed).
S0: Dict[str, str] = {'main.sml': ''}
S1: Dict[str, str] = {'main.sml': '# updated\n'}


def _make_fake_etcd_dict(initial: Dict[str, str]) -> type:
    """Build a ``ReadOnlyEtcdDict`` stand-in seeded with ``initial`` so the
    provider can be constructed without a real etcd."""

    class _FakeEtcdDict:
        def __init__(self, etcd_key: str, etcd_client: object = None, **kwargs: object) -> None:
            self._initial = dict(initial)

        def copy(self) -> Dict[str, str]:
            return dict(self._initial)

        def add_watcher(self, callback: object) -> None:
            pass

        def watch(self) -> None:
            pass

    return _FakeEtcdDict


def _make_provider(initial: Dict[str, str] = S0) -> EtcdSourcesProvider:
    with patch.object(sp_module, 'ReadOnlyEtcdDict', _make_fake_etcd_dict(initial)):
        # The engine compiles this initial snapshot at construction.
        return EtcdSourcesProvider(etcd_key='/test/key')


def test_redelivery_after_failed_apply_self_heals():
    """A recompile that fails once must be retried on the next identical
    re-delivery — the dedup baseline must not advance past what was applied."""
    provider = _make_provider()

    fired = {'n': 0}
    compile_succeeds = {'v': False}

    def fake_engine_callback() -> None:
        # Mirrors the engine contract: "compile" the current sources and only
        # confirm-applied on success. On failure (swallowed) it does not mark,
        # exactly like the engine keeping its old graph after a compile error.
        fired['n'] += 1
        if compile_succeeds['v']:
            provider.mark_sources_applied(provider.get_current_sources().hash())

    provider.set_sources_watcher(fake_engine_callback)

    # 1) S1 arrives; the apply FAILS (engine keeps old graph, does not mark).
    provider._notify_watcher(dict(S1))
    assert fired['n'] == 1

    # 2) etcd re-delivers S1 (FullSyncOne on reconnect). It MUST re-fire so the
    #    transient failure self-heals — with the bug this is suppressed and the
    #    engine stays wedged on S0.
    compile_succeeds['v'] = True
    provider._notify_watcher(dict(S1))
    assert fired['n'] == 2, 're-delivery after a failed apply was suppressed — engine wedged on stale rules'

    # 3) Once the engine has applied S1, further S1 re-deliveries are deduped.
    provider._notify_watcher(dict(S1))
    assert fired['n'] == 2, 'no-op re-delivery should be skipped once applied'


def test_noop_redelivery_of_applied_sources_is_skipped():
    """OOM mitigation preserved: re-delivery of already-applied sources (incl. the
    initial snapshot the engine compiled at construction) does not recompile."""
    provider = _make_provider()

    fired = {'n': 0}

    def fake_engine_callback() -> None:
        fired['n'] += 1
        provider.mark_sources_applied(provider.get_current_sources().hash())

    provider.set_sources_watcher(fake_engine_callback)

    # Re-delivery of the initial applied snapshot (S0) must be skipped.
    provider._notify_watcher(dict(S0))
    assert fired['n'] == 0, 're-delivery of the initial applied snapshot should be skipped'

    # A genuine change fires once; its subsequent re-deliveries are skipped.
    provider._notify_watcher(dict(S1))
    assert fired['n'] == 1
    provider._notify_watcher(dict(S1))
    assert fired['n'] == 1


def test_real_engine_recovers_from_transient_apply_failure_on_redelivery():
    """End-to-end with a REAL OspreyEngine wired to EtcdSourcesProvider.

    A valid rules update whose first apply fails transiently (engine swallows the
    error and keeps the old graph — the same shape as a dropped async-bridge
    dispatch) must be picked up when etcd re-delivers it. Proves the engine's
    live execution graph actually advances, not just the provider's bookkeeping.
    """
    s0_hash = Sources.from_dict(dict(S0)).hash()
    s1_hash = Sources.from_dict(dict(S1)).hash()
    assert s0_hash != s1_hash

    provider = _make_provider(S0)
    # Construct the engine directly with an empty UDF registry (trivial rules use
    # no UDFs) to avoid loading external plugin entrypoints.
    engine = OspreyEngine(sources_provider=provider, udf_registry=UDFRegistry())

    # The engine compiled the initial snapshot at construction.
    assert engine.execution_graph.validated_sources.sources.hash() == s0_hash

    # Make the next recompile fail exactly once — a transient apply failure on a
    # valid update (resource blip / dropped dispatch).
    real_compile = engine._compile_execution_graph
    calls = {'n': 0}

    def flaky_compile(*args: object, **kwargs: object) -> object:
        calls['n'] += 1
        if calls['n'] == 1:
            raise RuntimeError('transient compile failure')
        return real_compile(*args, **kwargs)

    engine._compile_execution_graph = flaky_compile  # type: ignore[method-assign]

    # 1) Valid S1 arrives; the apply fails and the engine keeps S0.
    provider._notify_watcher(dict(S1))
    assert (
        engine.execution_graph.validated_sources.sources.hash() == s0_hash
    ), 'engine should still serve S0 after a failed apply'
    # The OLD dedup (against _current_sources) WOULD now wedge — it advanced to S1
    # so every re-delivery is suppressed...
    assert provider._current_sources.hash() == s1_hash
    # ...but the fix dedups against the APPLIED hash, still S0, so re-delivery retries.
    assert provider._applied_sources_hash == s0_hash

    # 2) etcd re-delivers S1 (FullSyncOne). The engine recompiles and recovers —
    #    its live execution graph is now S1, with no restart.
    provider._notify_watcher(dict(S1))
    assert (
        engine.execution_graph.validated_sources.sources.hash() == s1_hash
    ), 'engine must serve S1 after re-delivery (self-healed)'
    assert provider._applied_sources_hash == s1_hash
