"""Tests for the schema-source seam in load_and_register_specialized_graphs.

These exercise WHERE schemas come from (in-memory ``schemas=`` map vs the disk
``resolve_schemas_dir``), not the specialization itself — ``specialize_graph`` and
the loaders are patched so the routing decision is isolated.
"""

from __future__ import annotations

from typing import List, Tuple

import osprey.engine.executor.typed_contract_dispatch as dispatch
from osprey.engine.executor.typed_contract_dispatch import load_and_register_specialized_graphs

_ACTIONS = ['guild_joined', 'message_sent']


def _run(monkeypatch, prune, shadow, schemas, *, resolve_should_be_called: bool):
    """Drive load_and_register_specialized_graphs with the loaders patched.

    Returns the list of (action_name, source) tuples for which a schema was 'loaded'.
    """
    loaded_from: List[Tuple[str, str]] = []
    registered: List[str] = []

    def fake_specialize(full_graph, schema, index=None):
        # schema is the sentinel we returned below; pass it straight through.
        return schema

    def fake_from_sources(action_name, schemas_map):
        loaded_from.append((action_name, 'sources'))
        return object()  # truthy sentinel -> a "specialized graph"

    def fake_from_disk(action_name, schemas_dir):
        loaded_from.append((action_name, 'disk'))
        return object()

    resolve_calls = {'n': 0}

    def fake_resolve():
        resolve_calls['n'] += 1
        from pathlib import Path

        return Path('/fake/schemas')

    monkeypatch.setattr(dispatch, 'specialize_graph', fake_specialize)
    # The corpus-wide index is a real collaborator that needs a real graph; this suite
    # isolates the schema-source seam, so stub it out alongside specialize_graph.
    monkeypatch.setattr(dispatch, 'build_specialization_index', lambda full_graph: object())
    monkeypatch.setattr(dispatch, 'load_schema_for_action_from_sources', fake_from_sources)
    monkeypatch.setattr(dispatch, 'load_schema_for_action', fake_from_disk)
    monkeypatch.setattr(dispatch, 'resolve_schemas_dir', fake_resolve)

    count = load_and_register_specialized_graphs(
        full_graph=object(),
        prune_filter=prune,
        shadow_filter=shadow,
        get_action_names=lambda: _ACTIONS,
        register=lambda name, graph: registered.append(name),
        schemas=schemas,
    )

    if resolve_should_be_called:
        assert resolve_calls['n'] >= 1, 'expected disk resolution to be used'
    else:
        assert resolve_calls['n'] == 0, 'resolve_schemas_dir must NOT be called when schemas= provided'

    return count, loaded_from, registered


class TestSchemaSourceSeam:
    def test_loads_from_sources_map_when_provided(self, monkeypatch) -> None:
        schemas = {'schemas/guild_joined.json': '{}'}
        count, loaded_from, registered = _run(
            monkeypatch,
            prune=frozenset({'*'}),
            shadow=frozenset(),
            schemas=schemas,
            resolve_should_be_called=False,
        )
        assert count == len(_ACTIONS)
        assert all(src == 'sources' for _, src in loaded_from)
        assert set(registered) == set(_ACTIONS)

    def test_falls_back_to_disk_when_schemas_none(self, monkeypatch) -> None:
        count, loaded_from, registered = _run(
            monkeypatch,
            prune=frozenset({'*'}),
            shadow=frozenset(),
            schemas=None,
            resolve_should_be_called=True,
        )
        assert count == len(_ACTIONS)
        assert all(src == 'disk' for _, src in loaded_from)

    def test_empty_schemas_map_falls_back_to_disk(self, monkeypatch) -> None:
        # An empty (falsy) schemas map means "no etcd schemas" -> use disk.
        count, loaded_from, _ = _run(
            monkeypatch,
            prune=frozenset({'guild_joined'}),
            shadow=frozenset(),
            schemas={},
            resolve_should_be_called=True,
        )
        assert count == 1
        assert loaded_from == [('guild_joined', 'disk')]

    def test_returns_zero_when_both_filters_empty(self, monkeypatch) -> None:
        # Neither gate set: returns 0 and reads NOTHING (no resolve, no action names).
        resolve_called = {'n': 0}
        monkeypatch.setattr(dispatch, 'resolve_schemas_dir', lambda: resolve_called.__setitem__('n', 1))

        def _boom():
            raise AssertionError('get_action_names must not be called when both filters empty')

        count = load_and_register_specialized_graphs(
            full_graph=object(),
            prune_filter=frozenset(),
            shadow_filter=frozenset(),
            get_action_names=_boom,
            register=lambda name, graph: None,
            schemas={'schemas/guild_joined.json': '{}'},
        )
        assert count == 0
        assert resolve_called['n'] == 0
