"""Shared runtime wiring for typed action contracts.

Used by BOTH the gevent (`osprey.worker.lib.osprey_engine.OspreyEngine`) and asyncio
(`osprey.async_worker.engine`) engines. The engines differ only in HOW they execute one
graph (a gevent pool vs the asyncio executor); the decisions of WHICH graph(s) to run,
schema loading + specialization, and shadow-divergence recording are identical and live
here so they are defined once.

Each engine keeps its own `_specialized_graphs` / `_prune_filter` / `_shadow_filter`
state and its thin `_load_and_register_schemas` + `execute` methods, delegating the shared
logic to these functions.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Dict, FrozenSet, Iterable, List, Mapping, Optional, Tuple

from osprey.engine.executor.execution_graph import ExecutionGraph
from osprey.engine.executor.graph_specializer import (
    SpecializationIndex,
    SpecializedExecutionGraph,
    build_specialization_index,
    shadow_divergences,
    specialize_graph,
)
from osprey.engine.schema.schema_loader import (
    ActionSchema,
    SchemaLoadError,
    filter_includes,
    load_schema_for_action,
    load_schema_for_action_from_sources,
    resolve_schemas_dir,
)
from osprey.engine.utils.periodic_execution_yielder import maybe_periodic_yield, periodic_execution_yield
from osprey.worker.lib.instruments import metrics

log = logging.getLogger(__name__)

_YIELD_EXECUTION_TIME_MS = 5
_YIELD_TIME_MS = 25
"""Yield cadence for the specialization pass. Same values the compile path uses, so both
CPU-bound rules-reload phases have the same duty cycle."""


def schema_source_for(schemas: Optional[Mapping[str, str]]) -> Tuple[bool, Optional[Path]]:
    """Resolve where schemas come from: ``(use_sources, schemas_dir)``.

    The in-memory ``schemas`` map carried on the etcd Sources payload wins when non-empty
    (that is the only source the prod worker has, which ships no schemas directory on disk);
    otherwise fall back to the on-disk directory. ``(False, None)`` means no schema source is
    available at all, so specialization must no-op.
    """
    if schemas:
        return True, None
    return False, resolve_schemas_dir()


def filter_matching_actions(register_filter: FrozenSet[str], action_names: Iterable[str]) -> List[str]:
    """The action names the typed-contract allowlist selects, in a stable (sorted) order.

    Sorted because ``get_known_action_names`` returns a set, and the warm-up order needs to be
    reproducible across pods for a given corpus.
    """
    return sorted(name for name in action_names if filter_includes(register_filter, name))


def _load_action_schema(
    action_name: str,
    schemas: Optional[Mapping[str, str]],
    schemas_dir: Optional[Path],
) -> Optional[ActionSchema]:
    """One action's schema, from the etcd Sources map when given, else from disk.

    Returns ``None`` both when the action simply has no schema and when its schema fails to
    parse (logged) — a broken schema must degrade to full-graph semantics, never raise into a
    reload or a warm-up pass.
    """
    try:
        if schemas:  # in-memory etcd Sources map (the truthiness narrows Optional for mypy)
            return load_schema_for_action_from_sources(action_name, schemas)
        if schemas_dir is not None:
            return load_schema_for_action(action_name, schemas_dir)
    except SchemaLoadError as e:
        log.warning("Failed to load schema for %s: %s", action_name, e)
    return None


def specialize_one_action(
    full_graph: ExecutionGraph,
    action_name: str,
    index: Optional[SpecializationIndex] = None,
    schemas: Optional[Mapping[str, str]] = None,
    schemas_dir: Optional[Path] = None,
    yield_during_specialize: bool = False,
) -> Optional[ExecutionGraph]:
    """Load + specialize exactly ONE action. Returns ``None`` if it has no usable schema.

    The single-action counterpart to ``load_and_register_specialized_graphs``, for the
    background warmer: it owns its own ``periodic_execution_yield`` block (so each action gets
    the same GIL duty cycle the eager pass gives the whole batch) and therefore must NOT be
    called from inside one — the context manager refuses to nest.

    ``index`` is the whole-corpus, action-independent analysis. Callers warming many actions
    against one graph must build it once and pass it in; omitting it makes every call rebuild
    it, which is what #25 removed from the per-action cost.
    """
    schema = _load_action_schema(action_name, schemas, schemas_dir)
    if schema is None:
        return None
    with periodic_execution_yield(
        on=yield_during_specialize,
        execution_time_ms=_YIELD_EXECUTION_TIME_MS,
        yield_time_ms=_YIELD_TIME_MS,
    ):
        return specialize_graph(full_graph, schema, index=index)


def load_and_register_specialized_graphs(
    full_graph: ExecutionGraph,
    prune_filter: FrozenSet[str],
    shadow_filter: FrozenSet[str],
    get_action_names: Callable[[], Iterable[str]],
    register: Callable[[str, ExecutionGraph], None],
    schemas: Optional[Mapping[str, str]] = None,
    yield_during_specialize: bool = False,
) -> int:
    """Load schemas for allowlisted actions, specialize them against ``full_graph``, and
    register each via ``register(action_name, specialized_graph)``. Returns the count
    registered.

    Schemas come from one of two sources: the in-memory ``schemas`` map carried on the etcd
    Sources payload (when non-empty), else the on-disk schemas directory resolved via
    ``resolve_schemas_dir``. The Sources path lets the specializer activate on the
    etcd-sourced prod worker, which has no schemas directory on disk.

    No-op (returns 0) when neither gate is set, or when no schemas are provided AND no
    schemas dir resolves — so shipping schema files cannot change behavior until an action is
    explicitly listed in ``OSPREY_TYPED_CONTRACT_PRUNING`` / ``_SHADOW``. ``get_action_names``
    is called lazily (only past those gate checks) so the disabled-by-default path does no work.

    ``yield_during_specialize`` mirrors the compile path's ``periodic_execution_yield``: this
    pass is CPU-bound and GIL-holding for as long as compilation is, and it runs on the same
    single-worker pool, so without the yields it starves the caller's event loop / gevent hub
    exactly the way an unyielded compile does. Callers must not already be inside a
    ``periodic_execution_yield`` block (it refuses to nest).
    """
    register_filter = prune_filter | shadow_filter
    if not register_filter:
        return 0
    use_sources, schemas_dir = schema_source_for(schemas)
    if not use_sources and schemas_dir is None:
        return 0
    loaded = 0
    with periodic_execution_yield(
        on=yield_during_specialize,
        execution_time_ms=_YIELD_EXECUTION_TIME_MS,
        yield_time_ms=_YIELD_TIME_MS,
    ):
        # Whole-corpus analysis is action-independent: compute it once and share it across
        # every action, so each specialization only walks its own source closure. Built
        # lazily so a run that loads no schema at all still touches nothing.
        index: Optional[SpecializationIndex] = None
        for action_name in get_action_names():
            maybe_periodic_yield()
            if not filter_includes(register_filter, action_name):
                continue
            schema = _load_action_schema(action_name, schemas, schemas_dir)
            if schema is None:
                continue
            if index is None:
                index = build_specialization_index(full_graph)
            register(action_name, specialize_graph(full_graph, schema, index=index))
            loaded += 1
    if loaded:
        source_desc = "Sources" if use_sources else schemas_dir
        log.info("Loaded %d specialized graphs from %s (prune=%r shadow=%r)",
                 loaded, source_desc, sorted(prune_filter), sorted(shadow_filter))
    return loaded


def resolve_dispatch(
    action_name: str,
    specialized_graphs: Dict[str, ExecutionGraph],
    prune_filter: FrozenSet[str],
    shadow_filter: FrozenSet[str],
    full_graph: ExecutionGraph,
    action_data: Optional[Mapping[str, object]] = None,
    on_miss: Optional[Callable[[str], None]] = None,
) -> Tuple[ExecutionGraph, Optional[ExecutionGraph]]:
    """Decide which graph(s) to run for an action. Returns
    ``(graph_to_serve, shadow_spec_or_None)``:

      * PRUNE  -> ``(specialized, None)``     — serve the lean (pruned + constant-folded) graph
      * SHADOW -> ``(full, specialized)``     — serve full, also run specialized to diff
      * else   -> ``(full, None)``            — default graph, zero overhead

    Schema-less / non-allowlisted actions hit the final case (``dict.get`` is O(1)).

    ``on_miss`` is called with the action name when the action IS allowlisted but has no
    specialized graph registered yet — the signal the asyncio engine's background warmer uses
    to move that action to the front of its queue, so live traffic warms hot actions first
    (see ``osprey.async_worker.typed_contract_warmer``). It must be cheap and non-raising: this
    is the per-action hot path. Omitted by the eager (gevent) engine, where a miss is terminal.

    Presence guard (safety keystone): the specialized graph constant-folds enforcement-feeding
    absent-group nodes, baking in the "absent" assumption. So the PRUNE branch serves the lean
    graph ONLY when every group it assumed absent is genuinely missing from this action's
    ``action_data``; a misclassified payload (an "absent" group actually present) falls back to the
    full graph — preserving the rescue's misclassification safety — and emits a metric so the bad
    schema is visible. SHADOW is unaffected: it always serves full and a misclassification simply
    shows up as a (real, worth-surfacing) shadow divergence.
    """
    spec = specialized_graphs.get(action_name)
    if spec is not None and filter_includes(prune_filter, action_name):
        # Fail CLOSED: serve the lean (folded) graph only when we can VERIFY the fold precondition
        # holds for THIS payload — a SpecializedExecutionGraph whose assumed-absent groups are
        # genuinely absent. Missing action_data, or any non-specialized graph, falls back to the
        # full graph (never serve baked-in folds unguarded) and emits a metric so the gap is visible.
        if (
            isinstance(spec, SpecializedExecutionGraph)
            and action_data is not None
            and spec.absent_groups_satisfied(action_data)
        ):
            return spec, None
        metrics.increment('osprey.typed_contracts.guard_fallback', tags=[f'action:{action_name}'])
    if spec is not None and filter_includes(shadow_filter, action_name):
        return full_graph, spec
    if (
        spec is None
        and on_miss is not None
        and (filter_includes(prune_filter, action_name) or filter_includes(shadow_filter, action_name))
    ):
        on_miss(action_name)
    return full_graph, None


def record_shadow(action_name: str, full_result: object, spec_result: object) -> None:
    """Diff a shadow run's full vs specialized result and emit the divergence metric."""
    issues = shadow_divergences(full_result, spec_result)
    metrics.increment(
        'osprey.typed_contracts.shadow',
        tags=[f'action:{action_name}', f'divergent:{str(bool(issues)).lower()}'],
    )
    if issues:
        log.warning("typed-contract SHADOW DIVERGENCE for %s: %s", action_name, '; '.join(issues[:8]))
