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
from typing import Callable, Dict, FrozenSet, Iterable, Mapping, Optional, Tuple

from osprey.engine.executor.execution_graph import ExecutionGraph
from osprey.engine.executor.graph_specializer import (
    SpecializedExecutionGraph,
    shadow_divergences,
    specialize_graph,
)
from osprey.engine.schema.schema_loader import (
    SchemaLoadError,
    filter_includes,
    load_schema_for_action,
    load_schema_for_action_from_sources,
    resolve_schemas_dir,
)
from osprey.worker.lib.instruments import metrics

log = logging.getLogger(__name__)


def load_and_register_specialized_graphs(
    full_graph: ExecutionGraph,
    prune_filter: FrozenSet[str],
    shadow_filter: FrozenSet[str],
    get_action_names: Callable[[], Iterable[str]],
    register: Callable[[str, ExecutionGraph], None],
    schemas: Optional[Mapping[str, str]] = None,
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
    """
    register_filter = prune_filter | shadow_filter
    if not register_filter:
        return 0
    use_sources = bool(schemas)
    schemas_dir = None if use_sources else resolve_schemas_dir()
    if not use_sources and schemas_dir is None:
        return 0
    loaded = 0
    for action_name in get_action_names():
        if not filter_includes(register_filter, action_name):
            continue
        try:
            if schemas:  # in-memory etcd Sources map (the truthiness narrows Optional for mypy)
                schema = load_schema_for_action_from_sources(action_name, schemas)
            else:
                assert schemas_dir is not None  # guaranteed by the gate above
                schema = load_schema_for_action(action_name, schemas_dir)
        except SchemaLoadError as e:
            log.warning('Failed to load schema for %s: %s', action_name, e)
            continue
        if schema is None:
            continue
        register(action_name, specialize_graph(full_graph, schema))
        loaded += 1
    if loaded:
        source_desc = 'Sources' if use_sources else schemas_dir
        log.info(
            'Loaded %d specialized graphs from %s (prune=%r shadow=%r)',
            loaded,
            source_desc,
            sorted(prune_filter),
            sorted(shadow_filter),
        )
    return loaded


def resolve_dispatch(
    action_name: str,
    specialized_graphs: Dict[str, ExecutionGraph],
    prune_filter: FrozenSet[str],
    shadow_filter: FrozenSet[str],
    full_graph: ExecutionGraph,
    action_data: Optional[Mapping[str, object]] = None,
) -> Tuple[ExecutionGraph, Optional[ExecutionGraph]]:
    """Decide which graph(s) to run for an action. Returns
    ``(graph_to_serve, shadow_spec_or_None)``:

      * PRUNE  -> ``(specialized, None)``     — serve the lean (pruned + constant-folded) graph
      * SHADOW -> ``(full, specialized)``     — serve full, also run specialized to diff
      * else   -> ``(full, None)``            — default graph, zero overhead

    Schema-less / non-allowlisted actions hit the final case (``dict.get`` is O(1)).

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
    return full_graph, None


def record_shadow(action_name: str, full_result: object, spec_result: object) -> None:
    """Diff a shadow run's full vs specialized result and emit the divergence metric."""
    issues = shadow_divergences(full_result, spec_result)
    metrics.increment(
        'osprey.typed_contracts.shadow',
        tags=[f'action:{action_name}', f'divergent:{str(bool(issues)).lower()}'],
    )
    if issues:
        log.warning('typed-contract SHADOW DIVERGENCE for %s: %s', action_name, '; '.join(issues[:8]))
