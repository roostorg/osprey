"""Background warm-up of typed-contract specialized graphs — asyncio engine only.

Specializing every allowlisted action inline is measurably too slow to sit on the reload
path. On the production corpus (242 actions) the closure-scoped pass costs 29.35s of CPU
(mean 120.6ms/action, p95 190.7ms), and the periodic-yield wrapper that stops it starving
the event loop stretches that ~6x in wall clock at the 5ms/25ms cadence. Every reload would
pay minutes of degraded duty cycle before the first pruned graph could be served.

This module takes the pass off the reload path entirely:

  * A miss (allowlisted action, no specialized graph yet) serves the FULL graph — i.e.
    exactly today's pre-typed-contract behavior, so a cold action is never *wrong*, only
    unoptimized — and moves that action to the front of the warm-up queue.
  * Boot and every reload seed the queue with all filter-matching actions, so warm-up does
    not wait for per-action traffic; traffic-driven misses jump the queue.
  * One background task warms actions one at a time on the engine's compile thread pool,
    with the periodic-yield wrapper on and a short sleep between actions, then publishes
    each finished graph into the engine's specialized-graph dict.

The gevent engine deliberately stays eager: prod runs the asyncio worker, and the gevent
engine has no event loop for a background task to live on — the equivalent would be a
spawned greenlet whose starvation profile is unmeasured. Not worth the risk on a path that
is being retired.

Reload correctness rests on one rule: a specialization is published only if the graph it
was computed against is STILL the engine's current graph. The graph object captured at
dequeue is compared by identity at publish, so an in-flight specialization for a retired
graph is dropped rather than leaking stale folds into the new generation.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Callable, Deque, FrozenSet, Iterable, Mapping, Optional, Set

from osprey.engine.executor.execution_graph import ExecutionGraph
from osprey.engine.executor.graph_specializer import SpecializationIndex, build_specialization_index
from osprey.engine.executor.typed_contract_dispatch import (
    filter_matching_actions,
    schema_source_for,
    specialize_one_action,
)
from osprey.worker.lib.instruments import metrics

log = logging.getLogger(__name__)

_WARM_INTERVAL_ENV = 'OSPREY_TYPED_CONTRACT_WARM_INTERVAL_MS'
_DEFAULT_WARM_INTERVAL_MS = 50
"""Sleep between two warm-ups. Bounds the warmer's share of the loop independently of the
in-specialization yield cadence: even if one action runs long, the loop always gets a clean
window before the next one starts."""

_PENDING_GAUGE = 'osprey.typed_contracts.warm_pending'
_SPECIALIZED_METRIC = 'osprey.typed_contracts.warm_specialized'
_FAILED_METRIC = 'osprey.typed_contracts.warm_failed'
_NO_SCHEMA_METRIC = 'osprey.typed_contracts.warm_no_schema'
_STALE_METRIC = 'osprey.typed_contracts.warm_stale_dropped'
_DURATION_METRIC = 'osprey.typed_contracts.warm_duration_ms'


def _warm_interval_seconds() -> float:
    raw = os.environ.get(_WARM_INTERVAL_ENV, '').strip()
    if not raw:
        return _DEFAULT_WARM_INTERVAL_MS / 1000.0
    try:
        return max(0, int(raw)) / 1000.0
    except ValueError:
        log.warning('%s=%r is not an integer; using %dms', _WARM_INTERVAL_ENV, raw, _DEFAULT_WARM_INTERVAL_MS)
        return _DEFAULT_WARM_INTERVAL_MS / 1000.0


@dataclass(frozen=True)
class _GenerationInputs:
    """The per-graph inputs every action in one warm-up cycle shares.

    Built once per graph on the pool thread — ``build_specialization_index`` is whole-corpus
    CPU work and ``Sources.schemas()`` copies the whole schema map, neither of which should be
    repeated 242 times. Held as ONE attribute so the loop thread's ``reset()`` can drop it with
    a single (GIL-atomic) assignment instead of racing a multi-field update.
    """

    graph: ExecutionGraph
    index: SpecializationIndex
    schemas: Optional[Mapping[str, str]]
    schemas_dir: Optional[Path]


class TypedContractWarmer:
    """Warms typed-contract specializations in the background for one engine instance.

    All queue state is touched only from the event loop thread; only ``_inputs`` crosses to
    the pool thread, and it is a single immutable attribute.
    """

    def __init__(
        self,
        graph_provider: Callable[[], ExecutionGraph],
        register_filter_provider: Callable[[], FrozenSet[str]],
        action_names_provider: Callable[[], Iterable[str]],
        register: Callable[[str, ExecutionGraph], None],
        thread_pool: ThreadPoolExecutor,
        on_drained: Optional[Callable[[], None]] = None,
        yield_during_specialize: bool = True,
    ) -> None:
        self._graph_provider = graph_provider
        self._register_filter_provider = register_filter_provider
        self._action_names_provider = action_names_provider
        self._register = register
        self._thread_pool = thread_pool
        self._on_drained = on_drained
        self._yield_during_specialize = yield_during_specialize
        self._interval_seconds = _warm_interval_seconds()

        self._queue: Deque[str] = deque()
        self._pending: Set[str] = set()
        # Actions already moved to the front by traffic. `deque.remove` is O(n); this caps it
        # at one move per action per generation so a hot un-warm action cannot make every
        # dispatch walk the queue.
        self._promoted: Set[str] = set()
        # Settled for THIS graph generation: specialized, schema-less, or failed. Doubles as
        # the negative cache — `reset()` clears it, so a failure is retried against the next
        # graph but never re-attempted against the one it failed on.
        self._settled: Set[str] = set()
        self._inflight: Optional[str] = None
        self._inputs: Optional[_GenerationInputs] = None
        self._task: Optional['asyncio.Task[None]'] = None
        self._specialized_count = 0
        self._drained_hook_fired = False
        self._window_start: Optional[float] = None

    @property
    def pending_count(self) -> int:
        return len(self._queue) + (1 if self._inflight is not None else 0)

    def reset(self) -> None:
        """Drop all state for the retired graph. Call while swapping the execution graph,
        next to ``_specialized_graphs.clear()``.

        Deliberately does NOT cancel the running task: cancelling cannot stop the thread
        already inside ``specialize_graph`` anyway, and the publish-time identity guard
        already discards its result. The task simply carries on into the re-seeded queue.

        ``_inflight`` is cleared for the same reason — whatever is running is now stale, so the
        following ``seed()`` must be free to re-queue that action against the new graph rather
        than dedup it against a specialization whose result is about to be thrown away.

        Caveat: a warm-up in flight holds the retired graph as a thread-local, so the reload's
        ``gc.collect()`` cannot reclaim it that cycle. Bounded (one graph, until that single
        specialization returns) and self-healing on the next collection — cheaper than putting
        the reload back to waiting on specialization, which is what this class exists to avoid.
        """
        self._queue.clear()
        self._pending.clear()
        self._promoted.clear()
        self._settled.clear()
        self._inflight = None
        self._inputs = None
        self._specialized_count = 0
        self._drained_hook_fired = False
        self._window_start = None

    def seed(self) -> None:
        """Queue every action the current allowlist selects, then make sure the task runs.

        Safe to call with no running event loop (engine ``__init__`` is sync, pre-loop): the
        queue is filled and the task starts on the first reload or the first dispatch miss.
        """
        register_filter = self._register_filter_provider()
        if not register_filter:
            return
        try:
            action_names = filter_matching_actions(register_filter, self._action_names_provider())
        except Exception:
            log.exception('typed-contract warm-up could not enumerate action names')
            return
        for action_name in action_names:
            if action_name in self._settled or action_name in self._pending or action_name == self._inflight:
                continue
            self._queue.append(action_name)
            self._pending.add(action_name)
        if self._queue and self._window_start is None:
            self._window_start = monotonic()
        metrics.gauge(_PENDING_GAUGE, self.pending_count)
        self.ensure_running()

    def note_miss(self, action_name: str) -> None:
        """Hot-path hook for ``resolve_dispatch``: this action took traffic before it was warm,
        so warm it next. Must stay cheap and non-raising."""
        if action_name in self._settled or action_name in self._promoted or action_name == self._inflight:
            return
        self._promoted.add(action_name)
        if action_name in self._pending:
            try:
                self._queue.remove(action_name)
            except ValueError:  # already dequeued between the membership check and here
                pass
        else:
            self._pending.add(action_name)
            if self._window_start is None:
                self._window_start = monotonic()
        self._queue.appendleft(action_name)
        self.ensure_running()

    def ensure_running(self) -> None:
        """Start the drain task if there is work and it is not already draining.

        The task is per warm-up cycle, not immortal: it exits when the queue empties, and a
        later seed or traffic miss starts a new one. Nothing idles between reloads.
        """
        if not self._queue:
            return
        if self._task is not None and not self._task.done():
            return
        try:
            self._task = asyncio.get_running_loop().create_task(self._drain())
        except RuntimeError:
            # No running loop (engine construction is pre-loop). The queue keeps the work;
            # the first reload or dispatch miss on the loop starts the task.
            pass

    def cancel(self) -> None:
        """Best-effort stop, for engine shutdown."""
        task = self._task
        if task is None or task.done():
            return
        try:
            task.cancel()
        except RuntimeError:
            log.debug('typed-contract warm-up task could not be cancelled from this thread')

    async def shutdown_async(self) -> None:
        """Cancel and drain the warmer task on its owning event loop."""
        task = self._task
        if task is None:
            return
        self.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _drain(self) -> None:
        try:
            while True:
                action_name = self._next()
                if action_name is None:
                    break
                # Captured BEFORE the await: this is the graph the result is valid for.
                graph = self._graph_provider()
                try:
                    specialized = await self._specialize(action_name, graph)
                except Exception:
                    log.exception('typed-contract warm-up failed for %s', action_name)
                    self._settle_failure(action_name, graph)
                else:
                    self._settle(action_name, graph, specialized)
                finally:
                    self._inflight = None
                metrics.gauge(_PENDING_GAUGE, self.pending_count)
                if self._interval_seconds:
                    await asyncio.sleep(self._interval_seconds)
            self._finish_window()
        except asyncio.CancelledError:
            raise
        except Exception:
            # Never let the warmer's own bookkeeping take down the task: dispatch keeps
            # serving full graphs, and the next seed starts a fresh one.
            log.exception('typed-contract warm-up loop aborted')

    def _next(self) -> Optional[str]:
        while self._queue:
            action_name = self._queue.popleft()
            self._pending.discard(action_name)
            if action_name in self._settled:
                continue
            self._inflight = action_name
            return action_name
        return None

    async def _specialize(self, action_name: str, graph: ExecutionGraph) -> Optional[ExecutionGraph]:
        """Specialize one action on the compile thread pool.

        Shares the engine's single-worker pool, so a concurrent reload's compile queues behind
        at most ONE action (p95 190.7ms of CPU, stretched by the yields) — irrelevant next to a
        multi-second compile, and worth not adding a second thread that competes for the GIL.
        """
        return await asyncio.get_running_loop().run_in_executor(
            self._thread_pool, self._specialize_sync, action_name, graph
        )

    def _specialize_sync(self, action_name: str, graph: ExecutionGraph) -> Optional[ExecutionGraph]:
        inputs = self._inputs
        if inputs is None or inputs.graph is not graph:
            schemas_map = graph.validated_sources.sources.schemas()
            use_sources, schemas_dir = schema_source_for(schemas_map)
            if not use_sources and schemas_dir is None:
                return None
            inputs = _GenerationInputs(
                graph=graph,
                index=build_specialization_index(graph),
                schemas=schemas_map if use_sources else None,
                schemas_dir=schemas_dir,
            )
            self._inputs = inputs
        return specialize_one_action(
            graph,
            action_name,
            index=inputs.index,
            schemas=inputs.schemas,
            schemas_dir=inputs.schemas_dir,
            yield_during_specialize=self._yield_during_specialize,
        )

    def _settle(self, action_name: str, graph: ExecutionGraph, specialized: Optional[ExecutionGraph]) -> None:
        if self._graph_provider() is not graph:
            # The graph was swapped while this ran. Publishing now would inject folds derived
            # from a retired corpus. Drop it; `reset()` already re-queued the action.
            metrics.increment(_STALE_METRIC)
            return
        self._settled.add(action_name)
        if specialized is None:
            # No schema (or an unparseable one). Negative-cache it, otherwise every dispatch
            # for this allowlisted-but-schema-less action re-queues it forever.
            metrics.increment(_NO_SCHEMA_METRIC)
            return
        self._register(action_name, specialized)
        self._specialized_count += 1
        metrics.increment(_SPECIALIZED_METRIC)

    def _settle_failure(self, action_name: str, graph: ExecutionGraph) -> None:
        metrics.increment(_FAILED_METRIC, tags=[f'action:{action_name}'])
        # Only negative-cache against the generation that actually failed — otherwise a
        # reload-race failure would suppress a legitimate retry on the new graph.
        if self._graph_provider() is graph:
            self._settled.add(action_name)

    def _finish_window(self) -> None:
        metrics.gauge(_PENDING_GAUGE, 0)
        if self._window_start is not None:
            metrics.timing(_DURATION_METRIC, (monotonic() - self._window_start) * 1000.0)
            self._window_start = None
        # The queue and in-flight slot are both empty now, so no specialization can still
        # consume this generation's construction-only index and schema map. Drop them before
        # the drain hook's gc.freeze so they are reclaimed rather than frozen permanently.
        self._inputs = None
        if self._specialized_count and self._on_drained is not None and not self._drained_hook_fired:
            # Debounced to once per generation: at most one extra freeze per reload cycle.
            self._drained_hook_fired = True
            try:
                self._on_drained()
            except Exception:
                log.exception('typed-contract warm-up drain hook failed')
