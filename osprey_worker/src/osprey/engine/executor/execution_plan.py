"""Immutable scheduling metadata for full execution graphs.

Each source's ``source_indices`` sequence must be a topologically ordered transitive
closure: every predecessor of an activated chain must be activated in the same source
or an earlier source. Full graphs satisfy this because their per-source sorted chains
are built by recursively walking dependencies. Pruned and specialized graphs filter
those closures, so they must keep using the legacy scheduler that explicitly drops
pruned predecessor edges.

``ExecutionPlan`` is shared by every action using a graph. All mutable scheduling
state remains in a per-action ``ExecutionPlanState``.
"""

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Mapping, Tuple

from osprey.engine.ast.grammar import Source
from osprey.engine.utils.periodic_execution_yielder import maybe_periodic_yield

from .dependency_chain import DependencyChain

if TYPE_CHECKING:
    from .execution_graph import ExecutionGraph


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ExecutionPlan:
    """Precomputed chain indices and adjacency for an immutable full graph."""

    chains: Tuple[DependencyChain, ...]
    index_by_chain_id: Mapping[int, int]
    predecessors: Tuple[Tuple[int, ...], ...]
    successors: Tuple[Tuple[int, ...], ...]
    source_indices: Mapping[Source, Tuple[int, ...]]

    @classmethod
    def from_graph(cls, graph: 'ExecutionGraph') -> 'ExecutionPlan':
        chains_by_id: dict[int, DependencyChain] = {}
        source_chain_ids: dict[Source, tuple[int, ...]] = {}
        for source in graph.validated_sources.sources:
            chains = tuple(graph.get_sorted_dependency_chain(source))
            source_chain_ids[source] = tuple(id(chain) for chain in chains)
            for chain in chains:
                chains_by_id[id(chain)] = chain
                maybe_periodic_yield()

        chains = tuple(chains_by_id.values())
        del chains_by_id
        index_by_chain_id: dict[int, int] = {}
        for index, chain in enumerate(chains):
            index_by_chain_id[id(chain)] = index
            maybe_periodic_yield()

        source_indices: dict[Source, tuple[int, ...]] = {}
        for source, chain_ids in source_chain_ids.items():
            indices: list[int] = []
            for chain_id in chain_ids:
                indices.append(index_by_chain_id[chain_id])
                maybe_periodic_yield()
            source_indices[source] = tuple(indices)
        del source_chain_ids

        predecessor_lists: list[tuple[int, ...]] = []
        for chain in chains:
            predecessor_lists.append(tuple(index_by_chain_id[id(predecessor)] for predecessor in chain.dependent_on))
            maybe_periodic_yield()
        predecessors = tuple(predecessor_lists)
        del predecessor_lists

        successors_lists: list[list[int]] = [[] for _ in chains]
        for successor, predecessor_indices in enumerate(predecessors):
            for predecessor in predecessor_indices:
                successors_lists[predecessor].append(successor)
            maybe_periodic_yield()

        successor_tuples: list[tuple[int, ...]] = []
        for items in successors_lists:
            successor_tuples.append(tuple(items))
            items.clear()
            maybe_periodic_yield()
        del successors_lists
        successors = tuple(successor_tuples)
        del successor_tuples

        return cls(
            chains=chains,
            index_by_chain_id=MappingProxyType(index_by_chain_id),
            predecessors=predecessors,
            successors=successors,
            source_indices=MappingProxyType(source_indices),
        )


_INACTIVE = -3
_OUT = -1
_DONE = -2


class LateDependencyActivationError(RuntimeError):
    """Raised when source activation violates the plan's transitive-closure invariant."""


class ExecutionPlanState:
    """Mutable per-action state for an :class:`ExecutionPlan`.

    ``_remaining`` stores the number of unfinished active predecessors when it is
    non-negative. Negative sentinels mean the chain is inactive (``-3``), has been
    returned by :meth:`get_ready` (``-1``), or is done (``-2``).

    ``_activation_rank`` reproduces the legacy sorter's node insertion order. Its
    ``prepare()`` rescans every zero-predecessor node in insertion order, so source
    activation sorts the complete ready set by rank. Successor edges are likewise
    inserted when their successor is activated, so newly ready successors are rank
    sorted after a completion before being appended to any already-ready work.
    """

    __slots__ = ('_plan', '_active', '_remaining', '_activation_rank', '_next_rank', '_ready')

    def __init__(self, plan: ExecutionPlan) -> None:
        self._plan = plan
        self._active = bytearray(len(plan.chains))
        self._remaining = [_INACTIVE] * len(plan.chains)
        self._activation_rank = [_INACTIVE] * len(plan.chains)
        self._next_rank = 0
        self._ready: list[int] = []

    def activate_source(self, source: Source) -> None:
        new_indices = tuple(index for index in self._plan.source_indices[source] if not self._active[index])
        if not new_indices:
            return

        new_set = set(new_indices)
        for index in new_indices:
            active_successors = tuple(
                successor for successor in self._plan.successors[index] if self._active[successor]
            )
            if active_successors:
                node = self._plan.chains[index].executor.node
                span = node.span
                raise LateDependencyActivationError(
                    f'source {source.path!r} would activate chain {index} '
                    f'({type(node).__name__} at {span.source.path}:{span.start_line}:{span.start_pos}) '
                    f'after active successor chain(s) {active_successors}'
                )

        counts = tuple(
            sum(
                1
                for predecessor in self._plan.predecessors[index]
                if (self._active[predecessor] or predecessor in new_set) and self._remaining[predecessor] != _DONE
            )
            for index in new_indices
        )

        for index, count in zip(new_indices, counts):
            self._active[index] = 1
            self._remaining[index] = count
            self._activation_rank[index] = self._next_rank
            self._next_rank += 1
            if count == 0:
                self._ready.append(index)

        self._ready.sort(key=self._activation_rank.__getitem__)

    def get_ready(self) -> tuple[DependencyChain, ...]:
        indices = tuple(self._ready)
        self._ready.clear()
        for index in indices:
            self._remaining[index] = _OUT
        return tuple(self._plan.chains[index] for index in indices)

    def done(self, chain: DependencyChain) -> None:
        index = self._plan.index_by_chain_id[id(chain)]
        if self._remaining[index] != _OUT:
            raise ValueError(f'chain {index} was not passed out')

        self._remaining[index] = _DONE
        newly_ready: list[int] = []
        for successor in self._plan.successors[index]:
            if not self._active[successor] or self._remaining[successor] < 0:
                continue
            self._remaining[successor] -= 1
            if self._remaining[successor] == 0:
                newly_ready.append(successor)

        newly_ready.sort(key=self._activation_rank.__getitem__)
        self._ready.extend(newly_ready)
