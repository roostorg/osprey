"""Immutable schedule data for a full execution graph.

The graph shares one plan across actions. Each action creates separate schedule state.
"""

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Mapping

from osprey.engine.ast.grammar import Source
from osprey.engine.utils.periodic_execution_yielder import maybe_periodic_yield

from .dependency_chain import DependencyChain

if TYPE_CHECKING:
    from .execution_graph import ExecutionGraph


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ExecutionPlan:
    """Store shared indices and edges for a full execution graph."""

    chains: tuple[DependencyChain, ...]
    index_by_chain_id: Mapping[int, int]
    predecessors: tuple[tuple[int, ...], ...]
    successors: tuple[tuple[int, ...], ...]
    source_indices: Mapping[Source, tuple[int, ...]]

    @classmethod
    def from_graph(cls, graph: 'ExecutionGraph') -> 'ExecutionPlan':
        chains_by_id: dict[int, DependencyChain] = {}
        source_chain_ids: dict[Source, tuple[int, ...]] = {}
        for source in graph.validated_sources.sources:
            chains = tuple(graph.get_sorted_dependency_chain(source))
            source_chain_ids[source] = tuple([id(chain) for chain in chains])
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

        predecessor_tuples: list[tuple[int, ...]] = []
        for chain in chains:
            predecessor_indices = [index_by_chain_id[id(predecessor)] for predecessor in chain.dependent_on]
            predecessor_tuples.append(tuple(predecessor_indices))
            maybe_periodic_yield()
        predecessors = tuple(predecessor_tuples)
        del predecessor_tuples

        successor_lists: list[list[int]] = [[] for _ in chains]
        for successor, predecessor_indices in enumerate(predecessors):
            for predecessor in predecessor_indices:
                successor_lists[predecessor].append(successor)
            maybe_periodic_yield()

        successor_tuples: list[tuple[int, ...]] = []
        for items in successor_lists:
            successor_tuples.append(tuple(items))
            items.clear()
            maybe_periodic_yield()
        del successor_lists
        successors = tuple(successor_tuples)
        del successor_tuples

        return cls(
            chains=chains,
            index_by_chain_id=MappingProxyType(index_by_chain_id),
            predecessors=predecessors,
            successors=successors,
            source_indices=MappingProxyType(source_indices),
        )
