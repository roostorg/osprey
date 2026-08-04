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
        index_by_chain_id: dict[int, int] = {}
        for index, chain in enumerate(chains):
            index_by_chain_id[id(chain)] = index
            maybe_periodic_yield()

        predecessor_lists: list[tuple[int, ...]] = []
        for chain in chains:
            predecessor_lists.append(
                tuple(index_by_chain_id[id(predecessor)] for predecessor in chain.dependent_on)
            )
            maybe_periodic_yield()
        predecessors = tuple(predecessor_lists)

        successors_lists: list[list[int]] = [[] for _ in chains]
        for successor, predecessor_indices in enumerate(predecessors):
            for predecessor in predecessor_indices:
                successors_lists[predecessor].append(successor)
            maybe_periodic_yield()

        successors: list[tuple[int, ...]] = []
        for items in successors_lists:
            successors.append(tuple(items))
            maybe_periodic_yield()

        source_indices: dict[Source, tuple[int, ...]] = {}
        for source, chain_ids in source_chain_ids.items():
            indices: list[int] = []
            for chain_id in chain_ids:
                indices.append(index_by_chain_id[chain_id])
                maybe_periodic_yield()
            source_indices[source] = tuple(indices)

        return cls(
            chains=chains,
            index_by_chain_id=MappingProxyType(index_by_chain_id),
            predecessors=predecessors,
            successors=tuple(successors),
            source_indices=MappingProxyType(source_indices),
        )
