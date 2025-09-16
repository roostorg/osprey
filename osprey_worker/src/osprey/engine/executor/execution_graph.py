from typing import TYPE_CHECKING, Any, Dict, Hashable, Iterator, List, Optional, Sequence, Set, TypeVar

from osprey.engine.ast.grammar import Assign, ASTNode, Load, Name, Source, Statement
from osprey.engine.utils.periodic_execution_yielder import maybe_periodic_yield

from .dependency_chain import DependencyChain

if TYPE_CHECKING:
    from osprey.engine.ast_validator.validation_context import ValidatedSources

    from .node_executor._base_node_executor import BaseNodeExecutor
    from .node_executor_registry import NodeExecutorRegistry


T = TypeVar('T', bound=Hashable)


class ExecutionGraph:
    """An execution graph stores all the dependency chains required to execute a validated sources collection.
    Generally, this class is not used directly, and instead, the factory method `compile_execution_graph` should
    be used."""

    __slots__ = (
        '_root_node_executor_mapping',
        '_assignment_executor_mapping',
        '_node_executor_registry',
        '_validated_sources',
        '_sorted_dependency_chains',
        '_nodes_to_unwrap',
    )

    _root_node_executor_mapping: Dict[int, DependencyChain]
    """This is a mapping of the node to its dependency chain. """

    _assignment_executor_mapping: Dict[str, DependencyChain]
    """This is a mapping of an identifier (stored Name), to the dependency chain which would execute it."""

    _node_executor_registry: 'NodeExecutorRegistry'
    """The node executor registry that will be used to construct executors for the AST nodes within the
    validated sources."""

    _validated_sources: 'ValidatedSources'
    """The validated sources that this execution graph was constructed from."""

    _sorted_dependency_chains: Dict[Source, Sequence[DependencyChain]]
    """A dict of sources to sorted dependency chains."""

    _nodes_to_unwrap: Set[int]
    """ID's for nodes that need to be unwrapped to its inner type when used."""

    def __init__(
        self, node_executor_registry: 'NodeExecutorRegistry', sources: 'ValidatedSources', nodes_to_unwrap: Set[int]
    ):
        self._root_node_executor_mapping = {}
        self._assignment_executor_mapping = {}
        self._node_executor_registry = node_executor_registry
        self._validated_sources = sources
        self._sorted_dependency_chains = {}
        self._nodes_to_unwrap = nodes_to_unwrap

    @property
    def validated_sources(self) -> 'ValidatedSources':
        return self._validated_sources

    def get_dependency_chain(self, statement: Statement) -> DependencyChain:
        """Gets the dependency chain that would be used to execute a given statement. This is a function
        used by the executor as it crawls through the entry-point, finding things to execute."""
        return self._root_node_executor_mapping[id(statement)]

    def get_entry_point(self) -> Source:
        """Returns the entry-point of the sources collection, or the 'main' file."""
        return self._validated_sources.sources.get_entry_point()

    def get_assignment_dependency_chain(self, name: Name) -> DependencyChain:
        """Gets the dependency chain that will be resolving the value of an stored name."""
        return self._assignment_executor_mapping[name.identifier_key]

    def get_sorted_dependency_chain(self, source: Source) -> Sequence[DependencyChain]:
        """Given a source, return the dependency chain that must be executed in order for the source to be
        fully resolved - in the order it must be executed."""
        return self._sorted_dependency_chains[source]

    def should_unwrap(self, node: ASTNode) -> bool:
        """Whether we need to unwrap the value that is represented by this node before using it."""
        return id(node) in self._nodes_to_unwrap

    def _get_executor_for(self, node: ASTNode) -> 'BaseNodeExecutor[Any, Any]':
        return self._node_executor_registry.construct_executor_for(node, validated_sources=self._validated_sources)

    def _build_dependency_chain(self, node: ASTNode) -> DependencyChain:
        # If we're using a feature, re-use the existing chain for it.
        if isinstance(node, Name) and isinstance(node.context, Load):
            return self.get_assignment_dependency_chain(node)

        executor = self._get_executor_for(node)
        dependent_on = tuple(self._build_dependency_chain(node) for node in executor.get_dependent_nodes())
        return DependencyChain(executor=executor, dependent_on=dependent_on)

    def _add_validated_source(self, source: Source) -> None:
        for statement in source.ast_root.statements:
            chain = self._build_dependency_chain(statement)
            self._root_node_executor_mapping[id(statement)] = chain

            if isinstance(statement, Assign):
                self._assignment_executor_mapping[statement.target.identifier_key] = chain

    def _add_sorted_dependency_chain(self, source: Source, sorted_dependency_chain: Sequence[DependencyChain]) -> None:
        self._sorted_dependency_chains[source] = sorted_dependency_chain


def compile_execution_graph(
    validated_sources: 'ValidatedSources', node_executor_registry: Optional['NodeExecutorRegistry'] = None
) -> ExecutionGraph:
    """Given a validated sources collection, compiles it into an execution graph that can then be used by
    the executor."""
    from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
    from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes

    from .node_executor_registry import NodeExecutorRegistry

    node_executor_registry = node_executor_registry or NodeExecutorRegistry.get_instance()
    try:
        sorted_sources: Sequence[Source] = validated_sources.get_validator_result(
            ImportsMustNotHaveCycles
        ).sorted_sources
    except KeyError:
        sorted_sources = []
    try:
        nodes_to_unwrap: Set[int] = validated_sources.get_validator_result(ValidateStaticTypes).nodes_to_unwrap
    except KeyError:
        nodes_to_unwrap = set()

    instance = ExecutionGraph(
        node_executor_registry=node_executor_registry, sources=validated_sources, nodes_to_unwrap=nodes_to_unwrap
    )
    ordered_sources: List[Source] = list(chain_dedupe(iter(sorted_sources), iter(validated_sources.sources)))

    for source in ordered_sources:
        # noinspection PyProtectedMember
        instance._add_validated_source(source)
        maybe_periodic_yield()

    for source in ordered_sources:
        sorted_dependency_chain = _topologically_sort_dependency_chain_for(instance, source)
        # noinspection PyProtectedMember
        instance._add_sorted_dependency_chain(source, sorted_dependency_chain)
        maybe_periodic_yield()

    return instance


def chain_dedupe(a: Iterator[T], b: Iterator[T]) -> Iterator[T]:
    seen: Set[T] = set()

    for iterator in (a, b):
        for item in iterator:
            if item in seen:
                continue

            seen.add(item)
            yield item


def _topologically_sort_dependency_chain_for(
    execution_graph: ExecutionGraph, source: Source
) -> Sequence[DependencyChain]:
    sorted_chain = []
    visited: Set[DependencyChain] = set()

    def do_topological_sort(chain: DependencyChain) -> None:
        for dependency in chain.dependent_on:
            do_topological_sort(dependency)

        if chain in visited:
            return
        visited.add(chain)

        sorted_chain.append(chain)

    for statement in source.ast_root.statements:
        do_topological_sort(execution_graph.get_dependency_chain(statement))

    return sorted_chain
