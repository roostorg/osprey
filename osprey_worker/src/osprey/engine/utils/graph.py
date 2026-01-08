from collections import defaultdict, deque
from typing import DefaultDict, Deque, Generic, Hashable, Iterator, Sequence, Set, TypeVar

T = TypeVar('T', bound=Hashable)


class Graph(Generic[T]):
    def __init__(self) -> None:
        self._graph: DefaultDict[T, Set[T]] = defaultdict(set)

    def add_node(self, node: T) -> bool:
        return self._graph[node] is not None

    def add_edge(self, node: T, edge: T) -> None:
        self.add_node(edge)
        self._graph[node].add(edge)

    def iter_edges(self, node: T) -> Iterator[T]:
        if node not in self._graph:
            return iter([])

        return iter(self._graph[node])

    def iter_nodes(self) -> Iterator[T]:
        return iter(self._graph.keys())

    def topological_sort(self) -> Sequence[T]:
        """Returns a flat sorted array of the graph in topological order. Throws a CyclicDependencyError if the
        graph is not a DAG."""
        # A path set - for O(1) lookups of nodes within the path.
        path: Set[T] = set()
        # A path list - to allow us to preserve path ordering for error reporting.
        sorted_path: list[T] = []
        # A set of node we've visited - to assure this sort is O(N + E) where N = Nodes, E = Edges
        visited: Set[T] = set()
        # The output of sorted nodes.
        sorted_nodes: list[T] = []

        def visit(node: T) -> None:
            # Node has already been visited, so we don't need to do anything.
            if node in visited:
                return

            visited.add(node)
            path.add(node)
            sorted_path.append(node)
            for edge in self.iter_edges(node):
                if edge in path:
                    raise CyclicDependencyError(path=sorted_path)

                visit(edge)

            assert sorted_path.pop() is node
            path.remove(node)
            sorted_nodes.append(node)

        for it in self.iter_nodes():
            visit(it)

        return sorted_nodes

    def bfs(self, start: T, end: T) -> list[T]:
        prev_ptrs: dict[T, T] = {}

        to_visit: Deque[T] = deque()
        to_visit.append(start)

        while to_visit:
            node = to_visit.popleft()
            if node == end:
                break
            for neighbor in self.iter_edges(node):
                prev_ptrs[neighbor] = node
                to_visit.append(neighbor)

        def construct_path() -> list[T]:
            path_reversed = []
            curr_node = end
            while curr_node != start:
                path_reversed.append(curr_node)
                curr_node = prev_ptrs[curr_node]
            path_reversed.append(curr_node)
            return path_reversed[::-1]

        return construct_path()


class CyclicDependencyError(Generic[T], Exception):
    def __init__(self, path: Sequence[T]):
        cycle_string = ' -> '.join(repr(n) for n in path)
        message = f'Cyclic Dependency Detected: [{cycle_string}]'
        super().__init__(message)
        self.path = path
