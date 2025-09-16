"""
Copy of graphlib python library with some changes to let us modify the graph after
calling `prepare()`. This semantic change is necessary because the execution of certain
Osprey nodes (e.g. Import and Require) cause new nodes to get added to the ExecutionContext
after execution has already started.

List of notable changes:
  - add() can be called after prepare, but you have to re-prepare() the graph after
  - add() can't be called on same node more than once. This is okay for the
    Osprey execution use-case because a given node (expression, operation, call, etc.) is only
    defined once. This protects us from adding dependencies after a node has already started
    execution.
  - prepare() is a no-op if run multiple times with no add() call in between
  - the find_cycle() is removed. The Osprey execution graph is already validated to be acyclic,
    so we optimize the prepare() step by skipping the cycle check.
"""

from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

_NODE_OUT = -1
_NODE_DONE = -2


class _NodeInfo:
    __slots__ = 'node', 'npredecessors', 'successors'

    def __init__(self, node: Any):
        # The node this class is augmenting.
        self.node = node

        # Number of predecessors, generally >= 0. When this value falls to 0,
        # and is returned by get_ready(), this is set to _NODE_OUT and when the
        # node is marked done by a call to done(), set to _NODE_DONE.
        self.npredecessors = 0

        # List of successor nodes. The list can contain duplicated elements as
        # long as they're all reflected in the successor's npredecessors attribute).
        self.successors: List[Any] = []


class CycleError(ValueError):
    """Subclass of ValueError raised by TopologicalSorterif cycles exist in the graph

    If multiple cycles exist, only one undefined choice among them will be reported
    and included in the exception. The detected cycle can be accessed via the second
    element in the *args* attribute of the exception instance and consists in a list
    of nodes, such that each node is, in the graph, an immediate predecessor of the
    next node in the list. In the reported list, the first and the last node will be
    the same, to make it clear that it is cyclic.
    """

    pass


class TopologicalSorter:
    """Provides functionality to topologically sort a graph of hashable nodes"""

    def __init__(self, graph: Optional[Dict[Any, Iterable[Any]]] = None):
        self._node2info: Dict[Any, _NodeInfo] = {}
        self._ready_nodes: List[Any] = []
        self._npassedout = 0
        self._nfinished = 0
        self._needs_prepare = True

        if graph is not None:
            for node, predecessors in graph.items():
                self.add(node, *predecessors)

    def _get_nodeinfo(self, node: Any) -> _NodeInfo:
        result = self._node2info.get(node)
        if result is None:
            self._node2info[node] = result = _NodeInfo(node)
        return result

    def already_added(self, node: Any) -> bool:
        return node in self._node2info

    def add(self, node: Any, *predecessors: Any) -> None:
        """Add a new node and its predecessors to the graph.

        Both the *node* and all elements in *predecessors* must be hashable.

        It is possible to add a node with no dependencies (*predecessors* is not provided)
        as well as provide a dependency twice. If a node that has not been provided before
        is included among *predecessors* it will be automatically added to the graph with
        no predecessors of its own.

        Raises ValueError if called on a node that already exists.
        """

        if self.already_added(node):
            raise ValueError('Cannot add the same node more than once')

        nodeinfo = self._get_nodeinfo(node)

        num_new_predecessors = 0
        # Create the predecessor -> node edges
        for pred in predecessors:
            pred_info = self._get_nodeinfo(pred)

            # We're adding a new node that depends on an already completed predecessor node,
            # so we should not include it in the outstanding predecessor count.
            if pred_info.npredecessors == _NODE_DONE:
                continue
            pred_info.successors.append(node)
            num_new_predecessors += 1

        nodeinfo.npredecessors += num_new_predecessors

        self._needs_prepare = True

    def prepare(self) -> None:
        """Mark the graph as finished and recalculate ready nodes"""
        if not self._needs_prepare:
            return

        self._ready_nodes = [i.node for i in self._node2info.values() if i.npredecessors == 0]
        self._needs_prepare = False

    def get_ready(self) -> Tuple[Any, ...]:
        """Return a tuple of all the nodes that are ready.

        Initially it returns all nodes with no predecessors; once those are marked
        as processed by calling "done", further calls will return all new nodes that
        have all their predecessors already processed. Once no more progress can be made,
        empty tuples are returned.

        Raises ValueError if called without calling "prepare" previously.
        """
        if self._needs_prepare:
            raise ValueError('prepare() must be called first')

        # Get the nodes that are ready and mark them
        result = tuple(self._ready_nodes)
        n2i = self._node2info
        for node in result:
            n2i[node].npredecessors = _NODE_OUT

        # Clean the list of nodes that are ready and update
        # the counter of nodes that we have returned.
        self._ready_nodes.clear()
        self._npassedout += len(result)

        return result

    def is_active(self) -> bool:
        """Return True if more progress can be made and ``False`` otherwise.

        Progress can be made if cycles do not block the resolution and either there
        are still nodes ready that haven't yet been returned by "get_ready" or the
        number of nodes marked "done" is less than the number that have been returned
        by "get_ready".

        Raises ValueError if called without calling "prepare" previously.
        """
        if self._needs_prepare:
            raise ValueError('prepare() must be called first')
        return self._nfinished < self._npassedout or bool(self._ready_nodes)

    def __bool__(self) -> bool:
        return self.is_active()

    def done(self, *nodes: Any) -> None:
        """Marks a set of nodes returned by "get_ready" as processed.

        This method unblocks any successor of each node in *nodes* for being returned
        in the future by a a call to "get_ready"

        Raises :exec:`ValueError` if any node in *nodes* has already been marked as
        processed by a previous call to this method, if a node was not added to the
        graph by using "add" or if called without calling "prepare" previously or if
        node has not yet been returned by "get_ready".
        """

        if self._needs_prepare:
            raise ValueError('prepare() must be called first')

        n2i = self._node2info

        for node in nodes:
            # Check if we know about this node (it was added previously using add()
            nodeinfo = n2i.get(node)
            if nodeinfo is None:
                raise ValueError(f'node {node!r} was not added using add()')

            # If the node has not being returned (marked as ready) previously, inform the user.
            stat = nodeinfo.npredecessors
            if stat != _NODE_OUT:
                if stat >= 0:
                    raise ValueError(f'node {node!r} was not passed out (still not ready)')
                elif stat == _NODE_DONE:
                    raise ValueError(f'node {node!r} was already marked done')
                else:
                    assert False, f'node {node!r}: unknown status {stat}'

            # Mark the node as processed
            nodeinfo.npredecessors = _NODE_DONE

            # Go to all the successors and reduce the number of predecessors, collecting all the ones
            # that are ready to be returned in the next get_ready() call.
            for successor in nodeinfo.successors:
                successor_info = n2i[successor]
                successor_info.npredecessors -= 1
                if successor_info.npredecessors == 0:
                    self._ready_nodes.append(successor)
            self._nfinished += 1

    def static_order(self) -> Iterator[Any]:
        """Returns an iterable of nodes in a topological order.

        The particular order that is returned may depend on the specific
        order in which the items were inserted in the graph.

        Using this method does not require to call "prepare" or "done". If any
        cycle is detected, :exc:`CycleError` will be raised.
        """
        self.prepare()
        while self.is_active():
            node_group = self.get_ready()
            yield from node_group
            self.done(*node_group)
