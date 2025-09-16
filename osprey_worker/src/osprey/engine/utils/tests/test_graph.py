import pytest

from ..graph import CyclicDependencyError, Graph


def test_graph_ops() -> None:
    g: Graph[str] = Graph()

    g.add_edge('a', 'b')
    g.add_edge('b', 'c')

    assert list(sorted(g.iter_nodes())) == ['a', 'b', 'c']
    assert list(sorted(g.iter_edges('a'))) == ['b']
    assert list(sorted(g.iter_edges('b'))) == ['c']
    assert list(sorted(g.iter_edges('c'))) == []


def test_topo_sort() -> None:
    g: Graph[str] = Graph()

    g.add_edge('a', 'b')
    g.add_edge('b', 'c')

    assert g.topological_sort() == ['c', 'b', 'a']


def test_topo_sort_cycle() -> None:
    g: Graph[str] = Graph()

    g.add_edge('a', 'b')
    g.add_edge('b', 'c')
    g.add_edge('c', 'a')

    with pytest.raises(CyclicDependencyError) as e:
        g.topological_sort()

    assert e.value.path == ['b', 'c', 'a']
    assert e.value.args[0] == "Cyclic Dependency Detected: ['b' -> 'c' -> 'a']"
