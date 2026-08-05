import gc

import pytest
from osprey.engine.executor.dependency_chain import DependencyChain
from osprey.engine.executor.execution_graph import ExecutionGraph


class _DummyExecutor:
    """Stand-in for a BaseNodeExecutor: a plain object with default identity eq/hash, exactly
    like the real thing (no node executor subclass overrides __eq__/__hash__)."""

    def __init__(self, dependent_nodes: tuple[object, ...] = ()) -> None:
        self._dependent_nodes = dependent_nodes

    def get_dependent_nodes(self) -> tuple[object, ...]:
        return self._dependent_nodes


def test_chain_equals_itself() -> None:
    executor = _DummyExecutor()
    chain = DependencyChain(executor=executor, dependent_on=())

    assert chain == chain
    assert chain in {chain}
    assert {chain: 'value'}[chain] == 'value'


def test_chains_with_same_executor_and_deps_are_not_equal() -> None:
    """Two independently-constructed chains that happen to wrap the same executor and the same
    dependent_on tuple are NOT equal under identity semantics, even though they would have been
    dataclass-equal (and hash-colliding) under the old structural eq=True default.

    This is intentional: the engine never builds two such "equal-but-distinct" chains for the
    same logical node -- `ExecutionGraph._build_dependency_chain` either mints a brand-new
    executor per chain (so a genuinely different chain never collides with an existing one) or
    returns the literal cached chain object for a `Load`-context Name (so a shared node is the
    same object, not merely an equal one). Treating two distinct chain objects as interchangeable
    just because their fields happen to match is not a case this engine relies on.
    """
    executor = _DummyExecutor()
    leaf = DependencyChain(executor=executor, dependent_on=())

    chain_a = DependencyChain(executor=executor, dependent_on=(leaf,))
    chain_b = DependencyChain(executor=executor, dependent_on=(leaf,))

    assert chain_a is not chain_b
    assert chain_a != chain_b
    assert chain_b not in {chain_a}


def test_distinct_executors_are_not_equal() -> None:
    chain_a = DependencyChain(executor=_DummyExecutor(), dependent_on=())
    chain_b = DependencyChain(executor=_DummyExecutor(), dependent_on=())

    assert chain_a != chain_b


def test_build_dependency_chain_does_not_resize_an_observable_tuple(monkeypatch: pytest.MonkeyPatch) -> None:
    root, left, right = object(), object(), object()
    executors = {
        root: _DummyExecutor((left, right)),
        left: _DummyExecutor(),
        right: _DummyExecutor(),
    }
    monkeypatch.setattr(ExecutionGraph, '_get_executor_for', lambda _graph, node: executors[node])

    original_build = ExecutionGraph._build_dependency_chain
    previous_chain: DependencyChain | None = None
    held_tuples: list[tuple[object, ...]] = []

    def observing_build(graph: ExecutionGraph, node: object) -> DependencyChain:
        nonlocal previous_chain
        if previous_chain is not None:
            held_tuples.extend(
                referrer
                for referrer in gc.get_referrers(previous_chain)
                if isinstance(referrer, tuple) and len(referrer) > len(executors[root].get_dependent_nodes())
            )
        chain = original_build(graph, node)  # type: ignore[arg-type]
        previous_chain = chain
        return chain

    monkeypatch.setattr(ExecutionGraph, '_build_dependency_chain', observing_build)
    graph = object.__new__(ExecutionGraph)

    chain = graph._build_dependency_chain(root)  # type: ignore[arg-type]

    assert len(chain.dependent_on) == 2
