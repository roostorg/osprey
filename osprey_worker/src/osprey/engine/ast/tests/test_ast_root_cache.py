from osprey.engine.ast import grammar
from osprey.engine.ast.grammar import Root, Source, _BoundedASTRootCache


def _source(name: str) -> Source:
    return Source(path=f'{name}.sml', contents=f'{name} = 1')


def test_bounded_cache_evicts_least_recently_used() -> None:
    a, b, c = _source('a'), _source('b'), _source('c')
    root_a, root_b, root_c = a.ast_root, b.ast_root, c.ast_root

    cache = _BoundedASTRootCache(max_size=2)
    cache.put(a, root_a)
    cache.put(b, root_b)
    assert len(cache) == 2

    # Access `a` so that `b` becomes the least-recently-used entry.
    assert cache.get(a) is root_a

    # Inserting a third entry must evict `b`, not grow past the bound.
    cache.put(c, root_c)
    assert len(cache) == 2
    assert cache.get(b) is None
    assert cache.get(a) is root_a
    assert cache.get(c) is root_c


def test_ast_root_is_cached_and_stable() -> None:
    source = _source('cache_stable_check')
    first = source.ast_root
    second = source.ast_root

    assert isinstance(first, Root)
    assert first is second
    assert source in grammar.parsed_ast_root_cache


def test_module_cache_is_bounded() -> None:
    # Regression: the process-global cache must be a bounded cache, not an unbounded dict.
    assert isinstance(grammar.parsed_ast_root_cache, _BoundedASTRootCache)
    assert grammar._AST_ROOT_CACHE_MAX_SIZE > 0
