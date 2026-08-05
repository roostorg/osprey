"""Tests for grammar.py node behaviors that aren't covered by validator-level tests."""

from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.grammar import Call, Source


def _parse_call(contents: str) -> Call:
    source = Source(path='<test>', contents=contents)
    return next(iter(filter_nodes(source.ast_root, Call)))


class TestCallArgumentDict:
    def test_returns_same_object_across_repeated_calls(self) -> None:
        """argument_dict() is invoked at least once per Call-node evaluation per message
        (via ArgumentsBase.__init__); it must be memoized rather than rebuilt each time."""
        call = _parse_call('Foo = Bar(a=1, b=2)\n')

        assert call.argument_dict() is call.argument_dict()

    def test_not_recomputed_after_underlying_arguments_change(self) -> None:
        """Prove the cached value isn't rebuilt: mutate the `arguments` it derives from after
        the first access and confirm the cached dict doesn't reflect the mutation."""
        call = _parse_call('Foo = Bar(a=1, b=2)\n')

        first = call.argument_dict()
        assert set(first) == {'a', 'b'}

        call.arguments = ()  # would change the result if argument_dict() were recomputed
        assert call.argument_dict() is first
        assert set(call.argument_dict()) == {'a', 'b'}

    def test_content_is_correct(self) -> None:
        call = _parse_call('Foo = Bar(a=1, b=2)\n')

        argument_dict = call.argument_dict()

        assert set(argument_dict) == {'a', 'b'}
        assert argument_dict['a'].value == 1  # type: ignore[attr-defined]
        assert argument_dict['b'].value == 2  # type: ignore[attr-defined]
