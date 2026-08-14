from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.grammar import Call, Source


def _parse_call(contents: str) -> Call:
    source = Source(path='test.sml', contents=contents)
    return next(iter(filter_nodes(source.ast_root, Call)))


def test_call_argument_dict_reuses_the_same_mapping() -> None:
    call = _parse_call('Result = Function(a=1, b=2)\n')

    assert call.argument_dict() is call.argument_dict()
