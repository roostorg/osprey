import copy
from typing import Any, Callable, Iterator, Optional, Sequence, Set, Tuple, Type, TypeVar, Union

# from osprey.engine.utils.periodic_execution_yielder import maybe_periodic_yield
from .grammar import ASTNode, Root, Statement

T = TypeVar('T', bound='ASTNode')


def iter_fields(node: 'ASTNode') -> Iterator[str]:
    """Figure out the fields to iterate over from a given class, given it's
    annotations to define ordering.

    For examples, see tests/ast_utils/test_iter_fields.py
    """
    # As of c-python 3.6, dict is guaranteed to be insertion-ordered. We can use this,
    # to make a simple ordered set.
    ordered_mro = {}

    def traverse_mro(klass: Any) -> None:
        if hasattr(klass, '__mro__'):
            for it in klass.__mro__[1:-1]:
                traverse_mro(it)

        ordered_mro[klass] = True

    traverse_mro(node.__class__)

    seen_fields: Set[str] = set()

    for klass in ordered_mro:
        for field in getattr(klass, '__annotations__', ()):
            # Do not iterate fields that were already iterated - and ignore `span/parent` fields,
            # as they are generally irrelevant for field iteration.
            if field not in ('span', 'parent', 'can_extract', 'is_constant') and field not in seen_fields:
                yield field

            seen_fields.add(field)


def _make_memoized_field_values_iterator() -> Callable[
    ['ASTNode'], Iterator[Tuple[str, Union['ASTNode', Sequence['ASTNode']]]]
]:
    _field_cache: dict[Type['ASTNode'], list[str]] = {}

    def _iter_field_values(node: ASTNode) -> Iterator[Tuple[str, Union['ASTNode', Sequence['ASTNode']]]]:
        # To avoid the cost of iterating fields over known node classes,
        # perform simple memoization.

        klass = node.__class__
        if klass not in _field_cache:
            _field_cache[klass] = list(iter_fields(node))

        fields = _field_cache[klass]
        for field in fields:
            if not hasattr(node, field):
                continue

            yield field, getattr(node, field)

    return _iter_field_values


iter_field_values = _make_memoized_field_values_iterator()
iter_field_values.__doc__ = (
    """Iterate over the fields within an AST node, yielding tuples of (field name, field value)"""
)


def iter_nodes(root: 'ASTNode') -> Iterator['ASTNode']:
    """Given a root node, iterate over all the nodes within that root node in
    order of appearance."""

    def _iter_inner(node: 'ASTNode') -> Iterator['ASTNode']:
        yield node
        for _field_name, field_value in iter_field_values(node):
            # TODO: maybe uncomment this when we have a periodic execution yielder?
            # NOTE(ayubun): this decision was a pretty old one. its probably for gevent.
            # im not sure if we need it still:
            # maybe_periodic_yield()
            if isinstance(field_value, ASTNode):
                yield from _iter_inner(field_value)
            elif isinstance(field_value, list):
                for item in field_value:
                    yield from _iter_inner(item)

    return _iter_inner(root)


def filter_nodes(root: 'ASTNode', ty: Type[T], filter_fn: Optional[Callable[[T], bool]] = None) -> Iterator[T]:
    """Given a root, iterate over nodes, filtering out those who's type do not
    match the given `ty`.

    Additional filtering can be provided by a filter_fn, which will take
    the node, and return a bool.
    """
    for node in iter_nodes(root):
        if isinstance(node, ty):
            if filter_fn is None or filter_fn(node):
                yield node


def replace_from_root(source: 'ASTNode', replacement: 'ASTNode') -> Statement:
    """Given a source node, traverse to the root statement, and replace all instances of `source` with `replacement`,
    returning the root statement. This is useful if you want to perform some AST mutation to provide as a suggestion
    to the end-user."""
    # Find the root statement:
    statement: ASTNode = source
    while not isinstance(statement.parent, Root) and statement.parent:
        statement = statement.parent

    assert isinstance(statement, Statement), 'Could not find statement.'
    statement = copy.deepcopy(statement)

    def is_source(node: Any) -> bool:
        # Since we are doing a deep copy to avoid the AST mutation, we break
        # referential equality (`a is b`), and instead, we need to compare if the nodes
        # as well as their spans are equal.
        return isinstance(node, ASTNode) and node.span == source.span and node == source

    # Replace the node with the replacement.
    def _replace_inner(root: 'ASTNode') -> None:
        for field_name, field_value in iter_field_values(root):
            if is_source(field_value):
                setattr(root, field_name, replacement)
            elif isinstance(field_value, ASTNode):
                _replace_inner(field_value)

            elif isinstance(field_value, list):
                for i, item in enumerate(field_value):
                    if is_source(item):
                        field_value[i] = replacement
                    else:
                        _replace_inner(item)

    _replace_inner(statement)
    return statement
