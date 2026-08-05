"""Static Import + Require source-closure index.

Two consumers need "which sources can this action possibly touch?":

  * :class:`~osprey.engine.ast_validator.validators.collect_json_data_paths.CollectJsonDataPaths`
    — to build the per-action json-path manifest.
  * :func:`osprey.engine.executor.graph_specializer.specialize_graph` — to scope
    typed-contract specialization to one action instead of the whole corpus.

Both want the same MAXIMAL static closure, so the walk lives here once. It follows
Import edges (statically exact) and Require edges (a runtime decision, so both
``require_if`` branches are walked and format-string rules are glob-expanded).

Per-source adjacency is resolved once at construction; a closure walk is then pure
graph traversal, which is what makes a per-action closure cheap enough to compute
for every action on every rules reload.
"""

from __future__ import annotations

import logging
from collections import deque
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Set, Tuple, Union

from osprey.engine.ast import grammar
from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.grammar import Call, Source

if TYPE_CHECKING:
    from osprey.engine.ast.sources import Sources
    from osprey.engine.ast_validator.validation_context import ValidatedSources
    from osprey.engine.ast_validator.validators.validate_call_kwargs import UDFNodeMapping
    from osprey.engine.utils.graph import Graph

log = logging.getLogger(__name__)

ACTION_SOURCE_DIR = 'actions'
"""Directory holding the one-file-per-action rule sources (``actions/<name>.sml``)."""

ACTION_SOURCE_SUFFIX = '.sml'

RequireEdge = Union[Source, str]
"""A resolved Require target: either a concrete Source (literal ``rule=``) or a glob
pattern (format-string ``rule=``) to be expanded against the Sources collection."""


def action_source_path(action_name: str) -> str:
    """The conventional source path holding ``action_name``'s rules."""
    return f'{ACTION_SOURCE_DIR}/{action_name}{ACTION_SOURCE_SUFFIX}'


def action_name_for_source(path: str) -> Optional[str]:
    """The action a source is the per-action file for, or None if it is not one."""
    pure = PurePosixPath(path)
    if len(pure.parts) != 2 or pure.parts[0] != ACTION_SOURCE_DIR or pure.suffix != ACTION_SOURCE_SUFFIX:
        return None
    return pure.stem


class SourceClosureIndex:
    """Precomputed per-source Import + Require adjacency for a Sources collection.

    Build once per validated-sources collection (see :func:`build_source_closure_index`)
    and call :meth:`reachable` per start set.
    """

    __slots__ = ('_sources', '_imports', '_requires', '_glob_cache')

    def __init__(
        self,
        sources: 'Sources',
        imports: Dict[str, Tuple[Source, ...]],
        requires: Dict[str, Tuple[RequireEdge, ...]],
    ) -> None:
        self._sources = sources
        self._imports = imports
        self._requires = requires
        self._glob_cache: Dict[str, Tuple[Source, ...]] = {}

    def _glob(self, pattern: str) -> Tuple[Source, ...]:
        cached = self._glob_cache.get(pattern)
        if cached is None:
            cached = tuple(self._sources.glob(pattern))
            self._glob_cache[pattern] = cached
        return cached

    def reachable(self, starts: Iterable[Source], action_name: Optional[str] = None) -> List[Source]:
        """BFS the Import + Require closure from ``starts``.

        With ``action_name=None`` this is the maximal static closure: every glob match of
        a format-string Require is followed, because any of them could be the runtime
        target.

        With ``action_name`` set, a glob match that is a *different* action's per-action
        source (``actions/<other>.sml``) is skipped — the corpus dispatches actions via
        ``Require(rule=f'actions/{ActionName}.sml')``, so at runtime exactly one of those
        matches is required and it is this action's. Every non-action glob match is still
        followed, and a literal Require is never narrowed.

        Narrowing can only ever UNDER-approximate (never over-approximate) the runtime
        source set, which is the safe direction for the specializer: a source left out of
        the closure simply keeps full-graph semantics.
        """
        visited: Set[str] = set()
        queue: deque[Source] = deque(starts)
        result: List[Source] = []

        while queue:
            src = queue.popleft()
            if src.path in visited:
                continue
            visited.add(src.path)
            result.append(src)

            for neighbor in self._imports.get(src.path, ()):
                if neighbor.path not in visited:
                    queue.append(neighbor)

            for edge in self._requires.get(src.path, ()):
                if isinstance(edge, Source):
                    if edge.path not in visited:
                        queue.append(edge)
                    continue
                for matched in self._glob(edge):
                    if matched.path in visited:
                        continue
                    if action_name is not None:
                        matched_action = action_name_for_source(matched.path)
                        if matched_action is not None and matched_action != action_name:
                            continue
                    queue.append(matched)

        return result


def build_source_closure_index(
    sources: 'Sources',
    import_graph: 'Graph[Source]',
    udf_node_mapping: 'UDFNodeMapping',
) -> SourceClosureIndex:
    """Resolve every source's Import and Require edges into a reusable index."""
    from osprey.engine.stdlib.udfs.require import Require

    imports: Dict[str, Tuple[Source, ...]] = {}
    requires: Dict[str, Tuple[RequireEdge, ...]] = {}

    for source in sources:
        import_edges = tuple(import_graph.iter_edges(source))
        if import_edges:
            imports[source.path] = import_edges

        require_edges: List[RequireEdge] = []
        for call_node in filter_nodes(source.ast_root, Call):
            entry = udf_node_mapping.get(id(call_node))
            if entry is None:
                continue
            udf, _ = entry
            if not isinstance(udf, Require):
                continue

            keyword = call_node.find_argument('rule')
            if keyword is None:
                continue
            rule_ast_node = keyword.value

            if isinstance(rule_ast_node, grammar.String):
                target = sources.get_by_path(rule_ast_node.value)
                if target is not None:
                    require_edges.append(target)
            elif isinstance(rule_ast_node, grammar.FormatString):
                # Same f-string -> glob conversion the Require UDF validates with.
                names_as_wildcards = {name.identifier: '*' for name in rule_ast_node.names}
                require_edges.append(rule_ast_node.format_string.format(**names_as_wildcards))

        if require_edges:
            requires[source.path] = tuple(require_edges)

    return SourceClosureIndex(sources=sources, imports=imports, requires=requires)


def build_source_closure_index_for(validated_sources: 'ValidatedSources') -> Optional[SourceClosureIndex]:
    """Build an index from a ValidatedSources, or None if a validator it needs
    (``ValidateCallKwargs`` / ``ImportsMustNotHaveCycles``) did not run.

    Returning None rather than raising lets callers fall back to whole-corpus behavior on
    a stripped validator registry instead of failing compilation.
    """
    from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
    from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs

    try:
        udf_node_mapping = validated_sources.get_validator_result(ValidateCallKwargs)
        import_graph = validated_sources.get_validator_result(ImportsMustNotHaveCycles).import_graph
    except KeyError:
        log.debug('source closure index unavailable: required validator results missing')
        return None
    return build_source_closure_index(validated_sources.sources, import_graph, udf_node_mapping)
