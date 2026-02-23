from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.grammar import Call, Source, Span
from osprey.engine.utils.graph import CyclicDependencyError, Graph

from ..base_validator import BaseValidator, HasResult
from .validate_call_kwargs import UDFNodeMapping, ValidateCallKwargs

if TYPE_CHECKING:
    from ..validation_context import ValidationContext


@dataclass
class ImportGraphResult:
    sorted_sources: Sequence[Source]
    import_graph: Graph[Source]


class ImportsMustNotHaveCycles(BaseValidator, HasResult[ImportGraphResult]):
    """
    Validates that imports do not have cyclic dependency. The result of this validator is the
    topologically sorted source list, which can be used by the execution graph compiler to
    iterate over the sources to build cross-source dependency chains in a single pass, as well
    as the import graph itself, which is used by validators that need to know about the import
    relationships between source files.
    """

    _graph: Graph[Source]
    """The dependency graph of sources that we will eventually try to resolve cycles on."""

    _udf_node_mapping: UDFNodeMapping
    """Cached result of  ValidateCallKwargs"""

    _pairs: dict[tuple[Source, Source], Span]
    """A mapping of XSource imported YSource -> Span, that will be used to build the error message if a cyclic
    dependency is found."""

    _sorted_imports: Sequence[Source]

    def __init__(self, context: 'ValidationContext'):
        super().__init__(context)
        self._udf_node_mapping = self.context.get_validator_result(ValidateCallKwargs)
        self._graph = Graph()
        self._sorted_imports = []
        self._pairs = {}

    def run(self) -> None:
        for source in self.context.sources:
            self.validate_source(source)

        try:
            self._sorted_imports = self._graph.topological_sort()
        except CyclicDependencyError as e:
            # We have our cycle path, and we're going to transform that into a coherent
            # error that can then be displayed to the end user.
            cycle_path = cast(list[Source], list(e.path))

            # In order to do that, we need to figure out all the spans we're going to point to
            # in the error message. This means that we need to map the cycle back to the spans
            # which the import happened. The mapping of "x imported y" -> span is maintained in `_pairs` and
            # is built as we're iterating over the sources.

            spans: list[Span] = []

            # So, we're going to loop over the cycle. Assuming we have the sources "foo", "bar" and "baz", and the
            # path is: foo -> bar -> baz, we want to get the spans that show where:
            #  a) foo -> bar
            #  b) bar -> baz
            #  c) baz -> foo

            # This loop gets us the spans for a & b.
            traversing_path = list(cycle_path)
            while len(traversing_path) >= 2:
                [node, edge] = traversing_path[:2]
                spans.append(self._pairs[node, edge])
                traversing_path.pop(0)

            # And this gets us the span for c.
            spans.append(self._pairs[cycle_path[-1], cycle_path[0]])

            # Now that we've got our spans, we can generate an error message. The first span,
            # is where the cycle started, and the rest of the spans is the path through the
            # sources.
            [span, *additional_spans] = spans
            self.context.add_error(
                message='import cycle detected here',
                span=span,
                hint='this import results in a cyclic dependency',
                additional_spans_message='the import chain is displayed below:',
                additional_spans=additional_spans,
            )

    def validate_source(self, source: Source) -> None:
        from osprey.engine.stdlib.udfs.import_ import Import

        for call_node in filter_nodes(source.ast_root, Call):
            udf, _ = self._udf_node_mapping[id(call_node)]
            if not isinstance(udf, Import):
                continue

            for edge, span in udf.sources_and_spans:
                self._graph.add_edge(source, edge)
                self._pairs[source, edge] = span

    def get_result(self) -> ImportGraphResult:
        return ImportGraphResult(self._sorted_imports, self._graph)
