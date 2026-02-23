from collections import defaultdict
from typing import TYPE_CHECKING

from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.grammar import Name, Span, Store

from ..base_validator import BaseValidator, HasResult

if TYPE_CHECKING:
    from ..validation_context import ValidationContext


IdentifierIndex = dict[str, Span]


class UniqueStoredNames(BaseValidator, HasResult[IdentifierIndex]):
    """
    Validates that stored names across all source files are unique.
    """

    def __init__(self, context: 'ValidationContext'):
        super().__init__(context)
        self.identifier_index: IdentifierIndex = {}

    def run(self) -> None:
        stored_global_names: defaultdict[str, list[Span]] = defaultdict(list)
        stored_local_names_by_file: defaultdict[str, defaultdict[str, list[Span]]] = defaultdict(
            lambda: defaultdict(list)
        )
        # Iterate over the ast, finding name nodes that are using the Store context.
        for source in self.context.sources:
            for name_node in filter_nodes(source.ast_root, Name, filter_fn=lambda n: isinstance(n.context, Store)):
                if name_node.is_local:
                    stored_local_names_by_file[name_node.span.source.path][name_node.identifier].append(name_node.span)
                else:
                    stored_global_names[name_node.identifier].append(name_node.span)

        # Now loop over all the stored names and find duplicates.
        for identifier, spans in stored_global_names.items():
            [span, *additional_spans] = spans

            if not additional_spans:
                self.identifier_index[identifier] = span
                continue

            self.context.add_error(
                message='features must be unique across all rule files',
                hint='this feature is defined in multiple locations',
                span=span,
                additional_spans_message='such as:',
                additional_spans=additional_spans,
            )

        for stored_local_names in stored_local_names_by_file.values():
            for identifier, spans in stored_local_names.items():
                [span, *additional_spans] = spans

                if not additional_spans:
                    continue

                self.context.add_error(
                    message='local variables must be unique in their file',
                    hint='this local variable is defined in multiple locations in this file',
                    span=span,
                    additional_spans_message='such as:',
                    additional_spans=additional_spans,
                )

    def get_result(self) -> IdentifierIndex:
        return self.identifier_index
