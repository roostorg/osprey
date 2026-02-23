from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import cast

from osprey.engine.ast.ast_utils import filter_nodes, iter_nodes
from osprey.engine.ast.grammar import Assign, ASTNode, Call, Load, Name, Source, Store
from osprey.engine.stdlib.udfs.import_ import Import
from osprey.engine.utils.get_closest_string_within_threshold import get_closest_string_within_threshold

from ..base_validator import BaseValidator, HasInput, HasResult
from .imports_must_not_have_cycles import ImportsMustNotHaveCycles
from .unique_stored_names import UniqueStoredNames
from .validate_call_kwargs import ValidateCallKwargs


class VariablesMustBeDefined(BaseValidator, HasInput[set[str]], HasResult[Mapping[Source, set[str]]]):
    """
    Checks that values are defined before being used.
    """

    def run(self) -> None:
        # We need to ensure that call kwargs are validated before we run this.
        self.context.validator_depends_on(validator_classes=[ValidateCallKwargs, ImportsMustNotHaveCycles])
        # Allow the validator to insert a set of known-available names.
        global_names: set[str] = self.context.get_validator_input(type(self), set())
        identifier_index = self.context.get_validator_result(UniqueStoredNames)
        identifiers_by_source: defaultdict[Source, set[str]] = defaultdict(set)

        for identifier, span in identifier_index.items():
            identifiers_by_source[span.source].add(identifier)

        self.known_identifiers_by_source: Mapping[Source, set[str]] = {}

        for source in self.context.sources:
            known_identifiers: set[str] = set(global_names)
            for node in iter_nodes(source.ast_root):
                if is_import(node):
                    imported_sources = self.get_imported_sources(cast(Call, node))
                    for imported_source in imported_sources:
                        known_identifiers |= identifiers_by_source[imported_source]

                elif isinstance(node, Name):
                    if isinstance(node.context, Store):
                        known_identifiers.add(node.identifier)

                    elif isinstance(node.context, Load) and not is_function_call(node):
                        # So, we didn't find the identifier, perhaps it exists elsewhere? Since
                        # identifiers are unique across all rules, we can try and search for it,
                        # using the index we built with `UniqueStoredNames`, and suggest a
                        # useful error message, perhaps.
                        if node.identifier not in known_identifiers:
                            # Identifier was found, but wasn't imported.
                            if node.identifier in identifier_index:
                                found_span = identifier_index[node.identifier]
                                self.context.add_error(
                                    message='unknown identifier',
                                    span=node.span,
                                    hint='this identifier was not imported into this file',
                                    additional_spans_message='however, it was found here:',
                                    additional_spans=[found_span],
                                )
                                continue

                            # Try to locate a similarly named string, looking first within the identifiers
                            # that exist within the scope, and then within the global scope.
                            closest_name = get_closest_string_within_threshold(
                                string=node.identifier, candidate_strings=known_identifiers
                            ) or get_closest_string_within_threshold(
                                string=node.identifier, candidate_strings=identifier_index.keys()
                            )
                            if closest_name:
                                maybe_found_span = identifier_index.get(closest_name)
                                if maybe_found_span is None:
                                    # This can happen when checking query sources
                                    self.context.add_error(
                                        message='unknown identifier',
                                        span=node.span,
                                        hint=f'unknown name `{node.identifier}`, did you mean `{closest_name}`?',
                                    )
                                else:
                                    if closest_name in known_identifiers:
                                        hint = 'however, a similar in-scope identifier was found here:'
                                    else:
                                        hint = 'however, a similar identifier was found here:'
                                    self.context.add_error(
                                        message='unknown identifier',
                                        span=node.span,
                                        hint='this identifier not found',
                                        additional_spans_message=hint,
                                        additional_spans=[maybe_found_span],
                                    )
                                continue

                            if node.is_local:
                                self.context.add_error(
                                    message='unknown local variable',
                                    span=node.span,
                                    hint=(
                                        f'`{node.identifier}` does not exist in this file\n'
                                        "variables that start with `_` can only be used in the file they're declared in"
                                    ),
                                )
                            else:
                                self.context.add_error(
                                    message='unknown identifier',
                                    span=node.span,
                                    hint=f'`{node.identifier}` does not exist in any rule files. typo, perhaps?',
                                )

                elif isinstance(node, Assign):
                    # This checks against saying `Foo = Foo`.
                    target_identifier = node.target.identifier
                    for name_node in filter_nodes(
                        node.value,
                        Name,
                        filter_fn=lambda n: isinstance(n.context, Load) and n.identifier == target_identifier,
                    ):
                        self.context.add_error(
                            message='cannot both store and load the same value within an assignment',
                            hint='you cannot use the identifier you are assigning to in its assignment',
                            span=name_node.span,
                            additional_spans_message='assignment happens here:',
                            additional_spans=[node.target.span],
                        )

            self.known_identifiers_by_source[source] = known_identifiers

    def get_result(self) -> Mapping[Source, set[str]]:
        if not hasattr(self, 'known_identifiers_by_source'):
            raise RuntimeError('Validator must be run before `get_result` is called')
        return self.known_identifiers_by_source

    def get_imported_sources(self, node: Call) -> Sequence[Source]:
        udf_mapping = self.context.get_validator_result(ValidateCallKwargs)
        call_udf, _ = udf_mapping[id(node)]
        assert isinstance(call_udf, Import)
        return [s for s, _ in call_udf.sources_and_spans]


def is_import(node: ASTNode) -> bool:
    return isinstance(node, Call) and isinstance(node.func, Name) and node.func.identifier == 'Import'


def is_function_call(node: Name) -> bool:
    """Checks to see if a name node is the target of a function call."""
    return isinstance(node.parent, Call) and node.parent.func == node
