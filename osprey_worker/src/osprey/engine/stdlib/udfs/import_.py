import typing
from collections import defaultdict

from osprey.engine.ast import ast_utils, grammar, printer

from ._prelude import ArgumentsBase, ConstExpr, ExecutionContext, Source, UDFBase, ValidationContext


class Arguments(ArgumentsBase):
    rules: ConstExpr[typing.List[str]]
    """A list of rule files to Include.

    Each string in this list must be a relative path to a *.sml file, and the list must be sorted lexicographically.
    """


class Import(UDFBase[Arguments, None]):
    """Includes a list of rule files in the execution path of the current event.

    The output of any Included rules are accessible in the file they are Included in.
    """

    def __init__(self, validation_context: 'ValidationContext', arguments: Arguments):
        super().__init__(validation_context, arguments)
        rules = arguments.rules
        assert isinstance(rules.literal, grammar.List)
        self.sources_and_spans: typing.List[typing.Tuple[Source, grammar.Span]] = []

        import_source_names: typing.List[str] = []
        import_spans_by_name: typing.DefaultDict[str, typing.List[grammar.Span]] = defaultdict(list)

        for node in rules.literal.items:
            assert isinstance(node, grammar.String)
            source = validation_context.sources.get_by_path(node.value)
            import_source_names.append(node.value)
            if not source:
                validation_context.add_error(
                    message=f'imported file: {node.value} not found', span=node.span, hint='import attempted here'
                )
            else:
                assert node.span is not None
                self.sources_and_spans.append((source, node.span))

            import_spans_by_name[node.value].append(node.span)

        for node_value, node_spans in import_spans_by_name.items():
            [first_span, *duped_spans] = node_spans
            if not duped_spans:
                continue

            validation_context.add_error(
                message=f'import has a duplicated items for rule: {node_value}',
                span=first_span,
                hint='first import attempted here',
                additional_spans=duped_spans,
                additional_spans_message=f'{len(duped_spans)} duplicate(s) found here',
            )

        sorted_import_source_names = sorted(import_source_names)
        if import_source_names != sorted_import_source_names:
            hint_lines = ['sort imports lexicographically:']
            suggested_ast = ast_utils.replace_from_root(
                source=arguments.rules.literal,
                replacement=grammar.List(
                    span=arguments.rules.literal.span,
                    items=[
                        grammar.String(value=source_name, span=arguments.rules.literal.span)
                        for source_name in sorted_import_source_names
                    ],
                ),
            )
            hint_lines.append(f'`{printer.print_ast(suggested_ast)}`')

            validation_context.add_error(
                message='import rules are not sorted',
                span=arguments.rules.literal.span,
                hint='\n'.join(hint_lines),
            )

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> None:
        for source, _span in self.sources_and_spans:
            execution_context.enqueue_source(source)
