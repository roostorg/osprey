from osprey.engine.ast import grammar

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase, ValidationContext
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    rule: str
    """The relative path to a *.sml rule file."""
    require_if: bool = True
    """Optional: Only include the specified rule if this conditional evaluates to `True`."""


class Require(UDFBase[Arguments, None]):
    """Requires a single rule file to be included in the execution path of the current event.

    The output of any Required files are not accessible in the file they are Required in."""

    category = UdfCategories.ENGINE

    def __init__(self, validation_context: 'ValidationContext', arguments: Arguments):
        super().__init__(validation_context, arguments)

        rule_ast_node = arguments.get_argument_ast('rule')
        if isinstance(rule_ast_node, grammar.String):
            source = validation_context.sources.get_by_path(rule_ast_node.value)
            if not source:
                validation_context.add_error(
                    message=f'required rule: `{rule_ast_node.value}` not found',
                    span=rule_ast_node.span,
                    hint='require attempted here',
                )
        elif isinstance(rule_ast_node, grammar.FormatString):
            # Let's look at the format string, for example `f'foo/{Bar}.sml` and convert it to
            # `foo/*.sml` and try to find sources.
            names_as_wildcards = {name.identifier: '*' for name in rule_ast_node.names}
            glob_path = rule_ast_node.format_string.format(**names_as_wildcards)
            matched_sources = validation_context.sources.glob(glob_path)
            if not matched_sources:
                validation_context.add_error(
                    message=f'required rule: `{rule_ast_node.format_string}` would never match any rule files',
                    span=rule_ast_node.span,
                    hint=f'require attempted here, glob: `{glob_path}` matches no sources',
                )
        else:
            validation_context.add_error(
                message='a literal string or format string is required here',
                span=rule_ast_node.span,
                hint=(
                    'you can only use literal strings, for example:\n'
                    '`Require(rule="hello.sml")` or `Require(rule=f"actions/{ActionName}.sml")`'
                ),
            )

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> None:
        source = execution_context.validated_sources.sources.get_by_path(arguments.rule)
        if source and arguments.require_if:
            execution_context.enqueue_source(source)
