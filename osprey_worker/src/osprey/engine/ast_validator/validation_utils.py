from osprey.engine.ast import ast_utils, grammar, printer

from .validation_context import ValidationContext


def add_must_assign_to_variable_error(
    context: ValidationContext,
    message: str,
    node: grammar.ASTNode,
    example_variable_name: str = 'SomeVariable',
    type_annotation: str | None = None,
) -> None:
    parent = node.parent
    type_annotation_str = '' if type_annotation is None else f': {type_annotation}'
    hint_lines = [
        'assign it to a variable before using it:',
        f'`{example_variable_name}{type_annotation_str} = {printer.print_ast(node)}`',
    ]
    if not isinstance(parent, grammar.Root):
        # If this isn't a top-level call, show how it would be used.
        suggested_ast = ast_utils.replace_from_root(
            node,
            # This span isn't quite right, but it won't be used anyway
            grammar.Name(identifier=example_variable_name, context=grammar.Load(), span=node.span),
        )
        hint_lines.append(f'`{printer.print_ast(suggested_ast)}`')

    context.add_error(message=message, span=node.span, hint='\n'.join(hint_lines))
