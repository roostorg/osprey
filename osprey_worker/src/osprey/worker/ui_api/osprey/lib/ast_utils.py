"""Shared engine-AST helpers for the ui_api.

Used by `views/features.py` and by the `lib/rule*.py` modules. Nothing here
touches Flask, the config, or the rules table — these are pure functions over
engine AST nodes.

Deliberately thin. Anything the engine already answers is not duplicated here:

- rendering an expression back to source -> `osprey.engine.ast.printer.print_ast`
- generic node traversal                 -> `osprey.engine.ast.ast_utils.filter_nodes`
- rule name -> description               -> `OspreyEngine.get_rule_to_info_mapping()`
- feature name -> entity type            -> `OspreyEngine.get_feature_name_to_entity_type_mapping()`
"""

from typing import Any

from osprey.engine.ast.ast_utils import iter_field_values
from osprey.engine.ast.grammar import ASTNode, Attribute, Call, Name


def get_func_identifier(call: Call) -> str | None:
    """The name of the function a Call invokes, or None if it isn't a plain name.

    `Call.func` is a closed `Name | Attribute` union (the parser rejects every
    other callee form), but the AST validators refuse attribute callees outright
    -- `validate_call_kwargs` reports "calling attributes isn't supported yet".
    So for anything the engine has validated this always returns a name.

    Attribute callees only reach here via the rule builder, which parses raw
    unvalidated editor SML. Returning None there is deliberate: callers compare
    the result against bare names ('Rule', 'WhenRules', 'TextContains'), and
    reporting `Foo.Bar(...)` as 'Bar' would let a namespaced call impersonate a
    top-level one. Note the engine reads the other half of that expression --
    `Attribute.identifier` returns the base ('Foo'), not the attribute.
    """
    if isinstance(call.func, Name):
        return call.func.identifier
    if isinstance(call.func, Attribute):
        return None
    return None


def collect_name_references(node: Any) -> set[str]:
    """Every `Name` identifier referenced by an expression.

    The function position of a `Call` is skipped: `JsonData` in `JsonData(...)`
    names a UDF, not a feature. Everything else is walked generically via the
    engine's field iterator, so new grammar nodes are covered without a change
    here.
    """
    out: set[str] = set()
    _collect_name_references(node, out)
    return out


def _collect_name_references(node: Any, out: set[str]) -> None:
    if not isinstance(node, ASTNode):
        return
    if isinstance(node, Name):
        out.add(node.identifier)
        return

    for field_name, value in iter_field_values(node):
        if field_name == 'func' and isinstance(node, Call):
            continue
        if isinstance(value, ASTNode):
            _collect_name_references(value, out)
        elif isinstance(value, (list, tuple)):
            for item in value:
                _collect_name_references(item, out)
