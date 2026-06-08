"""Shared engine-AST traversal helpers used by views (features.py, rules.py, ...)."""

from typing import Any, Optional, Set

from osprey.engine.ast.grammar import (
    Attribute,
    BinaryComparison,
    BinaryOperation,
    BooleanOperation,
    Call,
    FormatString,
    Name,
    Number,
    String,
    UnaryOperation,
)
from osprey.engine.ast.grammar import (
    List as AstList,
)


def get_func_identifier(call: Call) -> Optional[str]:
    """Get the function name from a Call node."""
    if isinstance(call.func, Name):
        return call.func.identifier
    if isinstance(call.func, Attribute):
        return call.func.attribute
    return None


def ast_to_string(node: Any) -> str:
    """Convert an AST expression node to a human-readable string."""
    if isinstance(node, Name):
        return node.identifier
    if isinstance(node, String):
        return repr(node.value)
    if isinstance(node, Number):
        return str(node.value)
    if isinstance(node, BinaryComparison):
        left = ast_to_string(node.left)
        right = ast_to_string(node.right)
        return f'{left} {node.comparator.original_comparator} {right}'
    if isinstance(node, BinaryOperation):
        left = ast_to_string(node.left)
        right = ast_to_string(node.right)
        return f'{left} {node.operator.original_operator} {right}'
    if isinstance(node, UnaryOperation):
        operand = ast_to_string(node.operand)
        return f'{node.operator.original_operator} {operand}'
    if isinstance(node, BooleanOperation):
        parts = [ast_to_string(v) for v in node.values]
        return f' {node.operand.original_operand} '.join(parts)
    if isinstance(node, Call):
        func_name = get_func_identifier(node) or '?'
        parts = []
        for arg in node.arguments:
            parts.append(f'{arg.name}={ast_to_string(arg.value)}')
        return f'{func_name}({", ".join(parts)})'
    if isinstance(node, Attribute):
        return f'{node.name.identifier}.{node.attribute}'
    if isinstance(node, AstList):
        items = [ast_to_string(item) for item in node.items]
        return f'[{", ".join(items)}]'
    if isinstance(node, FormatString):
        return f"f'{node.format_string}'"
    if hasattr(node, 'value'):
        return str(node.value)
    return str(node)


def collect_name_references(node: Any, out: Set[str]) -> None:
    """Recursively collect all Name.identifier values referenced by an expression node.

    Skip the function-identifier position of Call nodes (we don't treat e.g.
    `JsonData` in `JsonData(...)` as a feature reference). Walk FormatString.names,
    BinaryOperation/BinaryComparison left/right, BooleanOperation values,
    UnaryOperation operand, AstList items, and Attribute chains.
    """
    if node is None:
        return
    if isinstance(node, Name):
        out.add(node.identifier)
        return
    if isinstance(node, Call):
        for arg in node.arguments:
            collect_name_references(arg.value, out)
        return
    if isinstance(node, FormatString):
        for n in node.names:
            out.add(n.identifier)
        return
    if isinstance(node, BinaryComparison):
        collect_name_references(node.left, out)
        collect_name_references(node.right, out)
        return
    if isinstance(node, BinaryOperation):
        collect_name_references(node.left, out)
        collect_name_references(node.right, out)
        return
    if isinstance(node, UnaryOperation):
        collect_name_references(node.operand, out)
        return
    if isinstance(node, BooleanOperation):
        for v in node.values:
            collect_name_references(v, out)
        return
    if isinstance(node, AstList):
        for item in node.items:
            collect_name_references(item, out)
        return
    if isinstance(node, Attribute):
        collect_name_references(node.name, out)
        return
