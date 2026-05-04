"""Shared AST traversal helpers used by features.py.

Extracted from rules.py so features.py and any future AST-walking view can share
the same implementation.
"""

from typing import Any, Optional

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
