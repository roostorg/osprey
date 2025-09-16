from typing import Dict, Tuple

from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.grammar import Load, Name, Source, Store

from ..base_validator import SourceValidator


class NoUnusedLocals(SourceValidator):
    """
    Validates that all locals that are defined are read at least once.
    """

    def validate_source(self, source: 'Source') -> None:
        seen_locals: Dict[str, Tuple[int, Name]] = {}

        # Walk AST for all name nodes, finding nodes that are stored, but never loaded,
        # by maintaining a counter of identifier -> load count.
        for name in filter_nodes(source.ast_root, Name, lambda n: n.is_local):
            if isinstance(name.context, Store):
                seen_locals.setdefault(name.identifier, (0, name))

            elif isinstance(name.context, Load) and name.identifier in seen_locals:
                count, parent_node = seen_locals[name.identifier]
                seen_locals[name.identifier] = (count + 1, parent_node)

        for load_count, name in seen_locals.values():
            # If seen more than once, the local is used.
            if load_count > 0:
                continue

            self.context.add_error(
                message=f'unused local variable: `{name.identifier}`',
                span=name.span,
                hint='this variable is not used anywhere, and thus has no effect. either delete or comment it out',
            )
