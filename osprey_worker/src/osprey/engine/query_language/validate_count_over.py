import re
from typing import List, Optional, Tuple

from osprey.engine.ast import grammar
from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast_validator.base_validator import SourceValidator
from osprey.engine.query_language.ast_validator import REGISTRY

ALLOWED_COMPARATORS: Tuple[type, ...] = (
    grammar.GreaterThanEquals,
    grammar.GreaterThan,
    grammar.Equals,
    grammar.NotEquals,
    grammar.LessThanEquals,
    grammar.LessThan,
)

TRIVIAL_THRESHOLDS = {
    (grammar.GreaterThanEquals, 0),
    (grammar.GreaterThanEquals, 1),
    (grammar.GreaterThan, 0),
    (grammar.LessThan, 1),
    (grammar.LessThanEquals, -1),
    (grammar.LessThanEquals, 0),
}

DEFAULT_THRESHOLD_CAP = 100_000


def _is_valid_time_delta_string(s: str) -> bool:
    """Check if a string matches the time-delta format: digits followed by s/m/h/d."""
    return bool(re.match(r'^\d+[smhd]$', s))


def _is_count_over_call(node: grammar.ASTNode) -> bool:
    return (
        isinstance(node, grammar.Call) and isinstance(node.func, grammar.Name) and node.func.identifier == 'CountOver'
    )


@REGISTRY.register
class ValidateCountOver(SourceValidator):
    """Enforces structural rules on `CountOver(...) <op> N` usage.

    The CountOver UDF (Osprey upstream) is query-only; this validator gates
    which SML shapes are allowed to submit.
    """

    threshold_cap: int = DEFAULT_THRESHOLD_CAP

    def run(self) -> None:
        """Override run() to examine all sources together instead of per-source.

        This is necessary because we need to detect multiple CountOvers across
        all sources in a single query.
        """
        if self._sources_contain_rule_context():
            for call_node in self._all_count_over_calls():
                self.context.add_error(
                    message='`CountOver` is query-only',
                    span=call_node.span,
                    hint='`CountOver(...)` may only be used in query contexts, not inside SML rules',
                )
            return

        count_over_calls = list(self._all_count_over_calls())
        if not count_over_calls:
            return

        # Find only top-level CountOver calls (exclude those nested in predicates of other CountOvers)
        top_level_count_over_calls = self._filter_top_level_count_over_calls(count_over_calls)

        # Validate nested detection BEFORE checking for multiple top-level calls
        for count_over_call in count_over_calls:
            self._validate_predicate_no_nested(count_over_call)

        if len(top_level_count_over_calls) > 1:
            for extra in top_level_count_over_calls[1:]:
                self.context.add_error(
                    message='only one `CountOver` per query is allowed',
                    span=extra.span,
                    hint='a query may contain at most one `CountOver(...)` call',
                )
            return

        if not top_level_count_over_calls:
            return

        count_over_call = top_level_count_over_calls[0]

        # Run all validations and check if all passed
        # Each _validate_* method returns True if passed, False if rejected
        self._validate_top_level_placement(count_over_call)
        self._validate_window_argument(count_over_call)
        self._validate_key_argument(count_over_call)
        self._validate_predicate_supported(count_over_call)

    def _sources_contain_rule_context(self) -> bool:
        for source in self.context.sources:
            for call in filter_nodes(source.ast_root, grammar.Call):
                if isinstance(call.func, grammar.Name) and call.func.identifier in ('Rule', 'WhenRules'):
                    return True
        return False

    def _all_count_over_calls(self) -> List[grammar.Call]:
        result: List[grammar.Call] = []
        for source in self.context.sources:
            for call in filter_nodes(source.ast_root, grammar.Call):
                if _is_count_over_call(call):
                    result.append(call)
        return result

    def _validate_top_level_placement(self, count_over_call: grammar.Call) -> bool:
        """Rule 1 + Rules 4-7. The CountOver call must sit as the LHS of a
        BinaryComparison that itself is either the top-level expression of an
        Assign or a value of a top-level And-BooleanOperation.

        Returns True if validation passed, False if a rule was violated.
        """

        comparison = self._find_enclosing_comparison(count_over_call)
        if comparison is None:
            self.context.add_error(
                message='`CountOver` must be the LHS of a comparison',
                span=count_over_call.span,
                hint='use `CountOver(...) >= N` (or another supported comparison)',
            )
            return False

        if not self._comparison_is_top_level(comparison):
            self.context.add_error(
                message='`CountOver` must be top-level',
                span=count_over_call.span,
                hint='`CountOver(...) <op> N` must be the outermost filter expression, '
                'or one conjunct of a top-level `AND` (no `NOT`, `OR`, or function-argument nesting)',
            )
            return False

        if not isinstance(comparison.comparator, ALLOWED_COMPARATORS):
            self.context.add_error(
                message='unsupported `CountOver` comparator',
                span=comparison.span,
                hint='use one of: `>=`, `>`, `==`, `!=`, `<=`, `<`',
            )
            return False

        threshold = self._extract_integer_literal(comparison.right)
        if threshold is None:
            self.context.add_error(
                message='`CountOver` threshold must be an integer literal',
                span=comparison.right.span,
                hint='the right-hand side of `CountOver(...) <op> N` must be a concrete integer literal',
            )
            return False

        comparator_type = type(comparison.comparator)
        if (comparator_type, threshold) in TRIVIAL_THRESHOLDS or threshold < 0:
            self.context.add_error(
                message='`CountOver` threshold is trivial',
                span=comparison.right.span,
                hint='reject thresholds that always or never match (e.g. `>= 0`, `> 0`, `<= 0`, negatives)',
            )
            return False

        if threshold > self.threshold_cap:
            self.context.add_error(
                message='`CountOver` threshold exceeds cap',
                span=comparison.right.span,
                hint=f'threshold must be `<= {self.threshold_cap}`',
            )
            return False

        return True

    def _filter_top_level_count_over_calls(self, all_calls: List[grammar.Call]) -> List[grammar.Call]:
        """Return only CountOver calls that are not nested inside another CountOver's predicate."""
        top_level = []
        for call in all_calls:
            is_nested = False
            for other_call in all_calls:
                if other_call is call:
                    continue
                predicate = other_call.find_argument('predicate')
                if predicate and any(n is call for n in filter_nodes(predicate.value, grammar.Call)):
                    is_nested = True
                    break
            if not is_nested:
                top_level.append(call)
        return top_level

    def _validate_predicate_no_nested(self, count_over_call: grammar.Call) -> None:
        predicate = count_over_call.find_argument('predicate')
        if predicate is None:
            return
        for nested in filter_nodes(predicate.value, grammar.Call):
            if _is_count_over_call(nested):
                self.context.add_error(
                    message='`CountOver` may not be nested',
                    span=nested.span,
                    hint='the `predicate` argument of `CountOver(...)` cannot itself contain `CountOver`',
                )
                return

    def _validate_window_argument(self, count_over_call: grammar.Call) -> bool:
        r"""Enforce that window is a string literal matching the time-delta regex ^\d+[smhd]$.

        Supported formats: '10m', '1h', '30s', '7d', etc.
        Returns True if validation passed (or window is optional and not provided),
        False if a rule was violated.
        """
        window = count_over_call.find_argument('window')
        if window is None:
            return True

        # Check that window.value is a String literal
        if not isinstance(window.value, grammar.String):
            self.context.add_error(
                message='`CountOver` window must be a string literal',
                span=window.value.span,
                hint='use `window="10m"`, `window="1h"`, `window="30s"`, or `window="7d"`',
            )
            return False

        window_str = window.value.value
        # Match pattern: digits followed by one of (s, m, h, d)
        if not _is_valid_time_delta_string(window_str):
            self.context.add_error(
                message='`CountOver` window string has invalid format',
                span=window.value.span,
                hint="use format like '10m', '1h', '30s', or '7d' (digits followed by s/m/h/d)",
            )
            return False

        return True

    def _validate_key_argument(self, count_over_call: grammar.Call) -> bool:
        """Validate that key argument, if provided, is a column reference.

        Returns True if validation passed (or key is optional and not provided),
        False if a rule was violated.
        """
        key = count_over_call.find_argument('key')
        if key is None:
            return True
        if not isinstance(key.value, grammar.Name):
            self.context.add_error(
                message='`CountOver` key must be a column reference',
                span=key.value.span,
                hint='the `key` argument must be a single column name (e.g. `key=UserId`)',
            )
            return False
        return True

    def _validate_predicate_supported(self, count_over_call: grammar.Call) -> bool:
        """Validate that the predicate only uses supported operators.

        Phase 2's _predicate_to_sql supports:
        - BinaryComparison with Equals or NotEquals only
        - BooleanOperation with And/Or
        - UnaryOperation with Not
        - Column names (Name nodes)
        - String and Number literals

        This phase-1 validation gates unsupported predicates (In, NotIn, function calls, etc.)
        at parse time instead of failing during SQL emission.

        Returns True if validation passed (or predicate is optional and not provided),
        False if a rule was violated.
        """
        predicate_arg = count_over_call.find_argument('predicate')
        if predicate_arg is None:
            return True

        predicate_node = predicate_arg.value
        unsupported_nodes = list(self._find_unsupported_predicate_nodes(predicate_node))
        if unsupported_nodes:
            node = unsupported_nodes[0]
            self.context.add_error(
                message='`CountOver` predicate uses an unsupported operator',
                span=node.span,
                hint=(
                    'predicates support only `==`, `!=`, `and`, `or`, `not`, column names, and literal values '
                    f'(found `{type(node).__name__}` — use simpler predicates or split into multiple queries)'
                ),
            )
            return False
        return True

    def _find_unsupported_predicate_nodes(self, node: grammar.ASTNode) -> List[grammar.ASTNode]:
        """Recursively find predicate nodes that are not supported by _predicate_to_sql.

        Returns a list of unsupported nodes in traversal order.
        """
        unsupported = []

        if isinstance(node, grammar.BinaryComparison):
            # Only Equals and NotEquals are supported
            if not isinstance(node.comparator, (grammar.Equals, grammar.NotEquals)):
                unsupported.append(node)
            else:
                # Recurse into left and right
                unsupported.extend(self._find_unsupported_predicate_nodes(node.left))
                unsupported.extend(self._find_unsupported_predicate_nodes(node.right))
        elif isinstance(node, grammar.BooleanOperation):
            # Only And and Or are supported
            if not isinstance(node.operand, (grammar.And, grammar.Or)):
                unsupported.append(node)
            else:
                # Recurse into values
                for value in node.values:
                    unsupported.extend(self._find_unsupported_predicate_nodes(value))
        elif isinstance(node, grammar.UnaryOperation):
            # Only Not is supported
            if not isinstance(node.operator, grammar.Not):
                unsupported.append(node)
            else:
                unsupported.extend(self._find_unsupported_predicate_nodes(node.operand))
        elif isinstance(node, (grammar.Name, grammar.String, grammar.Number, grammar.Boolean)):
            # Supported leaf nodes
            pass
        elif isinstance(node, grammar.Call):
            # CountOver calls are handled by the nested-CountOver validator,
            # so skip them here to avoid duplicate errors
            if _is_count_over_call(node):
                pass
            else:
                # Any other function call is unsupported
                unsupported.append(node)
        else:
            # Any other node type (In, NotIn, List, etc.) is unsupported
            unsupported.append(node)

        return unsupported

    def _find_enclosing_comparison(self, count_over_call: grammar.Call) -> Optional[grammar.BinaryComparison]:
        for source in self.context.sources:
            for comparison in filter_nodes(source.ast_root, grammar.BinaryComparison):
                if comparison.left is count_over_call:
                    return comparison
        return None

    def _comparison_is_top_level(self, comparison: grammar.BinaryComparison) -> bool:
        """A comparison is 'top-level' if it is either:
        - the direct value of an Assign statement, or
        - a value of an `And` BooleanOperation that is itself the direct value
          of an Assign.

        Implementation: walk every Assign in every source. The comparison is
        top-level iff it appears in the valid-shape set described above.
        """
        for source in self.context.sources:
            for assign in filter_nodes(source.ast_root, grammar.Assign):
                value = assign.value
                if value is comparison:
                    return True
                if isinstance(value, grammar.BooleanOperation) and isinstance(value.operand, grammar.And):
                    if any(v is comparison for v in value.values):
                        return True
        return False

    @staticmethod
    def _extract_integer_literal(node: grammar.Expression) -> Optional[int]:
        """Return the integer value if `node` is a Number literal containing an int
        (optionally wrapped in a `USub` UnaryOperation for negative literals)."""
        if isinstance(node, grammar.Number) and isinstance(node.value, int) and not isinstance(node.value, bool):
            return node.value
        if isinstance(node, grammar.UnaryOperation) and isinstance(node.operator, grammar.USub):
            inner = node.operand
            if isinstance(inner, grammar.Number) and isinstance(inner.value, int) and not isinstance(inner.value, bool):
                return -inner.value
        return None
