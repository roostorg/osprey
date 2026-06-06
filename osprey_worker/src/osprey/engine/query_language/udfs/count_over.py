from dataclasses import dataclass
from typing import Dict, List, Optional, Type, Union

from osprey.engine.ast import grammar
from osprey.engine.query_language.udfs.registry import register
from osprey.engine.udf.arguments import ArgumentsBase, ConstExpr
from osprey.engine.udf.base import QueryUdfBase


# SQL operator strings keyed by AST comparator class. Used by the self-join
# CountOver lowering, which translates each operator directly into a
# `HAVING COUNT(*) <op> N` clause — simpler than the LAG approach (no need
# for double-LAG to express `== N` / `!= N`).
SELF_JOIN_COMPARATOR_OPS: Dict[Type[object], str] = {
    grammar.GreaterThanEquals: '>=',
    grammar.GreaterThan: '>',
    grammar.Equals: '=',
    grammar.NotEquals: '<>',
    grammar.LessThanEquals: '<=',
    grammar.LessThan: '<',
}


class Arguments(ArgumentsBase):
    predicate: bool
    window: ConstExpr[str]
    # `key` accepts column refs to Entity-typed columns. We use Union[int, str] so the type
    # evaluator's unwrap mechanism (EntityT[X] → X via PostExecutionConvertible) accepts
    # the common entity ID types — Entity[int] (snowflakes) and Entity[str].
    key: Optional[Union[int, str]] = None


@dataclass(frozen=True)
class OperatorMetadata:
    """Structural metadata for per-operator LAG-based subquery emission.

    Attributes:
        lag_offsets: Ordered list of LAG offset values (typically 1 or 2 elements).
                     Element 0 maps to pt1, element 1 (if present) maps to pt2.
        post_filter_template: SQL fragment using placeholders like {window_seconds}.
                             Example: "pt1 IS NOT NULL AND TIMESTAMPDIFF(SECOND, pt1, __time) <= {window_seconds}"
    """
    lag_offsets: List[int]
    post_filter_template: str


def operator_metadata_for(comparator_type: Type[object], threshold: int) -> OperatorMetadata:
    """Returns the per-operator metadata (LAG offsets and post-filter SQL) for CountOver lowering.

    Implements RFC Appendix A table mapping each comparison operator to its LAG configuration.

    Args:
        comparator_type: The outer operator class (e.g., grammar.GreaterThanEquals).
        threshold: The integer threshold literal (N in e.g., CountOver(...) >= N).

    Returns:
        OperatorMetadata with lag_offsets and post_filter_template populated.

    Raises:
        ValueError: If the comparator_type is not one of the six supported operators.
    """

    if comparator_type == grammar.GreaterThanEquals:
        # >= N: LAG(__time, N-1) AS pt1 + check pt1 exists within window
        return OperatorMetadata(
            lag_offsets=[threshold - 1],
            post_filter_template="pt1 IS NOT NULL AND TIMESTAMPDIFF(SECOND, pt1, __time) <= {window_seconds}",
        )
    elif comparator_type == grammar.GreaterThan:
        # > N: LAG(__time, N) AS pt1 + check pt1 exists within window
        return OperatorMetadata(
            lag_offsets=[threshold],
            post_filter_template="pt1 IS NOT NULL AND TIMESTAMPDIFF(SECOND, pt1, __time) <= {window_seconds}",
        )
    elif comparator_type == grammar.Equals:
        # == N: LAG(__time, N-1) AS pt1, LAG(__time, N) AS pt2
        # Filter: pt1 exists and within window AND (pt2 absent OR pt2 outside window)
        return OperatorMetadata(
            lag_offsets=[threshold - 1, threshold],
            post_filter_template=(
                "pt1 IS NOT NULL AND TIMESTAMPDIFF(SECOND, pt1, __time) <= {window_seconds} AND "
                "(pt2 IS NULL OR TIMESTAMPDIFF(SECOND, pt2, __time) > {window_seconds})"
            ),
        )
    elif comparator_type == grammar.NotEquals:
        # != N: Inverse of == N
        return OperatorMetadata(
            lag_offsets=[threshold - 1, threshold],
            post_filter_template=(
                "NOT (pt1 IS NOT NULL AND TIMESTAMPDIFF(SECOND, pt1, __time) <= {window_seconds} AND "
                "(pt2 IS NULL OR TIMESTAMPDIFF(SECOND, pt2, __time) > {window_seconds}))"
            ),
        )
    elif comparator_type == grammar.LessThanEquals:
        # <= N: LAG(__time, N) AS pt1 + check pt1 absent or outside window
        return OperatorMetadata(
            lag_offsets=[threshold],
            post_filter_template="pt1 IS NULL OR TIMESTAMPDIFF(SECOND, pt1, __time) > {window_seconds}",
        )
    elif comparator_type == grammar.LessThan:
        # < N: equivalent to <= N-1, so LAG(__time, N-1) AS pt1
        return OperatorMetadata(
            lag_offsets=[threshold - 1],
            post_filter_template="pt1 IS NULL OR TIMESTAMPDIFF(SECOND, pt1, __time) > {window_seconds}",
        )
    else:
        raise ValueError(
            f"Unsupported comparator type: {comparator_type}. "
            "Expected one of: GreaterThanEquals, GreaterThan, Equals, NotEquals, LessThanEquals, LessThan"
        )


@register
class CountOver(QueryUdfBase[Arguments, int]):
    """
    Counts occurrences of a predicate over a time window.

    # Examples

    `CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId)`
    `CountOver(predicate=Endpoint == '/foo', window='1m')`
    """

    def to_druid_query(self) -> Dict[str, object]:
        # Phase 2 Task 2 (DruidQueryTransformer) handles the actual Druid SQL emission.
        # The UDF itself only provides structural metadata via operator_metadata_for().
        # This method remains abstract-like for interface compliance; real work happens in the translator.
        raise NotImplementedError(
            "CountOver lowering happens in DruidQueryTransformer.transform(); "
            "call operator_metadata_for() to get structural metadata instead."
        )
