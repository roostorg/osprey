import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from osprey.engine.ast import grammar
from osprey.engine.ast_validator.validation_context import ValidatedSources
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.udf.base import QueryUdfBase
from osprey.engine.utils.osprey_unary_executor import OspreyUnaryExecutor
from osprey.engine.query_language.udfs.count_over import (
    SELF_JOIN_COMPARATOR_OPS,
    CountOver,
    operator_metadata_for,
)


# Output modes for `DruidQueryTransformer.transform()` when lowering CountOver
# into SQL. Each mode emits a different outer shape suitable for a particular
# Druid consumer.
COUNT_OVER_OUTPUT_INNER = 'inner'
COUNT_OVER_OUTPUT_SCAN = 'scan'
COUNT_OVER_OUTPUT_TIMESERIES = 'timeseries'
COUNT_OVER_OUTPUT_TOP_N = 'top_n'
_COUNT_OVER_OUTPUT_MODES = (
    COUNT_OVER_OUTPUT_INNER,
    COUNT_OVER_OUTPUT_SCAN,
    COUNT_OVER_OUTPUT_TIMESERIES,
    COUNT_OVER_OUTPUT_TOP_N,
)

# SQL engines for the CountOver lowering. The default 'window' uses SQL window
# functions (LAG over PARTITION BY/ORDER BY) and requires Druid 31+ for reliable
# execution. The 'self_join' engine emits a self-join + GROUP BY + HAVING SQL
# that works on Druid 27+ where window functions are unavailable or buggy.
COUNT_OVER_ENGINE_WINDOW = 'window'
COUNT_OVER_ENGINE_SELF_JOIN = 'self_join'
_COUNT_OVER_ENGINES = (COUNT_OVER_ENGINE_WINDOW, COUNT_OVER_ENGINE_SELF_JOIN)


def _format_druid_timestamp_literal(dt: datetime) -> str:
    """Format a datetime for a Druid Calcite `TIMESTAMP 'yyyy-MM-dd HH:mm:ss'` literal.

    Calcite rejects ISO 8601 `T` separators and any UTC offset suffix (e.g.
    `+00:00`), so tz-aware inputs are normalized to naive UTC first.
    """
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime('%Y-%m-%d %H:%M:%S')


class DruidQueryTransformException(Exception):
    """Some error happened while trying to transform the Osprey AST into a Druid Query"""

    def __init__(self, node: grammar.ASTNode, error: str):
        super().__init__(f'{error}: {node.__class__.__name__}')
        self.node = node


class DruidQueryTransformer:
    """Given a osprey_ast node tree, transform it into a Druid query.

    For CountOver lowering, `datasource_name` is interpolated into the emitted
    SQL's `FROM` clause. The default `'datasource'` preserves the historical
    behavior where callers substitute the placeholder themselves; pass a real
    Druid datasource name (e.g. `'events'`) to get executable SQL.
    The name is quoted in the SQL output to support names containing `.`.

    `sql_engine` selects the CountOver lowering strategy:
    - `'window'` (default) — emits LAG over PARTITION BY/ORDER BY. Requires
      Druid 31+ for reliable execution (window function GA cutoff).
    - `'self_join'` — emits self-join + GROUP BY + HAVING. Works on Druid 27+
      where SQL window functions are unavailable or buggy. SCAN-mode output
      is projected to `(__action_id, __time)` — the event-row contract;
      datasources without `__action_id` must use a different engine or output
      mode.

    Output-shape parameters control whether the emitted SQL is ready to run
    against Druid or just the inner CountOver shape:

    - `output_mode='inner'` (default) emits the inner CountOver shape (no
      outer time bound or projection). Callers wrap it themselves.
    - `output_mode='scan'` requires `time_bounds`. Emits a SCAN-style result.
    - `output_mode='timeseries'` requires `time_bounds` and
      `granularity_period` (ISO 8601 period like `'PT1H'`). Emits
      `SELECT TIME_FLOOR(__time, '<period>') AS bucket, COUNT(*) AS cnt ...
      GROUP BY 1 ORDER BY 1` over the inner result.
    """

    def __init__(
        self,
        validated_sources: ValidatedSources,
        datasource_name: str = 'datasource',
        output_mode: str = COUNT_OVER_OUTPUT_INNER,
        time_bounds: Optional[Tuple[datetime, datetime]] = None,
        granularity_period: Optional[str] = None,
        sql_engine: str = COUNT_OVER_ENGINE_WINDOW,
        topn_dimension: Optional[str] = None,
        topn_limit: Optional[int] = None,
    ):
        if output_mode not in _COUNT_OVER_OUTPUT_MODES:
            raise ValueError(
                f'output_mode must be one of {_COUNT_OVER_OUTPUT_MODES}; got {output_mode!r}'
            )
        if output_mode != COUNT_OVER_OUTPUT_INNER and time_bounds is None:
            raise ValueError(f'output_mode={output_mode!r} requires time_bounds')
        if output_mode == COUNT_OVER_OUTPUT_TIMESERIES and not granularity_period:
            raise ValueError("output_mode='timeseries' requires granularity_period (ISO 8601, e.g. 'PT1H')")
        if output_mode == COUNT_OVER_OUTPUT_TOP_N and (not topn_dimension or topn_limit is None):
            raise ValueError("output_mode='top_n' requires topn_dimension and topn_limit")
        if sql_engine not in _COUNT_OVER_ENGINES:
            raise ValueError(f'sql_engine must be one of {_COUNT_OVER_ENGINES}; got {sql_engine!r}')

        try:
            self._udf_node_mapping = validated_sources.get_validator_result(ValidateCallKwargs)
        except KeyError:
            self._udf_node_mapping = {}

        assign_node = validated_sources.sources.get_entry_point().ast_root.statements[0]
        assert isinstance(assign_node, grammar.Assign)
        self._root = assign_node.value
        self._datasource_name = datasource_name
        self._output_mode = output_mode
        self._time_bounds = time_bounds
        self._granularity_period = granularity_period
        self._sql_engine = sql_engine
        self._topn_dimension = topn_dimension
        self._topn_limit = topn_limit

    def transform(self) -> Dict[str, Any]:
        """Transform AST to Druid query.

        Returns a tagged shape: {'type': 'sql', 'sql': '...'} for CountOver queries,
        or {'type': 'native', 'filter': {...}} for native queries.
        """
        # Pre-pass: detect top-level CountOver pattern
        count_over_info = self._detect_count_over(self._root)
        if count_over_info:
            predicate, window_seconds, key, comparator_type, threshold, other_conjuncts = count_over_info
            if self._sql_engine == COUNT_OVER_ENGINE_SELF_JOIN:
                sql = self._compose_count_over_sql_self_join(
                    predicate, window_seconds, key, comparator_type, threshold, other_conjuncts
                )
            else:
                inner_sql = self._compose_count_over_sql(
                    predicate, window_seconds, key, comparator_type, threshold, other_conjuncts
                )
                sql = self._apply_output_wrap(inner_sql)
            return {'type': 'sql', 'sql': sql}

        # Fall through to native query transformation
        return {'type': 'native', 'filter': self._transform(self._root)}

    def _apply_output_wrap(self, inner_sql: str) -> str:
        """Wrap the inner CountOver SQL according to `self._output_mode`.

        Used by the window-function engine. The self-join engine builds its
        output shape directly in `_compose_count_over_sql_self_join` because
        GROUP BY semantics make it hard to do a generic outer wrap.
        """
        if self._output_mode == COUNT_OVER_OUTPUT_INNER:
            return inner_sql

        # Both SCAN and TIMESERIES need the time-bound WHERE clause.
        assert self._time_bounds is not None  # validated in __init__
        start, end = self._time_bounds
        start_lit = _format_druid_timestamp_literal(start)
        end_lit = _format_druid_timestamp_literal(end)
        time_where = (
            f"__time >= TIMESTAMP '{start_lit}' "
            f"AND __time < TIMESTAMP '{end_lit}'"
        )

        if self._output_mode == COUNT_OVER_OUTPUT_SCAN:
            # SCAN: just project everything and apply the time bound. Calcite
            # requires `AS __t` on the outer FROM subquery.
            return f"SELECT * FROM ({inner_sql}) AS __t WHERE {time_where}"

        if self._output_mode == COUNT_OVER_OUTPUT_TIMESERIES:
            # TIMESERIES: bucket by TIME_FLOOR and count. The native timeseries
            # consumer expects `[{timestamp, result: {count}}, ...]`; we emit
            # rows of `{bucket, cnt}` which the caller can adapt.
            assert self._granularity_period is not None  # validated in __init__
            return (
                f"SELECT TIME_FLOOR(__time, '{self._granularity_period}') AS bucket, "
                f"COUNT(*) AS cnt "
                f"FROM ({inner_sql}) AS __t "
                f"WHERE {time_where} "
                f"GROUP BY 1 ORDER BY 1"
            )

        # TOP_N: group the burst events by the requested dimension, ordered by
        # count desc, top K. Emits rows of `{__dim, cnt}` which the caller can
        # adapt to the native TopN response shape.
        assert self._output_mode == COUNT_OVER_OUTPUT_TOP_N
        assert self._topn_dimension is not None and self._topn_limit is not None
        return (
            f'SELECT {self._topn_dimension} AS __dim, COUNT(*) AS cnt '
            f"FROM ({inner_sql}) AS __t "
            f'WHERE {time_where} '
            f'GROUP BY {self._topn_dimension} '
            f'ORDER BY cnt DESC '
            f'LIMIT {self._topn_limit}'
        )

    def _detect_count_over(
        self, node: grammar.ASTNode
    ) -> Optional[Tuple[grammar.ASTNode, int, Optional[str], type, int, List[grammar.ASTNode]]]:
        """Detect top-level CountOver(p, w, key) <op> N pattern.

        Returns (predicate, window_seconds, key, comparator_type, threshold, other_conjuncts) or None.
        """
        # Check if it's a binary comparison at the top level
        if isinstance(node, grammar.BinaryComparison):
            return self._try_extract_count_over_from_comparison(node, [])

        # Check if it's an AND that might contain a CountOver comparison
        if isinstance(node, grammar.BooleanOperation) and isinstance(node.operand, grammar.And):
            # Find which value is the CountOver comparison, if any
            count_over_comp: Optional[Tuple[grammar.ASTNode, int, Optional[str], type, int, List[grammar.ASTNode]]] = None
            other_values: List[grammar.ASTNode] = []
            for value in node.values:
                if isinstance(value, grammar.BinaryComparison):
                    result = self._try_extract_count_over_from_comparison(value, [])
                    if result:
                        count_over_comp = result
                    else:
                        other_values.append(value)
                else:
                    other_values.append(value)
            if count_over_comp:
                # Return the full tuple with other_values
                return (count_over_comp[0], count_over_comp[1], count_over_comp[2], count_over_comp[3], count_over_comp[4], other_values)

        return None

    def _try_extract_count_over_from_comparison(
        self, comp: grammar.BinaryComparison, accumulated: List[grammar.ASTNode]
    ) -> Optional[Tuple[grammar.ASTNode, int, Optional[str], type, int, List[grammar.ASTNode]]]:
        """Try to extract CountOver pattern from a BinaryComparison.

        Returns (predicate, window_seconds, key, comparator_type, threshold, accumulated) or None.
        """
        # Check if left side is a CountOver call
        left_call = None
        if isinstance(comp.left, grammar.Call):
            left_call = comp.left

        # Check if right side is a CountOver call
        right_call = None
        if isinstance(comp.right, grammar.Call):
            right_call = comp.right

        # We expect exactly one CountOver call
        count_over_call = left_call or right_call
        if not count_over_call:
            return None

        # Verify it's actually a CountOver call
        if not (isinstance(count_over_call.func, grammar.Name) and count_over_call.func.identifier == 'CountOver'):
            return None

        # Get the UDF to verify it's the CountOver UDF
        udf, _ = self._udf_node_mapping.get(id(count_over_call), (None, None))
        if not isinstance(udf, CountOver):
            return None

        # Extract the threshold literal (the other side of the comparison)
        threshold_node = comp.right if left_call else comp.left
        threshold = self._extract_literal_int(threshold_node)
        if threshold is None:
            return None

        # Extract CountOver arguments: predicate, window, key
        predicate = self._find_argument(count_over_call, 'predicate')
        if predicate is None:
            return None

        window_arg = self._find_argument(count_over_call, 'window')
        if window_arg is None:
            return None
        window_seconds = self._extract_window_seconds(window_arg)
        if window_seconds is None:
            return None

        key = self._extract_key(count_over_call)

        comparator_type = type(comp.comparator)

        return (predicate, window_seconds, key, comparator_type, threshold, accumulated)

    def _find_argument(self, call: grammar.Call, name: str) -> Optional[grammar.ASTNode]:
        """Find a named argument in a Call node."""
        keyword = call.find_argument(name)
        return keyword.value if keyword else None

    def _extract_literal_int(self, node: grammar.ASTNode) -> Optional[int]:
        """Extract an integer literal from an AST node."""
        if isinstance(node, grammar.Number) and isinstance(node.value, int):
            return node.value
        return None

    def _extract_window_seconds(self, node: grammar.ASTNode) -> Optional[int]:
        """Extract window duration in seconds from a time-delta string literal.

        Supports formats like '10m', '1h', '30s', '7d' with units: s, m, h, d.
        Returns total seconds, or None if the format is invalid.
        """
        if not isinstance(node, grammar.String):
            return None

        window_str = node.value
        # Match pattern: digits followed by one of (s, m, h, d)
        match = re.match(r'^(\d+)([smhd])$', window_str)
        if not match:
            return None

        amount_str, unit = match.groups()
        try:
            amount = int(amount_str)
        except ValueError:
            return None

        # Convert to seconds based on unit
        multipliers = {
            's': 1,
            'm': 60,
            'h': 3600,
            'd': 86400,
        }

        return amount * multipliers[unit]

    def _extract_key(self, call: grammar.Call) -> Optional[str]:
        """Extract the key argument (partition column name) from a CountOver call."""
        key_node = self._find_argument(call, 'key')
        if key_node is None:
            return None
        if isinstance(key_node, grammar.Name):
            return key_node.identifier
        if isinstance(key_node, grammar.String):
            return key_node.value
        return None

    def _compose_count_over_sql(
        self,
        predicate: grammar.ASTNode,
        window_seconds: int,
        key: Optional[str],
        comparator_type: type,
        threshold: int,
        other_conjuncts: List[grammar.ASTNode],
    ) -> str:
        """Compose the SQL string for a CountOver query."""
        # Get operator metadata (LAG offsets and post-filter template)
        metadata = operator_metadata_for(comparator_type, threshold)

        # Translate predicate and other conjuncts to WHERE clause SQL
        predicate_sql = self._predicate_to_sql(predicate)
        other_sqls = [self._predicate_to_sql(conj) for conj in other_conjuncts]
        where_conditions = [predicate_sql] + other_sqls
        where_clause = ' AND '.join(where_conditions)

        # Build the OVER clause (with or without PARTITION BY)
        if key:
            over_clause = f"OVER (PARTITION BY {key} ORDER BY __time)"
        else:
            over_clause = "OVER (ORDER BY __time)"

        # Build LAG column selections
        lag_columns = []
        for i, offset in enumerate(metadata.lag_offsets):
            pt_name = f"pt{i + 1}"
            lag_columns.append(f"LAG(__time, {offset}) {over_clause} AS {pt_name}")

        lag_select = ', '.join(lag_columns)

        # Build the inner SELECT (with LAG columns).
        # `datasource_name` defaults to a literal `datasource` placeholder so
        # legacy callers that substitute it themselves keep working; callers
        # that pass a real name get executable SQL directly. The name is
        # double-quoted in case it contains `.` (Druid datasources commonly
        # do, e.g. `events`).
        quoted_datasource = f'"{self._datasource_name}"'
        inner_select = f"SELECT *, {lag_select} FROM {quoted_datasource} WHERE {where_clause}"

        # Build the post-filter SQL
        post_filter = metadata.post_filter_template.format(window_seconds=window_seconds)

        # Combine into outer SELECT. Druid's Calcite parser requires every
        # FROM-clause subquery to be aliased — `AS __inner` satisfies that.
        sql = f"SELECT * FROM ({inner_select}) AS __inner WHERE {post_filter}"

        return sql

    def _compose_count_over_sql_self_join(
        self,
        predicate: grammar.ASTNode,
        window_seconds: int,
        key: Optional[str],
        comparator_type: type,
        threshold: int,
        other_conjuncts: List[grammar.ASTNode],
    ) -> str:
        """Compose CountOver SQL as a self-join + GROUP BY + HAVING.

        Pattern (for `CountOver(predicate=P, window=W, key=K) <op> N`):

            SELECT t1.__action_id, t1.__time
            FROM (SELECT * FROM <ds> WHERE P) t1
            INNER JOIN (SELECT * FROM <ds> WHERE P) t2
              ON t1.K = t2.K              -- or 1=1 (synthetic constant) if no key
            WHERE t2.__time <= t1.__time
              AND t2.__time >= TIMESTAMPADD(SECOND, -W, t1.__time)
            GROUP BY t1.__action_id, t1.__time
            HAVING COUNT(*) <op> N

        Notes / constraints:
        - Druid 27+ supports this shape (verified against Druid 27.0.0 broker).
        - For SCAN output we project `(t1.__action_id, t1.__time)` rather than
          `t1.*` because GROUP BY otherwise needs to list every column on the
          datasource. `__action_id` is the canonical event-id column on
          osprey datasources.
        - For TIMESERIES output we only need `t1.__time` for bucketing.
        - For the no-key case we synthesize a constant join key (`1 AS __k`) so
          the join is still equi-joined — bare cross-join works too but is less
          friendly to Druid's planner.
        """
        op = SELF_JOIN_COMPARATOR_OPS.get(comparator_type)
        if op is None:
            raise ValueError(
                f'self_join engine: unsupported comparator {comparator_type.__name__}'
            )

        # WHERE clause applied to BOTH sides of the join — t1 and t2 must
        # satisfy the same predicate (only matching events count toward the
        # window).
        predicate_sql = self._predicate_to_sql(predicate)
        other_sqls = [self._predicate_to_sql(c) for c in other_conjuncts]
        where_clause = ' AND '.join([predicate_sql] + other_sqls)

        quoted_ds = f'"{self._datasource_name}"'

        # Synthesize a constant key for no-key CountOver so the planner sees an
        # equi-join rather than a bare cross-join.
        if key:
            t1_subq = f'(SELECT * FROM {quoted_ds} WHERE {where_clause}) t1'
            t2_subq = f'(SELECT * FROM {quoted_ds} WHERE {where_clause}) t2'
            join_on = f't1.{key} = t2.{key}'
        else:
            t1_subq = f'(SELECT *, 1 AS __k FROM {quoted_ds} WHERE {where_clause}) t1'
            t2_subq = f'(SELECT *, 1 AS __k FROM {quoted_ds} WHERE {where_clause}) t2'
            join_on = 't1.__k = t2.__k'

        time_window_where = (
            f't2.__time <= t1.__time '
            f'AND t2.__time >= TIMESTAMPADD(SECOND, -{window_seconds}, t1.__time)'
        )

        if self._output_mode == COUNT_OVER_OUTPUT_TIMESERIES:
            assert self._time_bounds is not None and self._granularity_period is not None
            start, end = self._time_bounds
            start_lit = _format_druid_timestamp_literal(start)
            end_lit = _format_druid_timestamp_literal(end)
            # Inner: identify the qualifying __time values via self-join + HAVING.
            # Outer: bucket those by TIME_FLOOR and count, applying the request's
            # time bound to restrict the histogram window.
            return (
                f"SELECT TIME_FLOOR(__time, '{self._granularity_period}') AS bucket, "
                f"COUNT(*) AS cnt FROM ("
                f"SELECT t1.__time AS __time "
                f"FROM {t1_subq} INNER JOIN {t2_subq} ON {join_on} "
                f"WHERE {time_window_where} "
                f"GROUP BY t1.__time HAVING COUNT(*) {op} {threshold}"
                f") AS __t "
                f"WHERE __time >= TIMESTAMP '{start_lit}' "
                f"AND __time < TIMESTAMP '{end_lit}' "
                f"GROUP BY 1 ORDER BY 1"
            )

        if self._output_mode == COUNT_OVER_OUTPUT_TOP_N:
            assert self._time_bounds is not None
            assert self._topn_dimension is not None and self._topn_limit is not None
            start, end = self._time_bounds
            start_lit = _format_druid_timestamp_literal(start)
            end_lit = _format_druid_timestamp_literal(end)
            # Inner: same self-join + HAVING, but also project the dimension
            # column from t1 so the outer GROUP BY can aggregate by it.
            # Outer: GROUP BY dimension, ORDER BY count DESC, LIMIT K.
            return (
                f'SELECT __dim, COUNT(*) AS cnt FROM ('
                f'SELECT t1.__time AS __time, t1.{self._topn_dimension} AS __dim '
                f'FROM {t1_subq} INNER JOIN {t2_subq} ON {join_on} '
                f'WHERE {time_window_where} '
                f'GROUP BY t1.__time, t1.{self._topn_dimension} '
                f'HAVING COUNT(*) {op} {threshold}'
                f') AS __t '
                f"WHERE __time >= TIMESTAMP '{start_lit}' "
                f"AND __time < TIMESTAMP '{end_lit}' "
                f'GROUP BY __dim ORDER BY cnt DESC LIMIT {self._topn_limit}'
            )

        # SCAN or INNER: project (__action_id, __time) of each qualifying
        # burst-end event. SCAN additionally filters by the request's time
        # bound; INNER returns the unwrapped result so the caller can wrap.
        scan_core = (
            f'SELECT t1.__action_id, t1.__time '
            f'FROM {t1_subq} INNER JOIN {t2_subq} ON {join_on} '
            f'WHERE {time_window_where} '
            f'GROUP BY t1.__action_id, t1.__time '
            f'HAVING COUNT(*) {op} {threshold}'
        )
        if self._output_mode == COUNT_OVER_OUTPUT_INNER:
            return scan_core
        assert self._output_mode == COUNT_OVER_OUTPUT_SCAN
        assert self._time_bounds is not None
        start, end = self._time_bounds
        start_lit = _format_druid_timestamp_literal(start)
        end_lit = _format_druid_timestamp_literal(end)
        return (
            f"SELECT * FROM ({scan_core}) AS __t "
            f"WHERE __time >= TIMESTAMP '{start_lit}' "
            f"AND __time < TIMESTAMP '{end_lit}'"
        )

    def _predicate_to_sql(self, node: grammar.ASTNode) -> str:
        """Convert a predicate AST node to SQL WHERE clause fragment.

        Supports: Name == 'literal', Name != 'literal', And, Or, and parenthesization.
        Raises NotImplementedError for unsupported constructs.
        """
        if isinstance(node, grammar.BinaryComparison):
            return self._binary_comparison_to_sql(node)
        elif isinstance(node, grammar.BooleanOperation):
            if isinstance(node.operand, grammar.And):
                operand_str = ' AND '
            elif isinstance(node.operand, grammar.Or):
                operand_str = ' OR '
            else:
                raise NotImplementedError(f"Unsupported boolean operand: {type(node.operand)}")
            parts = [self._predicate_to_sql(v) for v in node.values]
            return f"({operand_str.join(parts)})"
        elif isinstance(node, grammar.UnaryOperation):
            if isinstance(node.operator, grammar.Not):
                inner = self._predicate_to_sql(node.operand)
                return f"NOT ({inner})"
            else:
                raise NotImplementedError(f"Unsupported unary operator: {type(node.operator)}")
        else:
            raise NotImplementedError(f"Unsupported predicate node type: {type(node).__name__}")

    def _binary_comparison_to_sql(self, comp: grammar.BinaryComparison) -> str:
        """Convert a BinaryComparison to SQL (e.g., 'column == value')."""
        # Extract column name and value
        left_is_col = isinstance(comp.left, grammar.Name)
        right_is_col = isinstance(comp.right, grammar.Name)

        if left_is_col and not right_is_col:
            assert isinstance(comp.left, grammar.Name)
            col_name = comp.left.identifier
            value = self._get_ast_node_value(comp.right)
        elif right_is_col and not left_is_col:
            assert isinstance(comp.right, grammar.Name)
            col_name = comp.right.identifier
            value = self._get_ast_node_value(comp.left)
        else:
            raise NotImplementedError(f"Unsupported binary comparison: both or neither sides are columns")

        # Format the value
        value_sql = self._format_sql_value(value)

        # Map comparator to SQL operator
        if isinstance(comp.comparator, grammar.Equals):
            op = '='
        elif isinstance(comp.comparator, grammar.NotEquals):
            op = '!='
        elif isinstance(comp.comparator, grammar.GreaterThan):
            op = '>'
        elif isinstance(comp.comparator, grammar.GreaterThanEquals):
            op = '>='
        elif isinstance(comp.comparator, grammar.LessThan):
            op = '<'
        elif isinstance(comp.comparator, grammar.LessThanEquals):
            op = '<='
        else:
            raise NotImplementedError(f"Unsupported comparator: {type(comp.comparator)}")

        return f"{col_name} {op} {value_sql}"

    def _get_ast_node_value(self, node: grammar.ASTNode) -> Any:
        """Extract a Python value from an AST node (mirrors the existing get_ast_node_value)."""
        if isinstance(node, grammar.UnaryOperation):
            return OspreyUnaryExecutor(node).get_execution_value()
        elif isinstance(node, grammar.List):
            return [self._get_ast_node_value(i) for i in node.items]
        elif isinstance(node, grammar.None_):
            return None
        elif isinstance(node, (grammar.String, grammar.Number, grammar.Boolean)):
            return node.value
        else:
            raise NotImplementedError(f"Node has no known value: {type(node)}")

    def _format_sql_value(self, value: Any) -> str:
        """Format a Python value for use in SQL string."""
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            # Escape single quotes by doubling them
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        else:
            raise NotImplementedError(f"Cannot format value type: {type(value)}")

    def _transform(self, node: grammar.ASTNode) -> Dict[str, Any]:
        method = 'transform_' + node.__class__.__name__
        transformer = getattr(self, method, None)

        if not transformer:
            raise DruidQueryTransformException(node, 'Unknown AST Expression')

        ret = transformer(node)
        assert isinstance(ret, dict)
        return ret

    def transform_BooleanOperation(self, node: grammar.BooleanOperation) -> Dict[str, Any]:
        assert isinstance(node.operand, grammar.And) or isinstance(node.operand, grammar.Or)

        filter_type = 'and' if isinstance(node.operand, grammar.And) else 'or'
        values = [self._transform(v) for v in node.values]
        return {'type': filter_type, 'fields': values}

    def transform_BinaryComparison(self, node: grammar.BinaryComparison) -> Dict[str, Any]:
        if isinstance(node.left, grammar.Name) and isinstance(node.right, grammar.Name):
            column_comparison = {
                'type': 'columnComparison',
                'dimensions': [node.left.identifier, node.right.identifier],
            }
            if isinstance(node.comparator, grammar.Equals):
                return column_comparison
            elif isinstance(node.comparator, grammar.NotEquals):
                return {'type': 'not', 'field': column_comparison}
            else:
                raise DruidQueryTransformException(
                    node.comparator, 'When comparing two features, only the `==` and `!=` operators are supported'
                )

        dimension = get_comparison_dimension(node)
        value = get_comparison_value(node)

        if isinstance(node.comparator, grammar.Equals):
            return {'type': 'selector', 'dimension': dimension, 'value': value}
        elif isinstance(node.comparator, grammar.In):
            return get_in_query_by_value_type(node, dimension, value)
        elif isinstance(node.comparator, grammar.NotEquals):
            return {'type': 'not', 'field': {'type': 'selector', 'dimension': dimension, 'value': value}}
        elif isinstance(node.comparator, grammar.NotIn):
            return {'type': 'not', 'field': get_in_query_by_value_type(node, dimension, value)}

        bound_query = {
            'type': 'bound',
            'dimension': dimension,
            'ordering': get_value_bound_ordering(value),
            **get_druid_bound_query_props(node, value),
        }

        # greater than and less than queries require an explicit not null check
        return {
            'type': 'and',
            'fields': [
                {'type': 'not', 'field': {'type': 'selector', 'dimension': dimension, 'value': None}},
                bound_query,
            ],
        }

    def transform_UnaryOperation(self, node: grammar.UnaryOperation) -> Dict[str, Any]:
        if isinstance(node.operator, grammar.Not):
            return {'type': 'not', 'field': self._transform(node.operand)}
        else:
            raise DruidQueryTransformException(node, 'Unknown Unary Operator')

    def transform_Call(self, node: grammar.Call) -> Dict[str, Any]:
        udf, _ = self._udf_node_mapping[id(node)]

        if not isinstance(udf, QueryUdfBase):
            raise DruidQueryTransformException(node, 'Unknown function call type')

        return udf.to_druid_query()


def get_in_query_by_value_type(node: grammar.BinaryComparison, dimension: str, comparison_value: Any) -> Dict[str, Any]:
    if isinstance(comparison_value, str):
        return {
            'type': 'search',
            'dimension': dimension,
            'query': {'type': 'insensitive_contains', 'value': comparison_value},
        }
    elif isinstance(comparison_value, list):
        return {'type': 'in', 'dimension': dimension, 'values': comparison_value}
    else:
        raise DruidQueryTransformException(node, 'Invalid "in" comparison value type, must be string or list')


def get_druid_bound_query_props(node: grammar.BinaryComparison, comparison_value: Any) -> Dict[str, Any]:
    """Get the correct query properties for the various type of `bound` filters"""

    if isinstance(node.comparator, grammar.LessThan):
        return {'upper': comparison_value, 'upperStrict': True}
    elif isinstance(node.comparator, grammar.LessThanEquals):
        return {'upper': comparison_value}
    elif isinstance(node.comparator, grammar.GreaterThan):
        return {'lower': comparison_value, 'lowerStrict': True}
    elif isinstance(node.comparator, grammar.GreaterThanEquals):
        return {'lower': comparison_value}
    else:
        raise DruidQueryTransformException(node.comparator, 'Unknown Binary Comparator')


def get_comparison_dimension(node: grammar.BinaryComparison) -> str:
    """Extracts the dimension name for a binary comparison"""

    if isinstance(node.left, grammar.Name):
        return node.left.identifier
    elif isinstance(node.right, grammar.Name):
        return node.right.identifier
    else:
        raise DruidQueryTransformException(node, 'Binary Comparator must contain at least one column')


def get_comparison_value(node: grammar.BinaryComparison) -> Any:
    """Extracts the value for a binary comparison"""

    if isinstance(node.left, (grammar.Literal, grammar.UnaryOperation)):
        return get_ast_node_value(node.left)
    elif isinstance(node.right, (grammar.Literal, grammar.UnaryOperation)):
        return get_ast_node_value(node.right)


def get_ast_node_value(node: grammar.ASTNode) -> Any:
    """Gets the relevant value from any given expression type (Name or Literal)

    Unary operations can be evaluated into literals here (for negative Numbers)
    """

    if isinstance(node, grammar.UnaryOperation):
        return OspreyUnaryExecutor(node).get_execution_value()
    elif isinstance(node, grammar.List):
        return [get_ast_node_value(i) for i in node.items]
    elif isinstance(node, grammar.None_):
        return None
    elif isinstance(node, grammar.String) or isinstance(node, grammar.Number) or isinstance(node, grammar.Boolean):
        return node.value
    else:
        raise DruidQueryTransformException(node, 'Node has no known value attribute')


def get_value_bound_ordering(value: Any) -> str:
    """Given a value, return the appropriate comparator for the value to be used in a bound filter, throwing if it
    cannot be compared."""

    if isinstance(value, (int, float)):
        return 'numeric'
    elif isinstance(value, str):
        return 'lexicographic'

    raise TypeError(f'Cannot compare a {value.__class__.__name__}')
