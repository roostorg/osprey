"""ClickHouse query backend for Osprey UI — replaces druid.py.

Provides the same query types (timeseries, topN, scan, groupBy) but
generates ClickHouse SQL instead of Druid JSON queries. Uses the same
Pydantic models for responses so the UI API layer is unchanged.
"""

import base64
import logging
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

from osprey.engine.query_language import parse_query_to_validated_ast
from osprey.engine.query_language.ast_clickhouse_translator import ClickHouseQueryTransformer
from osprey.worker.lib.singletons import CONFIG, ENGINE
from pydantic.main import BaseModel

from .marshal import JsonBodyMarshaller

if TYPE_CHECKING:
    from .abilities import QueryFilterAbility

logger = logging.getLogger(__name__)

DEFAULT_QUERY_TIMEOUT = 300  # seconds


class Ordering(str, Enum):
    ASCENDING = 'ASCENDING'
    DESCENDING = 'DESCENDING'


class PaginatedScanResult(BaseModel):
    action_ids: List[int]
    next_page: Optional[str]


class EntityFilter(BaseModel):
    id: str
    type: str
    feature_filters: Optional[List[str]]

    def to_sql_clause(self) -> str:
        """Generate a SQL WHERE clause fragment for entity filtering."""
        feature_to_entity_mapping = ENGINE.instance().get_feature_name_to_entity_type_mapping()
        matching_features = [
            feature_name
            for feature_name, entity_type in feature_to_entity_mapping.items()
            if entity_type == self.type and (not self.feature_filters or feature_name in self.feature_filters)
        ]

        if not matching_features:
            return '1=0'

        escaped_id = self.id.replace("'", "\\'")
        clauses = [f"`{feat}` = '{escaped_id}'" for feat in matching_features]
        return '(' + ' OR '.join(clauses) + ')'


class DimensionData(BaseModel):
    count: int

    class Config:
        extra = 'allow'


class PeriodData(BaseModel):
    timestamp: datetime
    result: List[DimensionData]


class DimensionDifference(BaseModel):
    dimension_key: str | None
    current_count: int
    previous_count: int
    difference: int
    percentage_change: float | None


class ComparisonData(BaseModel):
    differences: List[DimensionDifference]


class TopNPoPResponse(BaseModel):
    current_period: List[PeriodData]
    previous_period: List[PeriodData] | None = None
    comparison: List[ComparisonData] | None = None


class ClickHouseQueryBackend:
    """Singleton backend that holds the ClickHouse connection and config."""

    def __init__(self, client: Any, database: str = 'osprey', table: str = 'osprey_events'):
        self.client = client
        self.database = database
        self.table = table

    @property
    def full_table(self) -> str:
        return f'{self.database}.{self.table}'

    def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a query and return rows as list of dicts."""
        result = self.client.query(sql, parameters=params or {})
        columns = result.column_names
        return [dict(zip(columns, row)) for row in result.result_rows]


def _build_where_clause(
    start: datetime,
    end: datetime,
    query_filter: str,
    entity: Optional[EntityFilter],
    query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = (),
) -> str:
    """Build the full WHERE clause from time range, user query filter, entity, and abilities."""
    # Strip tzinfo — ClickHouse DateTime64(3, 'UTC') rejects +00:00 suffix
    start_str = start.replace(tzinfo=None).isoformat()
    end_str = end.replace(tzinfo=None).isoformat()
    parts = [
        f"`__time` >= '{start_str}'",
        f"`__time` < '{end_str}'",
    ]

    # User query filter
    if query_filter:
        validated_sources = parse_query_to_validated_ast(
            query_filter, rules_sources=ENGINE.instance().execution_graph.validated_sources
        )
        sql_filter = ClickHouseQueryTransformer(validated_sources=validated_sources).transform()
        if sql_filter:
            parts.append(f'({sql_filter})')

    # Entity filter
    if entity:
        parts.append(entity.to_sql_clause())

    # Ability filters (permissions)
    for ability in query_filter_abilities:
        if ability:
            druid_filter = ability._get_query_filter()
            if druid_filter:
                # Convert Druid-format ability filter to SQL
                from osprey.engine.query_language.ast_clickhouse_translator import _druid_filter_to_sql

                parts.append(f'({_druid_filter_to_sql(druid_filter)})')

    return ' AND '.join(parts)


class BaseClickHouseQuery(BaseModel, JsonBodyMarshaller):
    start: datetime
    end: datetime
    query_filter: str
    entity: Optional[EntityFilter] = None


class TimeseriesClickHouseQuery(BaseClickHouseQuery):
    granularity: str
    aggregation_dimensions: Optional[List[str]] = None

    def execute(self, backend: ClickHouseQueryBackend) -> List[Dict[str, Any]]:
        where = _build_where_clause(self.start, self.end, self.query_filter, self.entity)

        granularity_expr = _granularity_to_clickhouse(self.granularity)

        if self.aggregation_dimensions and self.entity:
            # Filtered counts per dimension
            agg_parts = []
            escaped_id = self.entity.id.replace("'", "\\'")
            for dim in self.aggregation_dimensions:
                agg_parts.append(f"countIf(`{dim}` = '{escaped_id}') AS `{dim}`")
            agg_sql = ', '.join(agg_parts)
        else:
            agg_sql = 'count(*) AS `count`'

        sql = f"""
            SELECT {granularity_expr} AS `timestamp`, {agg_sql}
            FROM {backend.full_table}
            WHERE {where}
            GROUP BY `timestamp`
            ORDER BY `timestamp` ASC
        """

        return backend.query(sql)


class GroupByApproximateCountClickHouseQuery(BaseClickHouseQuery):
    dimension: str

    def execute(self, backend: ClickHouseQueryBackend) -> int:
        where = _build_where_clause(self.start, self.end, self.query_filter, self.entity)

        sql = f"""
            SELECT uniqHLL12(`{self.dimension}`) AS `cardinality`
            FROM {backend.full_table}
            WHERE {where}
        """

        rows = backend.query(sql)
        if rows and 'cardinality' in rows[0]:
            return int(rows[0]['cardinality'])
        return -1


class TopNClickHouseQuery(BaseClickHouseQuery):
    dimension: str
    limit: int = 100
    precision: float = 0

    def execute(self, backend: ClickHouseQueryBackend, calculate_previous_period: bool = True) -> TopNPoPResponse:
        current_results = self._execute_single_period(backend, self.start, self.end)

        period_duration = self.end - self.start
        previous_start = self.start - period_duration
        previous_end = self.start

        config = CONFIG.instance()
        max_days = config.get_int('MAX_HISTORICAL_QUERY_WINDOW_DAYS', 90)

        if (
            previous_start.replace(tzinfo=timezone.utc) < (datetime.now(timezone.utc) - timedelta(days=max_days))
            or not calculate_previous_period
        ):
            return TopNPoPResponse(current_period=current_results)

        previous_results = self._execute_single_period(backend, previous_start, previous_end)
        return self._analyze_pop_results(current_results, previous_results)

    def _execute_single_period(
        self, backend: ClickHouseQueryBackend, start: datetime, end: datetime
    ) -> List[PeriodData]:
        where = _build_where_clause(start, end, self.query_filter, self.entity)
        dim_expr = self._get_dimension_expression()

        sql = f"""
            SELECT {dim_expr} AS `dim_value`, count(*) AS `count`
            FROM {backend.full_table}
            WHERE {where}
            GROUP BY `dim_value`
            ORDER BY `count` DESC
            LIMIT {self.limit}
        """

        rows = backend.query(sql)

        result_data = [DimensionData(count=row['count'], **{self.dimension: row['dim_value']}) for row in rows]

        return [PeriodData(timestamp=start, result=result_data)] if result_data else []

    def _get_dimension_expression(self) -> str:
        if self.precision > 0:
            inverse = int(1 / self.precision)
            return f'floor(`{self.dimension}` * {inverse}) / {inverse}'
        return f'`{self.dimension}`'

    def _analyze_pop_results(
        self, current_results: List[PeriodData], previous_results: List[PeriodData]
    ) -> TopNPoPResponse:
        if not previous_results:
            return TopNPoPResponse(current_period=current_results)

        comparison = []
        for current_result, previous_result in zip(current_results, previous_results):
            current_map = {getattr(item, self.dimension, None): item.count for item in current_result.result}
            previous_map = {getattr(item, self.dimension, None): item.count for item in previous_result.result}

            diffs = []
            for key in set(current_map) | set(previous_map):
                curr = current_map.get(key, 0)
                prev = previous_map.get(key, 0)
                if curr == 0:
                    continue
                diff = curr - prev
                pct = (diff / prev * 100) if prev else None
                diffs.append(
                    DimensionDifference(
                        dimension_key=key,
                        current_count=curr,
                        previous_count=prev,
                        difference=diff,
                        percentage_change=pct,
                    )
                )
            comparison.append(ComparisonData(differences=diffs))

        return TopNPoPResponse(
            current_period=current_results,
            previous_period=previous_results,
            comparison=comparison,
        )


class PaginatedScanClickHouseQuery(BaseClickHouseQuery):
    limit: int = 100
    next_page: Optional[str] = None
    order: Ordering = Ordering.DESCENDING

    def execute(
        self,
        backend: ClickHouseQueryBackend,
        query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = (),
    ) -> PaginatedScanResult:
        paginated_limit = self.limit + 1
        start = self.start
        end = self.end

        if self.next_page:
            date_ms = int(base64.b64decode(self.next_page.encode('utf-8')))
            pagination_dt = datetime.fromtimestamp(date_ms // 1000, tz=timezone.utc)
            if self.order == Ordering.ASCENDING:
                start = pagination_dt
            else:
                end = pagination_dt

        where = _build_where_clause(start, end, self.query_filter, self.entity, query_filter_abilities)
        order_dir = 'ASC' if self.order == Ordering.ASCENDING else 'DESC'

        sql = f"""
            SELECT `__action_id`, `__time`
            FROM {backend.full_table}
            WHERE {where}
            ORDER BY `__time` {order_dir}
            LIMIT {paginated_limit}
        """

        rows = backend.query(sql)
        if not rows:
            return PaginatedScanResult(action_ids=[], next_page=None)

        next_page = None
        if len(rows) == paginated_limit:
            last_row = rows.pop()
            ts = last_row['__time']
            if isinstance(ts, datetime):
                ts_ms = int(ts.timestamp() * 1000)
            else:
                ts_ms = int(ts)
            next_page = base64.b64encode(str(ts_ms).encode('utf-8')).decode('utf-8')

        action_ids = [int(row['__action_id']) for row in rows]
        return PaginatedScanResult(action_ids=action_ids, next_page=next_page)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GRANULARITY_MAP = {
    'minute': 'toStartOfMinute(`__time`)',
    'fifteen_minute': 'toStartOfFifteenMinutes(`__time`)',
    'hour': 'toStartOfHour(`__time`)',
    'day': 'toStartOfDay(`__time`)',
    'week': 'toStartOfWeek(`__time`)',
    'month': 'toStartOfMonth(`__time`)',
    'all': "'all'",
}


def _granularity_to_clickhouse(granularity: str) -> str:
    expr = _GRANULARITY_MAP.get(granularity)
    if expr:
        return expr
    # Fallback: treat as interval duration string
    return f'toStartOfInterval(`__time`, INTERVAL 1 {granularity})'


def parse_query_filter(query_filter: str) -> Optional[str]:
    """Parse and validate a query filter string, returning a ClickHouse SQL WHERE clause."""
    if query_filter == '':
        return None

    validated_sources = parse_query_to_validated_ast(
        query_filter, rules_sources=ENGINE.instance().execution_graph.validated_sources
    )
    return ClickHouseQueryTransformer(validated_sources=validated_sources).transform()
