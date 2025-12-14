import base64
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

from clickhouse_connect.driver.client import Client
from osprey.engine.query_language import parse_query_to_validated_ast
from osprey.engine.query_language.ast_clickhouse_translator import ClickhouseTranslator
from osprey.worker.lib.singletons import CONFIG, ENGINE
from osprey.worker.ui_api.osprey.lib.druid import Ordering, PaginatedScanResult
from osprey.worker.ui_api.osprey.lib.marshal import JsonBodyMarshaller
from osprey.worker.ui_api.osprey.singletons import CLICKHOUSE
from pydantic import BaseModel

if TYPE_CHECKING:
    from .abilities import QueryFilterAbility


logger = logging.getLogger('clickhouse')

# query timeout in seconds
DEFAULT_CLICKHOUSE_TIMEOUT = 30


class EntityFilter(BaseModel):
    id: str
    type: str
    feature_filters: Optional[List[str]]

    def to_sql_filter(self) -> str:
        """Convert entity filter to a SQL WHERE clause"""
        feature_to_entity_mapping = ENGINE.instance().get_feature_name_to_entity_type_mapping()
        filters = [
            feature_name
            for feature_name, entity_type in feature_to_entity_mapping.items()
            if entity_type == self.type and (not self.feature_filters or feature_name in self.feature_filters)
        ]

        if not filters:
            return '1=1'

        # cast all feature columns to STRING to match {entity_id} parameter type
        conds = [f'toString({feature_name}) = {{entity_id: String}}' for feature_name in filters]
        return f'({" OR ".join(conds)})'


class BaseClickhouseQuery(BaseModel, JsonBodyMarshaller):
    start: datetime
    end: datetime
    query_filter: str
    entity_filter: Optional[EntityFilter]

    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True

    @property
    def _client(self) -> Client:
        return CLICKHOUSE.instance().client

    @property
    def _database(self) -> str:
        return CLICKHOUSE.instance().database

    @property
    def _table(self) -> str:
        return CLICKHOUSE.instance().table

    def _build_base_query(
        self,
        select_clause: str,
        where_conds: List[str],
        group_by: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> str:
        """Creates a SQL query from supplied parts"""
        parts = [f'SELECT {select_clause}', f'FROM {self._database}.{self._table}']

        if where_conds:
            parts.append(f'WHERE {" AND ".join(where_conds)}')

        if group_by:
            parts.append(f'GROUP BY {group_by}')

        if order_by:
            parts.append(f'ORDER BY {order_by}')

        if limit:
            parts.append(f'LIMIT {limit}')

        return '\n'.join(parts)

    def _execute_query(
        self,
        query: str,
        query_params: Optional[Dict[str, Any]] = None,
        query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = (),
    ) -> List[Dict[str, Any]]:
        """Execute the supplied query in Clickhouse"""
        try:
            result = self._client.query(query, parameters=query_params or {})
            return result.result_rows
        except Exception as e:
            logger.error(f'Clickhouse query failed: {e}')
            raise

    def _get_where_conds(self) -> tuple[List[str], Dict[str, Any]]:
        """Put together the WHERE conditions for a query"""
        # create conditions with the initial timestamp filter
        conds: List[str] = [
            '__timestamp >= {start_time: DateTime} AND __timestamp < {end_time: DateTime}',
        ]
        # add the start and end times to the parameters
        params: Dict[str, Any] = {
            'start_time': self.start,
            'end_time': self.end,
        }

        # add an entity filter if we have one
        if self.entity_filter:
            conds.append(self.entity_filter.to_sql_filter())
            params['entity_id'] = self.entity_filter.id

        if self.query_filter:
            translated_filter = self._parse_query_filter(self.query_filter)
            if translated_filter:
                conds.append(translated_filter['sql'])
                if 'params' in translated_filter:
                    # Merge the params from the translated filter
                    params.update(translated_filter['params'])

        return conds, params

    def _parse_query_filter(self, query_filter: str) -> Optional[Dict[str, Any]]:
        """Parse the query filter into SQL"""
        if not query_filter:
            return None

        validated_sources = parse_query_to_validated_ast(
            query_filter, rules_sources=ENGINE.instance().execution_graph.validated_sources
        )

        return ClickhouseTranslator(validated_sources=validated_sources).transform()


class TimeseriesResultRow(BaseModel):
    """Format for timeseries result rows, to match what Druid produces and the UI expects"""

    timestamp: Any
    result: Dict[str, Any]


class TimeseriesClickhouseQuery(BaseClickhouseQuery):
    granularity: str
    agg_dims: Optional[List[str]] = None

    def _get_time_bucket_sql(self) -> str:
        """Return the SQL granularity for the query's granularity"""
        grans = {
            'minute': 'toStartOfMinute(__timestamp)',
            'hour': 'toStartOfHour(__timestamp)',
            'day': 'toStartOfDay(__timestamp)',
            'week': 'toStartOfWeek(__timestamp)',
            'month': 'toStartOfMonth(__timestamp)',
        }
        return grans.get(self.granularity, 'toStartOfHour(__timestamp)')

    def execute(self) -> List[TimeseriesResultRow]:
        bucket = self._get_time_bucket_sql()

        if self.agg_dims and self.entity_filter:
            agg_selects = [f'countIf(toString({dim}) = {{entity_id}}) AS {dim}' for dim in self.agg_dims]
            select_clause = f'{bucket} AS timestamp, ' + ', '.join(agg_selects)
        else:
            select_clause = f'{bucket} AS timestamp, count() AS count'

        conds, params = self._get_where_conds()

        query = self._build_base_query(
            select_clause=select_clause,
            where_conds=conds,
            group_by='timestamp',
            order_by='timestamp',
        )

        results = self._execute_query(query, params)

        transformed_results: List[TimeseriesResultRow] = []
        for row in results:
            if isinstance(row, dict):
                timestamp = row.pop('timestamp')
                result_data = row
            else:
                timestamp = row[0]
                result_data = {k: v for k, v in zip(['count'], row[1:])}

            transformed_results.append(
                TimeseriesResultRow(
                    timestamp=timestamp,
                    result=result_data,
                )
            )

        return [row.dict() for row in transformed_results]


class PaginatedScanClickhouseQuery(BaseClickhouseQuery):
    limit: int = 100
    next_page: Optional[str] = None
    order: Ordering = Ordering.DESCENDING

    def execute(
        self, query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = ()
    ) -> PaginatedScanResult:
        paginated_limit = self.limit + 1
        conds, params = self._get_where_conds()

        if self.next_page:
            date_in_milliseconds = int(base64.b64decode(self.next_page.encode('utf-8')))
            pagination_datetime = datetime.fromtimestamp(date_in_milliseconds // 1000, tz=timezone.utc)

            if self.order == Ordering.ASCENDING:
                conds.append('__timestamp >= {page_cursor: DateTime}')
            else:
                conds.append('__timestamp < {page_cursor: DateTime}')

            params['page_cursor'] = pagination_datetime

        select_clause = '__action_id, __timestamp'
        order_dir = 'ASC' if self.order == Ordering.ASCENDING else 'DESC'

        query = self._build_base_query(
            select_clause=select_clause,
            where_conds=conds,
            order_by=f'__timestamp {order_dir}',
            limit=paginated_limit,
        )

        results = self._execute_query(query, params, query_filter_abilities)

        if not results:
            return PaginatedScanResult(action_ids=[], next_page=None)

        next_page = None

        if len(results) == paginated_limit:
            last_row = results.pop()
            timestamp_val = last_row['timestamp'] if isinstance(last_row, dict) else last_row[1]
            timestamp_ms = int(timestamp_val.timestamp() * 1000)
            timestamp_string = str(timestamp_ms).encode('utf-8')
            next_page = base64.b64encode(timestamp_string).decode('utf-8')

        action_ids = [int(row['action_id'] if isinstance(row, dict) else row[0]) for row in results]
        return PaginatedScanResult(action_ids=action_ids, next_page=next_page)


class GroupByApproximateCountClickhouseQuery(BaseClickhouseQuery):
    dim: str

    def execute(self) -> int:
        select_clause = f'uniq({self.dim}) AS cardinality'
        conds, params = self._get_where_conds()

        query = self._build_base_query(select_clause=select_clause, where_conds=conds)

        results = self._execute_query(query, params)

        if results:
            result = results[0]
            cardinality = result['cardinality'] if isinstance(result, dict) else result[0]
            return int(cardinality)

        return -1


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
    previous_period: List[PeriodData] | None
    comparison: List[ComparisonData] | None


class TopNClickhouseQuery(BaseClickhouseQuery):
    dimension: str
    limit: int = 100

    def execute(
        self,
        query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = (),
        calculate_previous_period: bool = True,
    ) -> TopNPoPResponse:
        current_results = self._execute_single_period(
            start=self.start, end=self.end, query_filter_abilities=query_filter_abilities
        )
        sanitized_current_results = self._sanitize_results(current_results)

        if not calculate_previous_period:
            return TopNPoPResponse(current_period=sanitized_current_results, previous_period=None, comparison=None)

        period_duration = self.end - self.start
        previous_start = self.start - period_duration
        previous_end = self.start

        config = CONFIG.instance()
        # i wonder if this default is high? seems okay based on the size of the data we are querying...
        max_historical_query_window_days = config.get_int('MAX_HISTORICAL_QUERY_WINDOW_DAYS', 90)

        if previous_start.replace(tzinfo=timezone.utc) < (
            datetime.now(timezone.utc) - timedelta(days=max_historical_query_window_days)
        ):
            return TopNPoPResponse(current_period=sanitized_current_results, previous_period=None, comparison=None)

        previous_results = self._execute_single_period(
            start=previous_start, end=previous_end, query_filter_abilities=query_filter_abilities
        )
        sanitized_previous_results = self._sanitize_results(previous_results)

        pop_results = self._analyze_pop_results(sanitized_current_results, sanitized_previous_results)

        return pop_results

    def _execute_single_period(
        self,
        start: datetime,
        end: datetime,
        query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = (),
    ) -> List[Dict[str, Any]]:
        select_clause = f'{self.dimension}, COUNT(*) AS count'

        original_start, original_end = self.start, self.end
        self.start, self.end = start, end

        try:
            conds, params = self._get_where_conds()

            query = self._build_base_query(
                select_clause=select_clause,
                where_conds=conds,
                group_by=self.dimension,
                order_by='count DESC',
                limit=self.limit,
            )

            results = self._execute_query(query, params, query_filter_abilities)
            return results
        finally:
            self.start, self.end = original_start, original_end

    def _sanitize_results(self, results: List[Dict[str, Any]]) -> List[PeriodData]:
        if not results:
            return []

        dimension_data = []
        for result in results:
            try:
                dimension_value = result.get(self.dimension)
                count = result.get('count', 0)

                data_dict = {'count': count, self.dimension: dimension_value}
                dimension_data.append(DimensionData(**data_dict))
            except Exception as e:
                logger.error(f'Failed to parse result: {result}, error: {e}')
                continue

        if dimension_data:
            return [PeriodData(timestamp=self.end, result=dimension_data)]
        return []

    # slop code that seems to work if i use the pop feature
    def _analyze_pop_results(
        self, current_results: List[PeriodData], previous_results: List[PeriodData]
    ) -> TopNPoPResponse:
        if not previous_results:
            return TopNPoPResponse(current_period=current_results, previous_period=None, comparison=None)

        dimension_key = self.dimension
        comparison = []

        for current_result, previous_result in zip(current_results, previous_results):
            current_map = {getattr(item, dimension_key): item.count for item in current_result.result}
            previous_map = {getattr(item, dimension_key): item.count for item in previous_result.result}
            dimension_differences = []

            all_keys = set(current_map.keys()) | set(previous_map.keys())

            for item in all_keys:
                current_count = current_map.get(item, 0)
                previous_count = previous_map.get(item, 0)

                if current_count == 0:
                    continue

                difference = current_count - previous_count
                pct_change = (difference / previous_count * 100) if previous_count else None

                dimension_differences.append(
                    DimensionDifference(
                        dimension_key=item,
                        current_count=current_count,
                        previous_count=previous_count,
                        difference=difference,
                        percentage_change=pct_change,
                    )
                )

            comparison.append(ComparisonData(differences=dimension_differences))

        return TopNPoPResponse(
            current_period=current_results,
            previous_period=previous_results,
            comparison=comparison,
        )
