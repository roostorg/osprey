from __future__ import annotations

import base64
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, get_args, get_origin

from osprey.engine import shared_constants
from osprey.engine.query_language.filter_ir import (
    ArrayContainsFilter,
    BooleanFilter,
    ComparisonFilter,
    ComparisonOperator,
    ContainsFilter,
    DruidRawFilter,
    FeatureRef,
    FilterExpression,
    InFilter,
    LikeFilter,
    LiteralValue,
    NotFilter,
    RegexFilter,
)
from osprey.worker.lib.singletons import ENGINE

from ..singletons import CLICKHOUSE
from .event_queries import (
    GroupByApproximateCountDruidQuery,
    Ordering,
    PaginatedScanDruidQuery,
    PaginatedScanResult,
    PeriodData,
    TimeseriesDruidQuery,
    TopNDruidQuery,
    TopNPoPResponse,
)
from .event_query_backend import EventQueryBackend

if TYPE_CHECKING:
    from .abilities import QueryFilterAbility

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ClickHouseFeatureExpression:
    sql: str
    raw_sql: str
    is_array: bool = False
    value_type: type | None = None


class ClickHouseSqlBuilder:
    def __init__(self) -> None:
        self._param_counter = 0
        self.params: Dict[str, Any] = {}

    def add_param(self, value: Any, type_name: Optional[str] = None) -> str:
        param_name = f'param_{self._param_counter}'
        self._param_counter += 1
        self.params[param_name] = value
        return f'{{{param_name}:{type_name or infer_clickhouse_param_type(value)}}}'


class ClickHouseFeatureMapper:
    def __init__(self, builder: ClickHouseSqlBuilder):
        self._builder = builder
        engine = ENGINE.instance()
        self._feature_types = engine.get_post_execution_feature_name_to_value_type_mapping()
        self._allowed_feature_names = (
            frozenset(self._feature_types)
            | frozenset(engine.get_feature_name_to_entity_type_mapping())
            | {
                'timestamp',
                '__timestamp',
                'action_id',
                '__action_id',
                'ActionName',
                shared_constants.VERDICT_DIMENSION_NAME,
                shared_constants.ENTITY_LABEL_MUTATION_DIMENSION_NAME,
            }
        )

    def validate_feature_name(self, name: str) -> str:
        if name not in self._allowed_feature_names:
            raise ValueError(f'Unknown feature name for ClickHouse query rendering: {name}')
        return name

    def feature_expression(self, feature: FeatureRef, force_array: bool = False) -> ClickHouseFeatureExpression:
        name = self.validate_feature_name(feature.name)
        if name in ('timestamp', '__timestamp'):
            return ClickHouseFeatureExpression(sql='timestamp', raw_sql='timestamp', value_type=datetime)
        elif name in ('action_id', '__action_id'):
            return ClickHouseFeatureExpression(sql='action_id', raw_sql='action_id', value_type=int)
        elif name == 'ActionName':
            return ClickHouseFeatureExpression(sql='action_name', raw_sql='action_name', value_type=str)

        name_param = self._builder.add_param(name, 'String')
        raw_sql = f"nullIf(JSONExtractRaw(raw_features, {name_param}), '')"
        value_type = self._feature_types.get(name)
        origin = get_origin(value_type)

        if force_array or name in (
            shared_constants.VERDICT_DIMENSION_NAME,
            shared_constants.ENTITY_LABEL_MUTATION_DIMENSION_NAME,
        ):
            return ClickHouseFeatureExpression(
                sql=f"JSONExtract(raw_features, {name_param}, 'Array(String)')",
                raw_sql=raw_sql,
                is_array=True,
                value_type=list,
            )
        elif origin is list:
            return ClickHouseFeatureExpression(
                sql=f"JSONExtract(raw_features, {name_param}, '{clickhouse_array_type(value_type)}')",
                raw_sql=raw_sql,
                is_array=True,
                value_type=list,
            )
        elif value_type is bool:
            return ClickHouseFeatureExpression(
                sql=f'JSONExtractBool(raw_features, {name_param})',
                raw_sql=raw_sql,
                value_type=bool,
            )
        elif value_type is int:
            return ClickHouseFeatureExpression(
                sql=f'JSONExtractInt(raw_features, {name_param})',
                raw_sql=raw_sql,
                value_type=int,
            )
        elif value_type is float:
            return ClickHouseFeatureExpression(
                sql=f'JSONExtractFloat(raw_features, {name_param})',
                raw_sql=raw_sql,
                value_type=float,
            )

        return ClickHouseFeatureExpression(
            sql=f'JSONExtractString(raw_features, {name_param})',
            raw_sql=raw_sql,
            value_type=str,
        )


class ClickHouseFilterTranslator:
    def __init__(self, builder: ClickHouseSqlBuilder, feature_mapper: ClickHouseFeatureMapper | None = None):
        self._builder = builder
        self._feature_mapper = feature_mapper or ClickHouseFeatureMapper(builder)

    def transform(self, filter_ir: FilterExpression) -> str:
        if isinstance(filter_ir, DruidRawFilter):
            raise NotImplementedError('ClickHouse event queries require query UDFs to implement to_filter_ir()')
        elif isinstance(filter_ir, BooleanFilter):
            return self._transform_boolean(filter_ir)
        elif isinstance(filter_ir, NotFilter):
            return f'NOT ({self.transform(filter_ir.field)})'
        elif isinstance(filter_ir, ComparisonFilter):
            return self._transform_comparison(filter_ir)
        elif isinstance(filter_ir, ContainsFilter):
            feature = self._feature_mapper.feature_expression(filter_ir.feature)
            param = self._builder.add_param(str(filter_ir.value.value), 'String')
            if feature.is_array:
                return f'has({feature.sql}, {param})'
            function = 'position' if filter_ir.case_sensitive else 'positionCaseInsensitive'
            return f'{function}(toString({feature.sql}), {param}) > 0'
        elif isinstance(filter_ir, InFilter):
            if not filter_ir.values:
                return '0'
            feature = self._feature_mapper.feature_expression(filter_ir.feature)
            param = self._builder.add_param(list(filter_ir.values), infer_clickhouse_array_param_type(filter_ir.values))
            return f'has({param}, {feature.sql})'
        elif isinstance(filter_ir, RegexFilter):
            feature = self._feature_mapper.feature_expression(filter_ir.feature)
            param = self._builder.add_param(filter_ir.pattern, 'String')
            return f'match(toString({feature.sql}), {param})'
        elif isinstance(filter_ir, LikeFilter):
            feature = self._feature_mapper.feature_expression(filter_ir.feature)
            param = self._builder.add_param(filter_ir.pattern, 'String')
            return f'toString({feature.raw_sql}) LIKE {param}'
        elif isinstance(filter_ir, ArrayContainsFilter):
            feature = self._feature_mapper.feature_expression(filter_ir.feature, force_array=True)
            param = self._builder.add_param(filter_ir.value.value)
            if feature.is_array:
                return f'has({feature.sql}, {param})'
            return f'positionCaseInsensitive(toString({feature.raw_sql}), {param}) > 0'

        raise NotImplementedError(f'Unknown filter IR type: {filter_ir.__class__.__name__}')

    def _transform_boolean(self, filter_ir: BooleanFilter) -> str:
        if not filter_ir.fields:
            return '1' if filter_ir.operator.value == 'and' else '0'

        joiner = f' {filter_ir.operator.value.upper()} '
        return '(' + joiner.join(self.transform(field) for field in filter_ir.fields) + ')'

    def _transform_comparison(self, filter_ir: ComparisonFilter) -> str:
        operator_sql = get_clickhouse_comparison_operator(filter_ir.operator)

        if isinstance(filter_ir.left, FeatureRef) and isinstance(filter_ir.right, FeatureRef):
            left = self._feature_mapper.feature_expression(filter_ir.left)
            right = self._feature_mapper.feature_expression(filter_ir.right)
            return f'{left.sql} {operator_sql} {right.sql}'

        if isinstance(filter_ir.left, FeatureRef) and isinstance(filter_ir.right, LiteralValue):
            return self._transform_feature_literal(
                feature=filter_ir.left,
                operator=filter_ir.operator,
                literal=filter_ir.right,
            )
        elif isinstance(filter_ir.left, LiteralValue) and isinstance(filter_ir.right, FeatureRef):
            return self._transform_literal_feature(
                literal=filter_ir.left,
                operator=filter_ir.operator,
                feature=filter_ir.right,
            )

        raise NotImplementedError('Comparison must contain one feature and one literal, or two features')

    def _transform_feature_literal(
        self, feature: FeatureRef, operator: ComparisonOperator, literal: LiteralValue
    ) -> str:
        feature_expr = self._feature_mapper.feature_expression(feature)
        if literal.value is None:
            if operator == ComparisonOperator.EQUALS:
                return f'{feature_expr.raw_sql} IS NULL'
            elif operator == ComparisonOperator.NOT_EQUALS:
                return f'{feature_expr.raw_sql} IS NOT NULL'

        param = self._builder.add_param(literal.value)
        return f'{feature_expr.sql} {get_clickhouse_comparison_operator(operator)} {param}'

    def _transform_literal_feature(
        self, literal: LiteralValue, operator: ComparisonOperator, feature: FeatureRef
    ) -> str:
        feature_expr = self._feature_mapper.feature_expression(feature)
        if literal.value is None:
            if operator == ComparisonOperator.EQUALS:
                return f'{feature_expr.raw_sql} IS NULL'
            elif operator == ComparisonOperator.NOT_EQUALS:
                return f'{feature_expr.raw_sql} IS NOT NULL'

        param = self._builder.add_param(literal.value)
        return f'{param} {get_clickhouse_comparison_operator(operator)} {feature_expr.sql}'


class ClickHouseEventQueryBackend(EventQueryBackend):
    def __init__(self) -> None:
        holder = CLICKHOUSE.instance()
        self._client = holder.client
        self._table_name = f'{quote_identifier(holder.database)}.{quote_identifier(holder.table)}'
        self._timeout_seconds = holder.timeout_seconds

    def timeseries(
        self,
        query: TimeseriesDruidQuery,
        query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = (),
    ) -> Any:
        builder = ClickHouseSqlBuilder()
        feature_mapper = ClickHouseFeatureMapper(builder)
        bucket = get_time_bucket_sql(query.granularity)
        where_conds = self._get_where_conds(query=query, builder=builder, query_filter_abilities=query_filter_abilities)

        if query.aggregation_dimensions and query.entity:
            select_parts = [f'{bucket} AS timestamp']
            for dim in query.aggregation_dimensions:
                feature_mapper.validate_feature_name(dim)
                feature = feature_mapper.feature_expression(FeatureRef(dim))
                entity_id = builder.add_param(query.entity.id, 'String')
                select_parts.append(f'countIf(toString({feature.sql}) = {entity_id}) AS {quote_identifier(dim)}')
            select_clause = ', '.join(select_parts)
        else:
            select_clause = f'{bucket} AS timestamp, count() AS count'

        sql = self._build_base_query(
            select_clause=select_clause,
            where_conds=where_conds,
            group_by='timestamp',
            order_by='timestamp',
        )

        rows = self._execute_query(sql, builder.params)
        return [
            {'timestamp': serialize_timestamp(row.get('timestamp')), 'result': strip_timestamp(row)} for row in rows
        ]

    def groupby_approximate_count(self, query: GroupByApproximateCountDruidQuery, **kwargs: Any) -> int:
        builder = ClickHouseSqlBuilder()
        feature_mapper = ClickHouseFeatureMapper(builder)
        feature_mapper.validate_feature_name(query.dimension)
        feature = feature_mapper.feature_expression(FeatureRef(query.dimension))
        where_conds = self._get_where_conds(query=query, builder=builder, **kwargs)
        sql = self._build_base_query(
            select_clause=f'uniqCombined64({feature.sql}) AS cardinality',
            where_conds=where_conds,
        )

        rows = self._execute_query(sql, builder.params)
        if rows and 'cardinality' in rows[0]:
            return int(rows[0]['cardinality'])
        return -1

    def topn(self, query: TopNDruidQuery, **kwargs: Any) -> TopNPoPResponse:
        calculate_previous_period = kwargs.pop('calculate_previous_period', True)

        current_results = self._execute_topn_single_period(query, query.start, query.end, **kwargs)
        sanitized_current_results = self._sanitize_topn_results(query, current_results, query.end)

        previous_period = self.get_previous_period(query, calculate_previous_period=calculate_previous_period)
        if previous_period is None:
            return TopNPoPResponse(current_period=sanitized_current_results)

        previous_start, previous_end = previous_period
        previous_results = self._execute_topn_single_period(query, previous_start, previous_end, **kwargs)
        sanitized_previous_results = self._sanitize_topn_results(query, previous_results, previous_end)

        return self.build_topn_pop_response(query, sanitized_current_results, sanitized_previous_results)

    def scan(
        self,
        query: PaginatedScanDruidQuery,
        query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = (),
    ) -> PaginatedScanResult:
        builder = ClickHouseSqlBuilder()
        paginated_limit = query.limit + 1
        where_conds = self._get_where_conds(
            query=query,
            builder=builder,
            query_filter_abilities=query_filter_abilities,
        )

        if query.next_page:
            date_in_milliseconds = int(base64.b64decode(query.next_page.encode('utf-8')))
            pagination_datetime = datetime.fromtimestamp(date_in_milliseconds / 1000, tz=timezone.utc)
            page_cursor = builder.add_param(pagination_datetime, "DateTime64(3, 'UTC')")
            if query.order == Ordering.ASCENDING:
                where_conds.append(f'timestamp >= {page_cursor}')
            elif query.order == Ordering.DESCENDING:
                where_conds.append(f'timestamp < {page_cursor}')
            else:
                raise ValueError(f'Unhandled order: {query.order}')

        order_dir = 'ASC' if query.order == Ordering.ASCENDING else 'DESC'
        limit = builder.add_param(paginated_limit, 'UInt64')

        sql = self._build_base_query(
            select_clause='action_id, timestamp',
            where_conds=where_conds,
            order_by=f'timestamp {order_dir}',
            limit=limit,
        )

        rows = self._execute_query(sql, builder.params)
        next_page = None

        if len(rows) == paginated_limit:
            last_row = rows.pop()
            timestamp_ms = int(to_datetime(last_row['timestamp']).timestamp() * 1000)
            timestamp_string = str(timestamp_ms).encode('utf-8')
            next_page = base64.b64encode(timestamp_string).decode('utf-8')

        return PaginatedScanResult(action_ids=[int(row['action_id']) for row in rows], next_page=next_page)

    def _execute_topn_single_period(
        self, query: TopNDruidQuery, start: datetime, end: datetime, **kwargs: Any
    ) -> List[Dict[str, Any]]:
        builder = ClickHouseSqlBuilder()
        feature_mapper = ClickHouseFeatureMapper(builder)
        feature_mapper.validate_feature_name(query.dimension)
        feature = feature_mapper.feature_expression(FeatureRef(query.dimension))
        where_conds = self._get_where_conds(query=query, builder=builder, start=start, end=end, **kwargs)
        limit = builder.add_param(query.limit, 'UInt64')

        sql = self._build_base_query(
            select_clause=f'{feature.sql} AS {quote_identifier(query.dimension)}, count() AS count',
            where_conds=where_conds,
            group_by=quote_identifier(query.dimension),
            order_by='count DESC',
            limit=limit,
        )

        return self._execute_query(sql, builder.params)

    def _get_where_conds(
        self,
        query: Any,
        builder: ClickHouseSqlBuilder,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = (),
        **_: Any,
    ) -> List[str]:
        start = start or query.start
        end = end or query.end

        if start > end:
            raise ValueError('Start date must be before end date')

        start_param = builder.add_param(start, "DateTime64(3, 'UTC')")
        end_param = builder.add_param(end, "DateTime64(3, 'UTC')")
        conds = [f'timestamp >= {start_param}', f'timestamp < {end_param}']

        filter_ir = query._get_combined_filter_ir(query_filter_abilities=query_filter_abilities)
        if filter_ir is not None:
            feature_mapper = ClickHouseFeatureMapper(builder)
            conds.append(ClickHouseFilterTranslator(builder, feature_mapper=feature_mapper).transform(filter_ir))

        return conds

    def _build_base_query(
        self,
        select_clause: str,
        where_conds: Sequence[str],
        group_by: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[str] = None,
    ) -> str:
        parts = [f'SELECT {select_clause}', f'FROM {self._table_name}']

        if where_conds:
            parts.append(f'WHERE {" AND ".join(where_conds)}')
        if group_by:
            parts.append(f'GROUP BY {group_by}')
        if order_by:
            parts.append(f'ORDER BY {order_by}')
        if limit:
            parts.append(f'LIMIT {limit}')

        return '\n'.join(parts)

    def _execute_query(self, query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        logger.debug('executing ClickHouse query: %s', query)
        result = self._client.query(
            query,
            parameters=params,
            settings={'max_execution_time': self._timeout_seconds},
        )
        return normalize_clickhouse_rows(result)

    def _sanitize_topn_results(
        self, query: TopNDruidQuery, rows: List[Dict[str, Any]], timestamp: datetime
    ) -> List[PeriodData]:
        result_items = [{'count': row.get('count', 0), query.dimension: row.get(query.dimension)} for row in rows]
        return self.build_period_data(timestamp, result_items)


def get_clickhouse_comparison_operator(operator: ComparisonOperator) -> str:
    return {
        ComparisonOperator.EQUALS: '=',
        ComparisonOperator.NOT_EQUALS: '!=',
        ComparisonOperator.LESS_THAN: '<',
        ComparisonOperator.LESS_THAN_EQUALS: '<=',
        ComparisonOperator.GREATER_THAN: '>',
        ComparisonOperator.GREATER_THAN_EQUALS: '>=',
    }[operator]


def get_time_bucket_sql(granularity: str) -> str:
    grans = {
        'minute': 'toStartOfMinute(timestamp)',
        'hour': 'toStartOfHour(timestamp)',
        'day': 'toStartOfDay(timestamp)',
        'week': 'toStartOfWeek(timestamp)',
    }
    return grans.get(granularity, 'toStartOfHour(timestamp)')


def normalize_clickhouse_rows(result: Any) -> List[Dict[str, Any]]:
    if hasattr(result, 'named_results'):
        return [dict(row) for row in result.named_results()]
    elif hasattr(result, 'result_rows') and hasattr(result, 'column_names'):
        return [dict(zip(result.column_names, row)) for row in result.result_rows]
    elif isinstance(result, list):
        return [dict(row) for row in result]
    return []


def strip_timestamp(row: Dict[str, Any]) -> Dict[str, Any]:
    copied = dict(row)
    copied.pop('timestamp', None)
    return copied


def serialize_timestamp(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return value


def to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise TypeError(f'Expected datetime-compatible value, got {type(value)}')


def quote_identifier(value: str) -> str:
    return '`' + value.replace('`', '``') + '`'


def infer_clickhouse_param_type(value: Any) -> str:
    if isinstance(value, bool):
        return 'Bool'
    elif isinstance(value, int):
        return 'Int64'
    elif isinstance(value, float):
        return 'Float64'
    elif isinstance(value, datetime):
        return "DateTime64(3, 'UTC')"
    elif isinstance(value, list):
        return infer_clickhouse_array_param_type(tuple(value))
    return 'String'


def infer_clickhouse_array_param_type(values: Sequence[Any]) -> str:
    if not values:
        return 'Array(String)'
    first = values[0]
    if isinstance(first, bool):
        return 'Array(Bool)'
    elif isinstance(first, int):
        return 'Array(Int64)'
    elif isinstance(first, float):
        return 'Array(Float64)'
    return 'Array(String)'


def clickhouse_array_type(value_type: Any) -> str:
    args = get_args(value_type)
    element_type = args[0] if args else str
    if element_type is bool:
        return 'Array(Bool)'
    elif element_type is int:
        return 'Array(Int64)'
    elif element_type is float:
        return 'Array(Float64)'
    return 'Array(String)'
