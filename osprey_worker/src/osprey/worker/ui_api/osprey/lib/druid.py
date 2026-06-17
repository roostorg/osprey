import base64
import json
import logging
import math
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

from osprey.engine.query_language import parse_query_to_validated_ast
from osprey.engine.query_language.ast_druid_translator import DruidQueryTransformer
from osprey.worker.lib.singletons import CONFIG, ENGINE
from pydantic.main import BaseModel
from pydruid.query import QueryBuilder
from pydruid.utils.aggregators import count, filtered
from pydruid.utils.filters import Dimension

from ..singletons import DRUID
from .marshal import JsonBodyMarshaller

if TYPE_CHECKING:
    from .abilities import QueryFilterAbility

logger = logging.getLogger(__name__)

# Query timeout in miiliseconds
DEFAULT_DRUID_QUERY_TIMEOUT = 300000


class DruidQueryTypes(str, Enum):
    TOP_N = 'topN'
    TIMESERIES = 'timeseries'
    SCAN = 'scan'
    GROUP_BY = 'groupBy'


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

    def wrap_filter(self, query_filter: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        feature_to_entity_mapping = ENGINE.instance().get_feature_name_to_entity_type_mapping()
        filters = (
            feature_name
            for feature_name, entity_type in feature_to_entity_mapping.items()
            if entity_type == self.type and (not self.feature_filters or feature_name in self.feature_filters)
        )

        entity_filters = {
            'type': 'or',
            'fields': [{'type': 'selector', 'dimension': feature_name, 'value': self.id} for feature_name in filters],
        }

        if query_filter:
            return {'type': 'and', 'fields': [query_filter['filter'], entity_filters]}

        return entity_filters


class BaseDruidQuery(BaseModel, JsonBodyMarshaller):
    start: datetime
    end: datetime
    query_filter: str
    entity: Optional[EntityFilter]

    def _query_with_filter(
        self,
        query_type: DruidQueryTypes,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        order: Optional[Ordering] = None,
        query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = (),
        **kwargs: Any,
    ) -> Any:
        transformed_filter = parse_query_filter(self.query_filter)
        start = start or self.start
        end = end or self.end

        if start > end:
            raise ValueError('Start date must be before end date')

        if order is not None:
            kwargs['order'] = order.value

        # Set default timeout
        context = kwargs.setdefault('context', {})
        context.setdefault('timeout', DEFAULT_DRUID_QUERY_TIMEOUT)

        druid = DRUID.instance()
        built_query = QueryBuilder().build_query(
            query_type.value,
            {'datasource': druid.datasource, 'intervals': f'{start.isoformat()}/{end.isoformat()}', **kwargs},
        )

        if transformed_filter or self.entity:
            assert built_query.query_dict is not None

            if self.entity:
                built_query.query_dict['filter'] = self.entity.wrap_filter(transformed_filter)
            elif transformed_filter:
                built_query.query_dict['filter'] = transformed_filter['filter']

        # Query filtering
        # Before the query is sent to Druid, we want to apply any Druid filters that will
        # simplify the results we get to not include things that the queryer does not have
        # access to.
        if query_filter_abilities:
            # if all query_filter_abilities have allow_all, then we don't need to create a permissions filter
            ability_filters = list(
                filter(None, [ability._get_query_filter() for ability in query_filter_abilities if ability])
            )

            if ability_filters:
                combined_ability_filter = {
                    'type': 'and',
                    'fields': ability_filters,
                }

                # if we already have a user query filter, we'll combine the permissions into the existing query filter
                assert built_query.query_dict is not None
                user_query_filter = built_query.query_dict.get('filter')
                if user_query_filter:
                    built_query.query_dict['filter'] = {
                        'type': 'and',
                        'fields': [user_query_filter, combined_ability_filter],
                    }

                # otherwise if there isn't a user query filter, we'll only filter the query using the abilities
                else:
                    built_query.query_dict['filter'] = combined_ability_filter

        result = druid.client._post(built_query)
        assert result.result_json
        return json.loads(result.result_json)


class TimeseriesDruidQuery(BaseDruidQuery):
    granularity: str
    aggregation_dimensions: Optional[List[str]] = None

    def execute(self) -> Any:
        aggregations = {'count': {'type': 'count'}}

        if self.aggregation_dimensions and self.entity:
            aggregations = {
                dimension: filtered(Dimension(dimension) == self.entity.id, count('count'))
                for dimension in self.aggregation_dimensions
            }

        return self._query_with_filter(
            DruidQueryTypes.TIMESERIES,
            granularity=self.granularity,
            aggregations=aggregations,
        )


class GroupByApproximateCountDruidQuery(BaseDruidQuery):
    """
    Returns an approximate count of a groupBy query's unique dimension values (i.e. number of unique UserIds).
    This is done using a cardinality aggregator.
    """

    dimension: str

    def execute(self, **kwargs: Any) -> int:
        query_result = self._query_with_filter(
            DruidQueryTypes.GROUP_BY,
            aggregations={
                'cardinality': {'type': 'cardinality', 'name': 'count', 'fieldNames': [self.dimension], 'round': 'true'}
            },
            granularity='all',
            **kwargs,
        )
        if isinstance(query_result, list):
            query_result = query_result[0]
        if query_result.get('event') is not None:
            query_result = query_result['event']
        if query_result.get('cardinality') is not None:
            return int(query_result['cardinality'])
        return -1


class DimensionData(BaseModel):
    count: int

    # e.g. 'ActionName': 'dm_channel_created'; this is allowed due to the original query's return type.
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


class TopNDruidQuery(BaseDruidQuery):
    dimension: str
    limit: int = 100
    precision: float = 0

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

    def execute(self, **kwargs: Any) -> TopNPoPResponse:
        calculate_previous_period = kwargs.pop('calculate_previous_period', True)

        current_results = self._execute_single_period(start=self.start, end=self.end, **kwargs)
        sanitized_current_results = self._sanitize_results(current_results)

        period_duration = self.end - self.start
        previous_start = self.start - period_duration
        previous_end = self.start

        config = CONFIG.instance()
        max_historical_query_window_days = config.get_int('MAX_HISTORICAL_QUERY_WINDOW_DAYS', 90)

        if (
            previous_start.replace(tzinfo=timezone.utc)
            < (datetime.now(timezone.utc) - timedelta(days=max_historical_query_window_days))
            or not calculate_previous_period
        ):
            return TopNPoPResponse(
                current_period=sanitized_current_results,
            )

        previous_results = self._execute_single_period(start=previous_start, end=previous_end, **kwargs)
        sanitized_previous_results = self._sanitize_results(previous_results)

        pop_results = self._analyze_pop_results(sanitized_current_results, sanitized_previous_results)

        return pop_results

    def _sanitize_results(self, results: List[Dict[str, Any]]) -> List[PeriodData]:
        """
        Sanitizes raw Druid query results into PeriodData objects.

        Args:
            results: Raw results from Druid query containing timestamp and dimension results

        Returns:
            List of PeriodData objects with properly formatted timestamps and dimension data
        """
        sanitized_results = []
        for result in results:
            try:
                timestamp = (
                    result['timestamp'].isoformat()
                    if hasattr(result['timestamp'], 'isoformat')
                    else result['timestamp']
                )

                dimension_data = []
                for item in result['result']:
                    try:
                        dimension_data.append(DimensionData(**item))
                    except Exception as e:
                        logger.error(f'Failed to parse dimension data: {item}, error: {e}')
                        continue

                # Append all instances of dimensional analysis data, skip if empty
                if dimension_data:
                    period_data = PeriodData(timestamp=timestamp, result=dimension_data)
                    sanitized_results.append(period_data)
            except Exception as e:
                logger.error(f'Failed to parse result: {result}, error: {e}')
                continue

        return sanitized_results

    def _analyze_pop_results(
        self, current_results: List[PeriodData], previous_results: List[PeriodData]
    ) -> TopNPoPResponse:
        # Extract the list of rows from the first (and only) element of each results list.
        current_data = current_results if current_results else []
        previous_data = previous_results if previous_results else []

        if len(previous_data) == 0:
            return TopNPoPResponse(
                current_period=current_results,
            )

        dimension_key = self.dimension
        comparison = []

        for current_result, previous_result in zip(current_data, previous_data):
            current_map = {getattr(item, dimension_key): item.count for item in current_result.result}
            previous_map = {getattr(item, dimension_key): item.count for item in previous_result.result}
            dimension_differences = []

            all_keys = set(current_map.keys()) | set(previous_map.keys())

            for item in all_keys:
                current_count = current_map.get(item, 0)
                previous_count = previous_map.get(item, 0)

                # Filter TopN items which do not have any values in the current time period
                # Especially important for bulk label operations
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

    def _execute_single_period(
        self, start: Optional[datetime] = None, end: Optional[datetime] = None, **kwargs: Any
    ) -> List[Dict[str, Any]]:
        assert self.precision >= 0 and self.precision < 1, (
            'Precision specified was not valid; Must be a float between 0 and 1!'
        )

        dimension_parameter = self._get_dimension_parameter()

        return self._query_with_filter(
            DruidQueryTypes.TOP_N,
            start=start,
            end=end,
            aggregations={'count': count('count')},
            granularity='all',
            dimension=dimension_parameter,
            metric='count',
            threshold=self.limit,
            **kwargs,
        )

    def _get_dimension_parameter(self) -> Union[str, Dict[str, Any]]:
        dimension_type: Optional[str] = ENGINE.instance().get_feature_name_to_entity_type_mapping().get(self.dimension)
        dimension_parameter: Union[str, Dict[str, Any]] = self.dimension

        # If dimension is not a float type, return as-is
        if (dimension_type is not None and dimension_type.lower() != 'float') or self.precision == 0:
            return dimension_parameter

        # Converts the expected precision value (0.001, for ex) into it's inverse (1000, for ex)
        inverse_precision = int(1 / self.precision)
        # Gets the num of decimal places in the precision (0.001 has a precision of 3 decimal places, for ex).
        # We do this by getting the inverse of the precision (0.001 to 1000) and then using log10 to get the num of
        # digits (log10 of a number returns num_digits - 1)
        precision_length = int(math.log10(inverse_precision))

        # Create extraction dimension parameter for rounding floats
        dimension_parameter = {
            'type': 'extraction',
            'dimension': self.dimension,
            'outputName': self.dimension,
            'outputType': 'STRING',
            'extractionFn': {
                'type': 'javascript',
                'function': (
                    'function(x) {'
                    'if (x === null || isNaN(x)) return x; '
                    'return "~" + '
                    f'(Math.floor(x * {inverse_precision})/{inverse_precision}).toFixed({precision_length})'
                    '}'
                ),
            },
        }

        return dimension_parameter


class PaginatedScanDruidQuery(BaseDruidQuery):
    limit: int = 100
    next_page: Optional[str] = None
    order: Ordering = Ordering.DESCENDING

    def execute(
        self, query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = ()
    ) -> PaginatedScanResult:
        paginated_limit = self.limit + 1
        kwargs: Dict[str, Any] = {'resultFormat': 'compactedList'}

        if self.next_page:
            date_in_milliseconds = int(base64.b64decode(self.next_page.encode('utf-8')))
            pagination_datetime = datetime.fromtimestamp(date_in_milliseconds // 1000, tz=timezone.utc)

            if self.order == Ordering.ASCENDING:
                kwargs['start'] = pagination_datetime
            elif self.order == Ordering.DESCENDING:
                kwargs['end'] = pagination_datetime
            else:
                raise ValueError(f'Unhandled order: {self.order}')

        results = self._query_with_filter(
            DruidQueryTypes.SCAN,
            limit=paginated_limit,
            order=self.order,
            columns=['__action_id', '__time'],
            query_filter_abilities=query_filter_abilities,
            **kwargs,
        )

        if not results:
            return PaginatedScanResult(action_ids=[], next_page=None)

        events: List[Any] = []
        for result in results:
            events += result['events']

        next_page = None

        if len(events) == paginated_limit:
            [_, timestamp] = events.pop()
            timestamp_string = str(timestamp).encode('utf-8')
            next_page = base64.b64encode(timestamp_string).decode('utf-8')

        action_ids = [int(action_id) for [action_id, _] in events]
        return PaginatedScanResult(action_ids=action_ids, next_page=next_page)


def parse_query_filter(query_filter: str) -> Optional[Dict[str, Any]]:
    if query_filter == '':
        return None

    validated_sources = parse_query_to_validated_ast(
        query_filter, rules_sources=ENGINE.instance().execution_graph.validated_sources
    )
    return DruidQueryTransformer(validated_sources=validated_sources).transform()
