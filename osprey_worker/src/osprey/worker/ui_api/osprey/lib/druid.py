import base64
import json
import logging
import math
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

from osprey.engine.query_language import parse_query_to_validated_ast
from osprey.engine.query_language.ast_druid_translator import DruidQueryTransformer
from osprey.engine.query_language.druid_filter_translator import DruidFilterTranslator
from osprey.worker.lib.singletons import ENGINE
from osprey.worker.ui_api.osprey.lib.event_query_backend import EventQueryBackend
from pydruid.query import QueryBuilder
from pydruid.utils.aggregators import count, filtered
from pydruid.utils.filters import Dimension

from ..singletons import DRUID
from .event_queries import (
    BaseDruidQuery,
    ComparisonData,
    DimensionData,
    DimensionDifference,
    EntityFilter,
    GroupByApproximateCountDruidQuery,
    Ordering,
    PaginatedScanDruidQuery,
    PaginatedScanResult,
    PeriodData,
    TimeseriesDruidQuery,
    TopNDruidQuery,
    TopNPoPResponse,
    parse_query_filter_ir,
)

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


class DruidEventQueryBackend(EventQueryBackend):
    def timeseries(
        self,
        query: TimeseriesDruidQuery,
        query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = (),
    ) -> Any:
        aggregations = {'count': {'type': 'count'}}

        if query.aggregation_dimensions and query.entity:
            aggregations = {
                dimension: filtered(Dimension(dimension) == query.entity.id, count('count'))
                for dimension in query.aggregation_dimensions
            }

        return self._query_with_filter(
            query=query,
            query_type=DruidQueryTypes.TIMESERIES,
            granularity=query.granularity,
            aggregations=aggregations,
            query_filter_abilities=query_filter_abilities,
        )

    def groupby_approximate_count(self, query: GroupByApproximateCountDruidQuery, **kwargs: Any) -> int:
        query_result = self._query_with_filter(
            query=query,
            query_type=DruidQueryTypes.GROUP_BY,
            aggregations={
                'cardinality': {
                    'type': 'cardinality',
                    'name': 'count',
                    'fieldNames': [query.dimension],
                    'round': 'true',
                }
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

    def topn(self, query: TopNDruidQuery, **kwargs: Any) -> TopNPoPResponse:
        calculate_previous_period = kwargs.pop('calculate_previous_period', True)

        current_results = self._execute_topn_single_period(query, start=query.start, end=query.end, **kwargs)
        sanitized_current_results = self._sanitize_topn_results(current_results)

        previous_period = self.get_previous_period(query, calculate_previous_period=calculate_previous_period)
        if previous_period is None:
            return TopNPoPResponse(current_period=sanitized_current_results)

        previous_start, previous_end = previous_period
        previous_results = self._execute_topn_single_period(query, start=previous_start, end=previous_end, **kwargs)
        sanitized_previous_results = self._sanitize_topn_results(previous_results)

        return self.build_topn_pop_response(query, sanitized_current_results, sanitized_previous_results)

    def scan(
        self,
        query: PaginatedScanDruidQuery,
        query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = (),
    ) -> PaginatedScanResult:
        paginated_limit = query.limit + 1
        kwargs: Dict[str, Any] = {'resultFormat': 'compactedList'}

        if query.next_page:
            date_in_milliseconds = int(base64.b64decode(query.next_page.encode('utf-8')))
            pagination_datetime = datetime.fromtimestamp(date_in_milliseconds / 1000, tz=timezone.utc)

            if query.order == Ordering.ASCENDING:
                kwargs['start'] = pagination_datetime
            elif query.order == Ordering.DESCENDING:
                kwargs['end'] = pagination_datetime
            else:
                raise ValueError(f'Unhandled order: {query.order}')

        results = self._query_with_filter(
            query=query,
            query_type=DruidQueryTypes.SCAN,
            limit=paginated_limit,
            order=query.order,
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

    def _query_with_filter(
        self,
        query: BaseDruidQuery,
        query_type: DruidQueryTypes,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        order: Optional[Ordering] = None,
        query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = (),
        **kwargs: Any,
    ) -> Any:
        combined_filter = query._get_combined_filter_ir(query_filter_abilities)
        transformed_filter = (
            {'filter': DruidFilterTranslator().transform(combined_filter)} if combined_filter is not None else None
        )
        start = start or query.start
        end = end or query.end

        if start > end:
            raise ValueError('Start date must be before end date')

        if order is not None:
            kwargs['order'] = order.value

        context = kwargs.setdefault('context', {})
        context.setdefault('timeout', DEFAULT_DRUID_QUERY_TIMEOUT)

        druid = DRUID.instance()
        built_query = QueryBuilder().build_query(
            query_type.value,
            {'datasource': druid.datasource, 'intervals': f'{start.isoformat()}/{end.isoformat()}', **kwargs},
        )

        if transformed_filter:
            assert built_query.query_dict is not None
            built_query.query_dict['filter'] = transformed_filter['filter']

        result = druid.client._post(built_query)
        assert result.result_json
        return json.loads(result.result_json)

    def _execute_topn_single_period(
        self, query: TopNDruidQuery, start: Optional[datetime] = None, end: Optional[datetime] = None, **kwargs: Any
    ) -> List[Dict[str, Any]]:
        assert query.precision >= 0 and query.precision < 1, (
            'Precision specified was not valid; Must be a float between 0 and 1!'
        )

        return self._query_with_filter(
            query=query,
            query_type=DruidQueryTypes.TOP_N,
            start=start,
            end=end,
            aggregations={'count': count('count')},
            granularity='all',
            dimension=self._get_dimension_parameter(query),
            metric='count',
            threshold=query.limit,
            **kwargs,
        )

    def _sanitize_topn_results(self, results: List[Dict[str, Any]]) -> List[PeriodData]:
        sanitized_results: List[PeriodData] = []
        for result in results:
            sanitized_results.extend(self.build_period_data(result.get('timestamp'), result.get('result', [])))
        return sanitized_results

    def _get_dimension_parameter(self, query: TopNDruidQuery) -> Union[str, Dict[str, Any]]:
        dimension_type: Optional[str] = ENGINE.instance().get_feature_name_to_entity_type_mapping().get(query.dimension)
        dimension_parameter: Union[str, Dict[str, Any]] = query.dimension

        if (dimension_type is not None and dimension_type.lower() != 'float') or query.precision == 0:
            return dimension_parameter

        inverse_precision = int(1 / query.precision)
        precision_length = int(math.log10(inverse_precision))

        return {
            'type': 'extraction',
            'dimension': query.dimension,
            'outputName': query.dimension,
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


def parse_query_filter(query_filter: str) -> Optional[Dict[str, Any]]:
    if query_filter == '':
        return None

    validated_sources = parse_query_to_validated_ast(
        query_filter, rules_sources=ENGINE.instance().execution_graph.validated_sources
    )
    return DruidQueryTransformer(validated_sources=validated_sources).transform()


__all__ = [
    'BaseDruidQuery',
    'ComparisonData',
    'DEFAULT_DRUID_QUERY_TIMEOUT',
    'DimensionData',
    'DimensionDifference',
    'DruidEventQueryBackend',
    'EntityFilter',
    'GroupByApproximateCountDruidQuery',
    'Ordering',
    'PaginatedScanDruidQuery',
    'PaginatedScanResult',
    'PeriodData',
    'TimeseriesDruidQuery',
    'TopNDruidQuery',
    'TopNPoPResponse',
    'parse_query_filter',
    'parse_query_filter_ir',
]
