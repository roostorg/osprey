from __future__ import annotations

import logging
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, List, Optional, Sequence

from osprey.engine.query_language import parse_query_to_validated_ast
from osprey.engine.query_language.ast_filter_ir_translator import FilterIrTransformer
from osprey.engine.query_language.filter_ir import (
    BooleanFilter,
    BooleanOperator,
    ComparisonFilter,
    ComparisonOperator,
    FeatureRef,
    FilterExpression,
    LiteralValue,
    and_filters,
)
from osprey.worker.lib.singletons import ENGINE
from pydantic.main import BaseModel

from .marshal import JsonBodyMarshaller

if TYPE_CHECKING:
    from .abilities import QueryFilterAbility

logger = logging.getLogger(__name__)


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

    def to_filter_ir(self) -> FilterExpression:
        feature_to_entity_mapping = ENGINE.instance().get_feature_name_to_entity_type_mapping()
        filters = tuple(
            ComparisonFilter(
                left=FeatureRef(feature_name),
                operator=ComparisonOperator.EQUALS,
                right=LiteralValue(self.id),
            )
            for feature_name, entity_type in feature_to_entity_mapping.items()
            if entity_type == self.type and (not self.feature_filters or feature_name in self.feature_filters)
        )

        if not filters:
            logger.warning(
                'Entity filter produced no matching features for type=%s feature_filters=%s',
                self.type,
                self.feature_filters,
            )

        return BooleanFilter(operator=BooleanOperator.OR, fields=filters)


class BaseDruidQuery(BaseModel, JsonBodyMarshaller):
    start: datetime
    end: datetime
    query_filter: str
    entity: Optional[EntityFilter]

    def _get_combined_filter_ir(
        self, query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = ()
    ) -> Optional[FilterExpression]:
        filters: List[FilterExpression] = []

        transformed_filter = parse_query_filter_ir(self.query_filter, allow_druid_udf_fallback=True)
        if transformed_filter is not None:
            filters.append(transformed_filter)

        if self.entity:
            filters.append(self.entity.to_filter_ir())

        ability_filters_list: List[FilterExpression] = []
        for ability in query_filter_abilities:
            if ability is None:
                continue

            ability_filter = ability._get_query_filter()
            if ability_filter is not None:
                ability_filters_list.append(ability_filter)

        ability_filters = tuple(ability_filters_list)
        if ability_filters:
            filters.append(BooleanFilter(operator=BooleanOperator.AND, fields=ability_filters))

        return and_filters(tuple(filters))


class TimeseriesDruidQuery(BaseDruidQuery):
    granularity: str
    aggregation_dimensions: Optional[List[str]] = None

    def execute(self, **kwargs: Any) -> Any:
        from .event_query_backend import get_event_query_backend

        return get_event_query_backend().timeseries(self, **kwargs)


class GroupByApproximateCountDruidQuery(BaseDruidQuery):
    """
    Returns an approximate count of a groupBy query's unique dimension values (i.e. number of unique UserIds).
    This is done using a cardinality aggregator.
    """

    dimension: str

    def execute(self, **kwargs: Any) -> int:
        from .event_query_backend import get_event_query_backend

        return get_event_query_backend().groupby_approximate_count(self, **kwargs)


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


class TopNDruidQuery(BaseDruidQuery):
    dimension: str
    limit: int = 100
    precision: float = 0

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

    def execute(self, **kwargs: Any) -> TopNPoPResponse:
        from .event_query_backend import get_event_query_backend

        return get_event_query_backend().topn(self, **kwargs)


class PaginatedScanDruidQuery(BaseDruidQuery):
    limit: int = 100
    next_page: Optional[str] = None
    order: Ordering = Ordering.DESCENDING

    def execute(
        self, query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = ()
    ) -> PaginatedScanResult:
        from .event_query_backend import get_event_query_backend

        return get_event_query_backend().scan(self, query_filter_abilities=query_filter_abilities)


BaseEventQuery = BaseDruidQuery
TimeseriesEventQuery = TimeseriesDruidQuery
GroupByApproximateCountEventQuery = GroupByApproximateCountDruidQuery
TopNEventQuery = TopNDruidQuery
PaginatedScanEventQuery = PaginatedScanDruidQuery


def parse_query_filter_ir(query_filter: str, allow_druid_udf_fallback: bool = False) -> Optional[FilterExpression]:
    if query_filter == '':
        return None

    validated_sources = parse_query_to_validated_ast(
        query_filter, rules_sources=ENGINE.instance().execution_graph.validated_sources
    )
    return FilterIrTransformer(
        validated_sources=validated_sources, allow_druid_udf_fallback=allow_druid_udf_fallback
    ).transform()


__all__ = [
    'BaseDruidQuery',
    'BaseEventQuery',
    'ComparisonData',
    'DimensionData',
    'DimensionDifference',
    'EntityFilter',
    'GroupByApproximateCountEventQuery',
    'GroupByApproximateCountDruidQuery',
    'Ordering',
    'PaginatedScanEventQuery',
    'PaginatedScanDruidQuery',
    'PaginatedScanResult',
    'PeriodData',
    'TimeseriesEventQuery',
    'TimeseriesDruidQuery',
    'TopNEventQuery',
    'TopNDruidQuery',
    'TopNPoPResponse',
    'parse_query_filter_ir',
]
