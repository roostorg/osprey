from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence

from osprey.worker.lib.singletons import CONFIG

from .event_queries import (
    ComparisonData,
    DimensionData,
    DimensionDifference,
    GroupByApproximateCountDruidQuery,
    PaginatedScanDruidQuery,
    PaginatedScanResult,
    PeriodData,
    TimeseriesDruidQuery,
    TopNDruidQuery,
    TopNPoPResponse,
)

if TYPE_CHECKING:
    from osprey.worker.lib.config import Config

    from .abilities import QueryFilterAbility

logger = logging.getLogger(__name__)


class EventQueryBackend(ABC):
    @abstractmethod
    def timeseries(
        self,
        query: TimeseriesDruidQuery,
        query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = (),
    ) -> Any: ...

    @abstractmethod
    def groupby_approximate_count(self, query: GroupByApproximateCountDruidQuery, **kwargs: Any) -> int: ...

    @abstractmethod
    def topn(self, query: TopNDruidQuery, **kwargs: Any) -> TopNPoPResponse: ...

    @abstractmethod
    def scan(
        self,
        query: PaginatedScanDruidQuery,
        query_filter_abilities: Sequence[Optional['QueryFilterAbility[Any, Any]']] = (),
    ) -> PaginatedScanResult: ...

    @staticmethod
    def build_period_data(timestamp: Any, result_items: Sequence[Mapping[str, Any]]) -> list[PeriodData]:
        dimension_data: list[DimensionData] = []

        for item in result_items:
            try:
                dimension_data.append(DimensionData(**item))
            except Exception as exc:
                logger.error('Failed to parse dimension data: %s, error: %s', item, exc)

        if not dimension_data:
            return []

        return [PeriodData(timestamp=timestamp, result=dimension_data)]

    @staticmethod
    def get_previous_period(
        query: TopNDruidQuery, calculate_previous_period: bool = True
    ) -> tuple[datetime, datetime] | None:
        if not calculate_previous_period:
            return None

        period_duration = query.end - query.start
        previous_start = query.start - period_duration
        previous_end = query.start

        max_historical_query_window_days = CONFIG.instance().get_int('MAX_HISTORICAL_QUERY_WINDOW_DAYS', 90)
        historical_cutoff = datetime.now(timezone.utc) - timedelta(days=max_historical_query_window_days)

        if previous_start.replace(tzinfo=timezone.utc) < historical_cutoff:
            return None

        return previous_start, previous_end

    @staticmethod
    def build_topn_pop_response(
        query: TopNDruidQuery, current_results: list[PeriodData], previous_results: list[PeriodData]
    ) -> TopNPoPResponse:
        if not previous_results:
            return TopNPoPResponse(current_period=current_results)

        dimension_key = query.dimension
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


def get_event_query_backend(config: 'Config | None' = None) -> EventQueryBackend:
    if config is None:
        from osprey.worker.lib.singletons import CONFIG as GLOBAL_CONFIG

        config = GLOBAL_CONFIG.instance()

    backend_type = config.get_str('OSPREY_EVENT_QUERY_BACKEND', 'druid').lower()

    if backend_type == 'druid':
        from osprey.worker.ui_api.osprey.lib.druid import DruidEventQueryBackend

        return DruidEventQueryBackend()
    elif backend_type == 'clickhouse':
        from osprey.worker.ui_api.osprey.lib.clickhouse import ClickHouseEventQueryBackend

        return ClickHouseEventQueryBackend()
    elif backend_type == 'plugin':
        from osprey.worker.adaptor.plugin_manager import bootstrap_event_query_backend

        backend = bootstrap_event_query_backend(config=config)
        if backend is None:
            raise AssertionError('No event query backend registered')
        return backend

    raise ValueError(f'Unknown OSPREY_EVENT_QUERY_BACKEND value: {backend_type}')
