import { History, Location } from 'history';
import { matchPath } from 'react-router-dom';

import { saveQueryToHistory } from '../actions/QueryActions';
import {
  BaseQuery,
  DefaultIntervals,
  QueryState,
  CustomSummaryFeatures,
  ScanQueryOrder,
  MomentRangeValues,
  TopNTable,
  Chart,
} from '../types/QueryTypes';
import { getQueryDateRange, isEmptyDateRange } from './QueryUtils';

import { Routes } from '../Constants';

type QueryStoreState = QueryState & {
  customSummaryFeatures: CustomSummaryFeatures;
  entityFeatureFilters: Set<string>;
};

export function extractQueryStateFromSearchParams(location: Location): QueryStoreState {
  const isEntityView = matchPath(location.pathname, { path: Routes.ENTITY }) != null;
  const queryString = new URLSearchParams(location.search);

  const topNTables: Map<string, TopNTable> = new Map();
  queryString.getAll('topn').map((data) => {
    const table: TopNTable = TopNTable.fromQueryParam(data);
    topNTables.set(table.dimension, table);
  });

  const charts: Array<Chart> = queryString.getAll('chart').map(Chart.fromQueryParam);

  const customSummaryFeatures = queryString.getAll('customSummaryFeatures');

  const queryState = {
    executedQuery: {
      queryFilter: queryString.get('queryFilter') || '',
      start: queryString.get('start') || '',
      end: queryString.get('end') || '',
      interval:
        queryString.get('interval') != null && queryString.get('interval') !== ''
          ? (queryString.get('interval') as DefaultIntervals)
          : 'day',
    },
    // Parse a `ScanQueryOrder` from the query string, defaulting to `descending` if the order isn't understood or
    // not provided.
    sortOrder:
      queryString.get('order') === ScanQueryOrder.ASCENDING ? ScanQueryOrder.ASCENDING : ScanQueryOrder.DESCENDING,
    topNTables,
    charts,
    entityFeatureFilters: new Set(queryString.getAll('entityFeatureFilters')),
    customSummaryFeatures: customSummaryFeatures.length === 0 ? null : customSummaryFeatures,
  };

  const { start, end } = queryState.executedQuery;

  // Do not save queries from the entity view yet
  if (!isEntityView && !isEmptyDateRange(start, end)) {
    saveQueryToHistory(queryState);
  }

  return queryState;
}

export function getSearchParamsForExecutedQuery(query: BaseQuery, location?: Location): URLSearchParams {
  const queryString = new URLSearchParams(location?.search ?? '');

  Object.entries(query).forEach(([key, value]) => {
    if (value != null) {
      queryString.set(key, String(value));
    }
  });

  return queryString;
}

const ENTITY_VIEW_DEFAULT_SEARCH_PARAMS: Array<[string, string]> = [
  ['queryFilter', ''],
  ['topn', 'ActionName'],
];
const ENTITY_VIEW_DEFAULT_INTERVAL: MomentRangeValues = 'twoWeeks';

export function setSearchParamsForEntityView(history: History) {
  // Check that we are acting on the correct route.
  const entityViewMatch = matchPath(history.location.pathname, { path: Routes.ENTITY });
  if (entityViewMatch == null) {
    return;
  }

  const locationSearchParams = new URLSearchParams(history.location.search);

  // If not all location based params are set, choose a default of last two weeks.
  if (!['start', 'end', 'interval'].every((param) => locationSearchParams.has(param))) {
    const { start, end } = getQueryDateRange(ENTITY_VIEW_DEFAULT_INTERVAL);
    locationSearchParams.set('start', start);
    locationSearchParams.set('end', end);
    locationSearchParams.set('interval', ENTITY_VIEW_DEFAULT_INTERVAL);
  }

  // Ensure all defaults are set.
  for (const [key, value] of ENTITY_VIEW_DEFAULT_SEARCH_PARAMS) {
    if (!locationSearchParams.has(key)) {
      locationSearchParams.set(key, value);
    }
  }

  history.replace({ pathname: history.location.pathname, search: `?${locationSearchParams}` });
}
