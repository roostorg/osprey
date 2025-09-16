import { createBrowserHistory } from 'history';
import create from 'zustand';

import { Feature } from '../types/EntityTypes';
import { BaseQuery, ScanQueryOrder, CustomSummaryFeatures, TopNTable, Chart } from '../types/QueryTypes';
import {
  extractQueryStateFromSearchParams,
  getSearchParamsForExecutedQuery,
  setSearchParamsForEntityView,
} from '../utils/QueryStoreUtils';
import { baseQueryEquals } from '../utils/QueryUtils';

export const history = createBrowserHistory();

// If the application is loaded on the entity route with an empty query string,
// set the query string to the default entity query before extracting
// query state.
setSearchParamsForEntityView(history);

export interface QueryStore {
  executedQuery: BaseQuery;
  sortOrder: ScanQueryOrder;
  topNTables: Map<string, TopNTable>;
  charts: Array<Chart>;
  entityFeatureFilters: Set<string>;

  updateExecutedQuery: (query: BaseQuery) => void;
  updateSortOrder: (sortOrder: ScanQueryOrder) => void;
  updateTopNTables: (topNTables: Map<string, TopNTable>) => void;
  addChart: (chart: Chart) => void;
  updateChart: (index: number, chart: Chart) => void;
  updateEntityFeatureFilters: (entityFeatureFilters: Set<string>) => void;

  /*
   * If you are performing an asynchronous operation, such as fetching data from an API, the current query
   * may have changed. This utility function will check if the query provided matches the current query,
   * and if it does, will invoke the provided `fn`.
   */
  applyIfQueryIsCurrent(query: BaseQuery, fn: () => unknown): void;

  queuedQueryFilters: Feature[];
  updateQueuedQueryFilters: (shortcutUpdates: Feature[]) => void;

  customSummaryFeatures: CustomSummaryFeatures;
  updateCustomSummaryFeatures: (features: CustomSummaryFeatures) => void;
}

const _updateQueryStringCharts = (charts: Array<Chart>) => {
  const queryString = new URLSearchParams(history.location.search);
  queryString.delete('chart');
  charts.filter((g) => !g.deleted).forEach((chart) => queryString.append('chart', chart.query));
  history.push({ search: `?${queryString.toString()}` });
};

const useQueryStore = create<QueryStore>((set, get) => ({
  ...extractQueryStateFromSearchParams(history.location),

  updateExecutedQuery: (query: BaseQuery) => {
    const queryString = getSearchParamsForExecutedQuery(query, history.location);
    history.push({ search: `?${queryString.toString()}` });
  },
  updateSortOrder: (sortOrder: ScanQueryOrder) => {
    const queryString = new URLSearchParams(history.location.search);
    queryString.set('order', sortOrder);

    history.push({ search: `?${queryString.toString()}` });
  },
  updateTopNTables: (topNTables: Map<string, TopNTable>) => {
    const queryString = new URLSearchParams(history.location.search);

    queryString.delete('topn');
    topNTables.forEach((table, __) => queryString.append('topn', table.asQueryParam()));

    history.push({ search: `?${queryString.toString()}` });
  },
  addChart: (chart: Chart) =>
    set(({ charts }) => {
      const newCharts = [...charts, chart];
      _updateQueryStringCharts(newCharts);
      return { charts: newCharts };
    }),
  updateChart: (index: number, chart: Chart) =>
    set(({ charts }) => {
      const newCharts = [...charts];
      newCharts[index] = chart;
      _updateQueryStringCharts(newCharts);
      return { charts: newCharts };
    }),
  updateEntityFeatureFilters: (entityFeatureFilters: Set<string>) => {
    const queryString = new URLSearchParams(history.location.search);

    queryString.delete('entityFeatureFilters');
    entityFeatureFilters.forEach((filter) => queryString.append('entityFeatureFilters', filter));

    history.push({ search: `?${queryString.toString()}` });
  },

  applyIfQueryIsCurrent: (query: BaseQuery, fn: () => unknown) => {
    const { executedQuery } = get();
    if (baseQueryEquals(executedQuery, query)) {
      fn();
    }
  },

  queuedQueryFilters: [],
  updateQueuedQueryFilters: (shortcutUpdates: Feature[]) => set(() => ({ queuedQueryFilters: shortcutUpdates })),

  updateCustomSummaryFeatures: (features: CustomSummaryFeatures) => {
    const queryString = new URLSearchParams(history.location.search);

    queryString.delete('customSummaryFeatures');
    if (features != null) {
      features.forEach((feature) => queryString.append('customSummaryFeatures', feature));
    }

    history.push({ search: `?${queryString.toString()}` });
  },
}));

export default useQueryStore;
