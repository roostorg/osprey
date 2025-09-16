import moment from 'moment';

import { history } from '../stores/QueryStore';
import { QueryRecord, ScanQueryOrder } from '../types/QueryTypes';
import { getIntervalFromDateRange } from './DateUtils';
import { getQueryDateRange } from './QueryUtils';

export function openNewQueryWindow(queryFilter: string) {
  const queryString = new URLSearchParams(history.location.search);
  queryString.set('queryFilter', queryFilter);

  window.open(`${window.location.origin}/?${queryString.toString()}`, window.name);
}

export function getSearchParamsForQueryRecord(query: QueryRecord, useInterval: boolean = false): string {
  const interval = getIntervalFromDateRange(query.date_range);

  let start = moment.utc(query.date_range.start).format();
  let end = moment.utc(query.date_range.end).format();

  if (useInterval && interval != null) {
    const dateRange = getQueryDateRange(interval);

    start = dateRange.start;
    end = dateRange.end;
  }

  const queryString = new URLSearchParams();
  queryString.append('queryFilter', query.query_filter);
  queryString.append('start', start);
  queryString.append('end', end);

  if (interval != null) {
    queryString.append('interval', interval);
  }

  if (query.sort_order === ScanQueryOrder.ASCENDING) {
    queryString.append('sortOrder', ScanQueryOrder.ASCENDING);
  }

  if (query.top_n.length > 0) {
    query.top_n.forEach((table) => {
      queryString.append('topn', table.asQueryParam());
    });
  }

  return `?${queryString.toString()}`;
}
