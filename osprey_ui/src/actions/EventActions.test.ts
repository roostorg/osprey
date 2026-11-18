import { describe, expect, it, rs } from '@rstest/core';
import { AxiosError } from 'axios';

import { getScanQueryResults, getTimeseriesQueryResults, getTopNQueryResults } from './EventActions';
import { ScanQueryOrder } from '../types/QueryTypes';

// The real QueryStore calls createBrowserHistory() at import time.
rs.mock('../stores/QueryStore', () => ({ history: { location: { pathname: '/' } } }));

rs.mock('../utils/HTTPUtils', () => ({
  default: {
    post: async () => {
      const error = new AxiosError('Request failed with status code 502', 'ERR_BAD_RESPONSE');
      error.response = {
        status: 502,
        statusText: '',
        data: 'Druid query failed: Predicate does not support ARRAY types',
        headers: {},
        config: {} as never,
      };
      return { ok: false, error };
    },
  },
  getErrorMessage: (error: AxiosError) => error.response?.data,
}));

const query = { start: '2026-11-18T00:00:00Z', end: '2026-11-19T00:00:00Z', queryFilter: '', interval: 'day' };

// These used to resolve to empty results on failure, which the panels rendered as "No data".
describe('query actions reject when the API fails', () => {
  const message = 'Druid query failed: Predicate does not support ARRAY types';

  it('timeseries', async () => {
    await expect(getTimeseriesQueryResults(query as never, 'day')).rejects.toThrow(message);
  });

  it('top n', async () => {
    await expect(getTopNQueryResults(query as never, 'UserId')).rejects.toThrow(message);
  });

  it('scan', async () => {
    await expect(getScanQueryResults(query as never, 'DESCENDING' as ScanQueryOrder, 10)).rejects.toThrow(message);
  });
});
