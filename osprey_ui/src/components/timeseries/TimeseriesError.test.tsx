import { describe, it, expect, rs } from '@rstest/core';
import { render, screen } from '@testing-library/react';

import '../../utils/DayjsSetup';

import Timeseries from './Timeseries';

rs.mock('echarts-for-react', () => ({ default: () => null }));

rs.mock('../../actions/EventActions', () => ({
  getTimeseriesQueryResults: () =>
    Promise.reject(new Error('Druid query failed: Predicate does not support ARRAY types')),
}));

// The real QueryStore calls createBrowserHistory() at import time, which throws outside a browser.
rs.mock('../../stores/QueryStore', () => {
  const state = {
    executedQuery: { start: '2026-11-18T00:00:00Z', end: '2026-11-19T00:00:00Z', queryFilter: '' },
    entityFeatureFilters: new Set(),
    applyIfQueryIsCurrent: (_query: unknown, apply: () => void) => apply(),
    updateExecutedQuery: () => {},
  };
  return { default: (selector: (s: typeof state) => unknown) => selector(state) };
});

describe('Timeseries', () => {
  it('shows the query error instead of "No data available"', async () => {
    render(<Timeseries />);

    expect(await screen.findByText('Druid query failed: Predicate does not support ARRAY types')).toBeTruthy();
    expect(screen.queryByText('No data available')).toBeNull();
  });
});
