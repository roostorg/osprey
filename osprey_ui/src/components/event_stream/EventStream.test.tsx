import { describe, it, expect, rs } from '@rstest/core';
import { render, screen } from '@testing-library/react';

import EventStream from './EventStream';

// No events render on failure; the cards import Sass modules the test bundler can't parse.
rs.mock('./EventStreamCard', () => ({ default: () => null }));
rs.mock('./FeatureSelectModal', () => ({ default: () => null }));

rs.mock('../../actions/EventActions', () => ({
  getScanQueryResults: () => Promise.reject(new Error('Druid query failed: Predicate does not support ARRAY types')),
  getFeatureLocations: () => Promise.resolve({ locations: [] }),
}));

// The real QueryStore calls createBrowserHistory() at import time, which throws outside a browser.
rs.mock('../../stores/QueryStore', () => {
  const state = {
    executedQuery: { start: '2026-11-18T00:00:00Z', end: '2026-11-19T00:00:00Z', queryFilter: '' },
    sortOrder: 'DESCENDING',
    entityFeatureFilters: new Set(),
    applyIfQueryIsCurrent: (_query: unknown, apply: () => void) => apply(),
    updateSortOrder: () => {},
    customSummaryFeatures: null,
    updateCustomSummaryFeatures: () => {},
  };
  return { default: (selector: (s: typeof state) => unknown) => selector(state) };
});

describe('EventStream', () => {
  it('shows the query error instead of an empty list', async () => {
    render(<EventStream />);

    expect(await screen.findByText('Druid query failed: Predicate does not support ARRAY types')).toBeTruthy();
    expect(screen.getByText('Query failed')).toBeTruthy();
  });
});
