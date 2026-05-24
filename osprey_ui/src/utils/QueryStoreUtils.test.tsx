import { Location } from 'history';
import dayjs from 'dayjs';

import '../utils/DayjsSetup';

import { saveQueryToHistory } from '../actions/QueryActions';
import { extractQueryStateFromSearchParams } from './QueryStoreUtils';

jest.mock('../actions/QueryActions', () => {
  return {
    saveQueryToHistory: jest.fn(),
  };
});

const mockedSaveQueryToHistory = saveQueryToHistory as jest.Mock;

const makeLocation = (search: string): Location => {
  return {
    pathname: '/',
    search,
    hash: '',
    state: undefined,
    key: 'test',
  } as Location;
};

const ONE_MINUTE_MS = 60 * 1000;

const expectIsApproximatelyNow = (timestamp: string, toleranceMs: number = ONE_MINUTE_MS): void => {
  const diff = Math.abs(dayjs.utc(timestamp).diff(dayjs.utc()));
  expect(diff).toBeLessThanOrEqual(toleranceMs);
};

const expectIsApproximatelyNowMinus = (
  timestamp: string,
  amount: number,
  unit: dayjs.ManipulateType,
  toleranceMs: number = ONE_MINUTE_MS
): void => {
  const expected = dayjs.utc().subtract(amount, unit);
  const diff = Math.abs(dayjs.utc(timestamp).diff(expected));
  expect(diff).toBeLessThanOrEqual(toleranceMs);
};

describe('extractQueryStateFromSearchParams', () => {
  describe('home route with no URL params (fresh page load)', () => {
    it('populates start and end from the default 1-day interval relative to now', () => {
      const state = extractQueryStateFromSearchParams(makeLocation(''));

      expect(state.executedQuery.interval).toBe('day');
      expect(state.executedQuery.start).not.toBe('');
      expect(state.executedQuery.end).not.toBe('');
      expectIsApproximatelyNowMinus(state.executedQuery.start, 1, 'day');
      expectIsApproximatelyNow(state.executedQuery.end);
    });
  });

  describe('home route with relative interval and stale timestamps', () => {
    it('recomputes start and end from the interval relative to now (ignores stale URL timestamps)', () => {
      const staleStart = '2025-01-01T00:00:00Z';
      const staleEnd = '2025-01-02T00:00:00Z';
      const state = extractQueryStateFromSearchParams(
        makeLocation(`?interval=day&start=${encodeURIComponent(staleStart)}&end=${encodeURIComponent(staleEnd)}`)
      );

      expect(state.executedQuery.interval).toBe('day');
      // The URL timestamps were from 2025-01-01, but interval=day means "last
      // 24h relative to now" — the returned range must be fresh.
      expect(state.executedQuery.start).not.toBe(staleStart);
      expect(state.executedQuery.end).not.toBe(staleEnd);
      expectIsApproximatelyNowMinus(state.executedQuery.start, 1, 'day');
      expectIsApproximatelyNow(state.executedQuery.end);
    });

    it('recomputes start and end for a different relative interval (twoHours)', () => {
      const state = extractQueryStateFromSearchParams(
        makeLocation(`?interval=twoHours&start=2025-01-01T00:00:00Z&end=2025-01-01T02:00:00Z`)
      );

      expect(state.executedQuery.interval).toBe('twoHours');
      expectIsApproximatelyNowMinus(state.executedQuery.start, 2, 'hour');
      expectIsApproximatelyNow(state.executedQuery.end);
    });
  });

  describe('home route with custom interval and explicit timestamps', () => {
    it('preserves the explicit start and end timestamps from the URL', () => {
      const explicitStart = '2025-01-01T00:00:00Z';
      const explicitEnd = '2025-01-02T00:00:00Z';
      const state = extractQueryStateFromSearchParams(
        makeLocation(
          `?interval=custom&start=${encodeURIComponent(explicitStart)}&end=${encodeURIComponent(explicitEnd)}`
        )
      );

      expect(state.executedQuery.interval).toBe('custom');
      expect(state.executedQuery.start).toBe(explicitStart);
      expect(state.executedQuery.end).toBe(explicitEnd);
    });
  });

  describe('home route with relative interval but no timestamps', () => {
    it('derives fresh timestamps from the interval', () => {
      const state = extractQueryStateFromSearchParams(makeLocation('?interval=hour'));

      expect(state.executedQuery.interval).toBe('hour');
      expectIsApproximatelyNowMinus(state.executedQuery.start, 1, 'hour');
      expectIsApproximatelyNow(state.executedQuery.end);
    });
  });

  describe('saveQueryToHistory side effect', () => {
    beforeEach(() => {
      mockedSaveQueryToHistory.mockClear();
    });

    it('does not save to history when the URL had no explicit timestamps (fresh page load)', () => {
      extractQueryStateFromSearchParams(makeLocation(''));

      expect(mockedSaveQueryToHistory).not.toHaveBeenCalled();
    });

    it('saves to history when the URL has explicit start and end timestamps', () => {
      extractQueryStateFromSearchParams(
        makeLocation('?interval=custom&start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z')
      );

      expect(mockedSaveQueryToHistory).toHaveBeenCalledTimes(1);
    });
  });
});
