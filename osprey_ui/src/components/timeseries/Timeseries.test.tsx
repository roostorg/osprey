import { describe, it, expect, rs } from '@rstest/core';
import { renderToStaticMarkup } from 'react-dom/server';

import useThemeStore from '../../stores/ThemeStore';

import Timeseries from './Timeseries';

const themes = rs.hoisted((): unknown[] => []);

rs.mock('echarts-for-react', () => ({
  default: (props: { theme?: string }) => {
    themes.push(props.theme);
    return null;
  },
}));

// The real QueryStore calls createBrowserHistory() at import time, which throws outside a browser.
rs.mock('../../stores/QueryStore', () => {
  const state = { executedQuery: { start: '', end: '' } };
  return { default: (selector: (s: typeof state) => unknown) => selector(state) };
});

describe('Timeseries', () => {
  it('passes the dark ECharts theme only in dark mode', () => {
    useThemeStore.setState({ mode: 'light' });
    renderToStaticMarkup(<Timeseries />);
    useThemeStore.setState({ mode: 'dark' });
    renderToStaticMarkup(<Timeseries />);

    expect(themes).toEqual([undefined, 'dark']);
  });
});
