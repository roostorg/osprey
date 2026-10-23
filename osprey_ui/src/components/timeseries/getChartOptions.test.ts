import { describe, it, expect } from '@rstest/core';

import * as echarts from 'echarts';

import { getChartOptions } from './getChartOptions';

const data: [number, number][] = [
  [Date.UTC(2026, 9, 20, 0), 100],
  [Date.UTC(2026, 9, 20, 1), 120],
];

// Render through ECharts' SSR renderer so the test exercises the real option,
// including how the built-in 'dark' theme (the one Timeseries passes) applies.
const renderSvg = (theme?: 'dark'): string => {
  const chart = echarts.init(null, theme, { renderer: 'svg', ssr: true, width: 400, height: 200 });
  chart.setOption(getChartOptions(data, 'MMM D hA', 'MMM D, YYYY h:mmA'));
  const svg = chart.renderToSVGString();
  chart.dispose();
  return svg;
};

const backgroundFill = (svg: string): string | undefined => {
  return svg.match(/<rect width="400" height="200" x="0" y="0" fill="([^"]+)"/)?.[1];
};

const textFills = (svg: string): Set<string> => {
  return new Set([...svg.matchAll(/<text[^>]*fill="([^"]+)"/g)].map((m) => m[1]));
};

describe('getChartOptions', () => {
  it('keeps the chart background transparent under both themes', () => {
    expect(backgroundFill(renderSvg('dark'))).toBe('none');
    expect(backgroundFill(renderSvg())).toBe('none');
  });

  it('lets the dark theme recolour the axis labels', () => {
    const light = textFills(renderSvg());
    const dark = textFills(renderSvg('dark'));

    expect(light.size).toBeGreaterThan(0);
    expect(dark.size).toBeGreaterThan(0);
    expect([...dark].filter((fill) => light.has(fill))).toEqual([]);
  });
});
