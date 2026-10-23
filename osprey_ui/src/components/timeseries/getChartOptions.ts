import dayjs from 'dayjs';
import type { EChartsCoreOption } from 'echarts';

/**
 * Builds the ECharts option for the Timeseries bar chart. Kept outside the
 * component so the rendered output can be tested with ECharts' SSR renderer.
 * Typed as the loose EChartsCoreOption because the strict EChartsOption typings
 * reject `splitLine.customValues`, which these options set.
 */
export const getChartOptions = (
  chartData: [number, number][],
  axisDateFormat: string,
  tooltipDateFormat: string
): EChartsCoreOption => {
  // Only show ticks/labels at timestamps that actually have data, matching the
  // previous Highcharts behaviour, while keeping a continuous time axis (so brush
  // drag-to-select still maps directly to real timestamps).
  const dataTimestamps = chartData.map(([timestamp]) => timestamp);

  return {
    // Let the panel background show through; the built-in dark theme would
    // otherwise paint its own near-black canvas over it.
    backgroundColor: 'transparent',
    grid: { left: '3%', right: '4%', bottom: 10, containLabel: true },
    xAxis: {
      type: 'time',
      splitLine: { show: true, customValues: dataTimestamps },
      axisTick: { customValues: dataTimestamps },
      axisLabel: {
        customValues: dataTimestamps,
        // ECharts time axis provides millisecond timestamps; dayjs renders them
        // in the browser's local timezone, matching the previous Highcharts behaviour.
        formatter: (value: number) => dayjs(value).format(axisDateFormat),
        rotate: 45,
        align: 'right',
      },
    },
    yAxis: {
      type: 'value',
      min: 0,
      axisLabel: {
        formatter: (value: number) => value.toLocaleString('en-US', { maximumFractionDigits: 0 }),
      },
    },
    tooltip: {
      trigger: 'axis',
      formatter: (params: unknown) => {
        const list = params as Array<{ data: [number, number] }>;
        if (!Array.isArray(list) || !list.length || !list[0]?.data) return '';
        const [timestamp, value] = list[0].data;
        return `${dayjs(timestamp).format(tooltipDateFormat)}: ${Number(value).toLocaleString()}`;
      },
    },
    series:
      chartData.length > 0
        ? [
            {
              type: 'bar',
              data: chartData,
              name: '# Events',
              itemStyle: { color: '#8e5ea2' },
              barMaxWidth: 40,
            },
          ]
        : [],
    // Register the brush component. The toolbox buttons are hidden; brush mode is
    // activated programmatically in handleChartReady so drag-to-select works immediately.
    brush: { toolbox: ['lineX'], xAxisIndex: 0 },
    toolbox: { show: false },
  };
};
