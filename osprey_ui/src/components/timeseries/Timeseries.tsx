import * as React from 'react';
import { Select, Spin } from 'antd';
import ReactECharts from 'echarts-for-react';
import type { EChartsType } from 'echarts';
import dayjs from 'dayjs';
import shallow from 'zustand/shallow';

import { getTimeseriesQueryResults } from '../../actions/EventActions';
import useQueryStore from '../../stores/QueryStore';
import { TimeseriesResult } from '../../types/QueryTypes';
import Text, { TextSizes } from '../../uikit/Text';
import TimeseriesIcon from '../../uikit/icons/TimeseriesIcon';
import Panel from '../common/Panel';

import styles from './Timeseries.module.css';

// prettier-ignore
const Granularities = {
  // name: duration in ms
  minute: 1000 * 60,
  fifteen_minute: 1000 * 60 * 15,
  thirty_minute: 1000 * 60 * 30,
  hour: 1000 * 60 * 60,
  day: 1000 * 60 * 60 * 24,
  week: 1000 * 60 * 60 * 24 * 7,
  month: 1000 * 60 * 60 * 24 * 30,
};
const GRANULARITY_STRINGS = {
  minute: 'minute',
  fifteen_minute: 'fifteen minutes',
  thirty_minute: 'half hour',
  hour: 'hour',
  day: 'day',
  week: 'week',
  month: 'month',
};
const DEFAULT_GRANULARITY = 'hour';
type Granularity = keyof typeof Granularities;

function getDateFormatForGranularity(granularity: Granularity | 'other'): string {
  // dayjs format strings — https://day.js.org/docs/en/display/format
  switch (granularity) {
    case 'minute':
      // 4:23PM
      return 'h:mmA';
    case 'fifteen_minute':
    case 'thirty_minute':
    case 'hour':
      // Jun 1 4PM
      return 'MMM D hA';
    case 'day':
    case 'week':
      // Jun 1, 2020
      return 'MMM D, YYYY';
    case 'month':
      // Jun 2020
      return 'MMM YYYY';
    default:
      // Jun 1, 2020 4:23PM
      return 'MMM D, YYYY h:mmA';
  }
}

const MIN_GRANULARITY_DATAPOINTS = 7;
function getDefaultGranularityForTimeSpan(start: string | null, end: string | null): Granularity {
  // choose the largest granularity that would have more than $MIN_GRANULARITY_DATAPOINTS datapoints
  if (!start || !end) {
    return DEFAULT_GRANULARITY;
  }

  const startDate = dayjs(start);
  const endDate = dayjs(end);
  const duration = Math.abs(dayjs.duration(endDate.diff(startDate)).asMilliseconds());

  const sortedGranularities = (Object.entries(Granularities) as Array<[Granularity, number]>).sort(
    ([, durationA], [, durationB]) => durationB - durationA
  );

  let currentGranularity: Granularity | null = null;
  for (const [granularity, granularityDuration] of sortedGranularities) {
    currentGranularity = granularity;
    if (duration / granularityDuration > MIN_GRANULARITY_DATAPOINTS) {
      break;
    }
  }

  if (currentGranularity == null) {
    throw new Error('Could not determine granularity for timespan!');
  }

  return currentGranularity;
}

function getChartData(timeseriesData: TimeseriesResult[]): [number, number][] {
  if (timeseriesData.length === 0) {
    return [];
  }
  return timeseriesData.map((point: TimeseriesResult) => [Date.parse(point.timestamp), point.result.count]);
}

const EmptyOverlay = ({ show, children }: { show: boolean; children: React.ReactNode }) => {
  const overlay = show ? (
    <Text size={TextSizes.LARGE} className={styles.emptyChartOverlay}>
      No data available
    </Text>
  ) : null;
  return (
    <div className={styles.emptyChartOverlayContainer}>
      {children}
      {overlay}
    </div>
  );
};

const GranularitySelect = ({
  onChange,
  granularity,
}: {
  onChange: (value: Granularity) => void;
  granularity: Granularity;
}): React.ReactElement => {
  return (
    <Select showSearch value={granularity} onChange={onChange} style={{ minWidth: 200 }}>
      {Object.keys(Granularities).map((option: string) => (
        <Select.Option key={option} value={option}>
          {GRANULARITY_STRINGS[option as Granularity]}
        </Select.Option>
      ))}
    </Select>
  );
};

interface TimeseriesProps {
  extraQuery?: string;
}

const Timeseries: React.FC<TimeseriesProps> = ({ extraQuery }: TimeseriesProps) => {
  const [executedQuery, entityFeatureFilters, applyIfQueryIsCurrent, updateExecutedQuery] = useQueryStore(
    (state) => [
      state.executedQuery,
      state.entityFeatureFilters,
      state.applyIfQueryIsCurrent,
      state.updateExecutedQuery,
    ],
    shallow
  );
  const { start, end } = executedQuery;

  const [timeseriesData, setTimeseriesData] = React.useState<TimeseriesResult[]>([]);
  const [isLoading, setIsLoading] = React.useState(false);
  const [granularity, setGranularity] = React.useState(getDefaultGranularityForTimeSpan(start, end));

  React.useEffect(() => {
    setIsLoading(true);

    let shouldApply = true;
    async function getTimeseriesResults() {
      if (start !== '' && end !== '') {
        const query = { ...executedQuery };
        query.queryFilter = [extraQuery, executedQuery.queryFilter]
          .filter((q) => q !== undefined && q !== '')
          .map((q) => `(${q})`)
          .join(' and ');
        const results = await getTimeseriesQueryResults({ ...query, entityFeatureFilters }, granularity);
        // don't apply results if we triggered a new fetch
        if (!shouldApply) return;
        applyIfQueryIsCurrent(executedQuery, () => setTimeseriesData(results));
      }

      setIsLoading(false);
    }

    setTimeseriesData([]);
    getTimeseriesResults();

    return () => {
      // this means the component is either unmounting, or is about to run a new fetch
      shouldApply = false;
    };
  }, [granularity, executedQuery, applyIfQueryIsCurrent, start, end, entityFeatureFilters, extraQuery]);

  React.useEffect(() => {
    // update granularity when we change the query range
    setGranularity(getDefaultGranularityForTimeSpan(start, end));
  }, [start, end]);

  // Activate lineX brush mode immediately so the user can drag to select a date range
  // without needing to click a toolbox button first (matching the previous Highcharts zoomType:'x' UX).
  function handleChartReady(chart: EChartsType): void {
    chart.dispatchAction({
      type: 'takeGlobalCursor',
      key: 'brush',
      brushOption: { brushType: 'lineX', brushMode: 'single' },
    });
  }

  function handleBrushEnd(params: { areas?: Array<{ coordRange?: [number, number] }> }): void {
    const area = params.areas?.[0];
    if (!area?.coordRange) return;
    const [startTs, endTs] = area.coordRange;

    // ECharts does not close over executedQuery like React handlers do,
    // so read the latest value directly from the store.
    const { executedQuery: current } = useQueryStore.getState();
    updateExecutedQuery({
      ...current,
      interval: 'custom',
      start: new Date(startTs).toISOString(),
      end: new Date(endTs).toISOString(),
    });
  }

  const chartData = getChartData(timeseriesData);
  const axisDateFormat = getDateFormatForGranularity(granularity);
  const tooltipDateFormat = getDateFormatForGranularity('other');

  const chartOptions = {
    grid: { left: '3%', right: '4%', bottom: '15%', containLabel: true },
    xAxis: {
      type: 'time',
      splitLine: { show: true },
      axisLabel: {
        // ECharts time axis provides millisecond timestamps; dayjs renders them
        // in the browser's local timezone, matching the previous Highcharts behaviour.
        formatter: (value: number) => dayjs(value).format(axisDateFormat),
        rotate: -45,
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
        if (!list.length) return '';
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

  return (
    <Panel
      className={styles.timeseriesPanel}
      title="Timeseries Chart"
      icon={<TimeseriesIcon />}
      titleRight={<GranularitySelect granularity={granularity} onChange={setGranularity} />}
    >
      <Spin spinning={isLoading}>
        <div className={styles.chartContainer}>
          <EmptyOverlay show={chartData.length === 0}>
            <ReactECharts
              option={chartOptions}
              style={{ height: 300 }}
              onChartReady={handleChartReady}
              onEvents={{ brushEnd: handleBrushEnd }}
            />
          </EmptyOverlay>
        </div>
      </Spin>
    </Panel>
  );
};

export default Timeseries;
