import * as React from 'react';
import { Select, Spin } from 'antd';
import Plot from 'react-plotly.js';
import type { Layout, Config, PlotRelayoutEvent } from 'plotly.js';
import dayjs from 'dayjs';
import shallow from 'zustand/shallow';

import { getTimeseriesQueryResults } from '../../actions/EventActions';
import useQueryStore from '../../stores/QueryStore';
import { TimeseriesResult } from '../../types/QueryTypes';
import Text, { TextSizes } from '../../uikit/Text';
import TimeseriesIcon from '../../uikit/icons/TimeseriesIcon';
import Panel from '../common/Panel';

import styles from './Timeseries.module.css';
import { makeEntityRoute } from '../../utils/RouteUtils';

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
  // D3 time format strings used by Plotly: https://d3js.org/d3-time-format
  switch (granularity) {
    case 'minute':
      // 04:23PM
      return '%I:%M%p';
    case 'fifteen_minute':
    case 'thirty_minute':
    case 'hour':
      // Jun  1 04PM
      return '%b %e %I%p';
    case 'day':
    case 'week':
      // Jun  1, 2020
      return '%b %e, %Y';
    case 'month':
      // Jun 2020
      return '%b %Y';
    default:
      // Jun  1, 2020 04:23PM
      return '%b %e, %Y %I:%M%p';
  }
}

const MIN_GRANULARITY_DATAPOINTS = 7;
function getDefaultGranularityForTimeSpan(start: string | null, end: string | null): Granularity {
  // choose the largest granularity that would have more than $MIN_GRANULARITY_DATAPOINTS datapoints
  // calculate duration, and from there, figure out

  if (!start || !end) {
    // we won't be able to determine duration
    return DEFAULT_GRANULARITY;
  }

  // ms difference between start and end dates
  const startDate = dayjs(start);
  const endDate = dayjs(end);
  const duration = Math.abs(dayjs.duration(endDate.diff(startDate)).asMilliseconds());

  // sort granularities by desc duration
  const sortedGranularities = (Object.entries(Granularities) as Array<[Granularity, number]>).sort(
    ([, durationA], [, durationB]) => durationB - durationA
  );

  let currentGranularity: Granularity | null = null;
  for (const [granularity, granularityDuration] of sortedGranularities) {
    currentGranularity = granularity;

    if (duration / granularityDuration > MIN_GRANULARITY_DATAPOINTS) {
      // this granularity meets our minimum data points
      break;
    }
  }

  if (currentGranularity == null) {
    throw new Error('Could not determine granularity for timespan!');
  }

  return currentGranularity;
}

function getChartData(timeseriesData: TimeseriesResult[]): Partial<Plotly.PlotData>[] {
  if (timeseriesData.length <= 1) {
    return [];
  }

  const tz = dayjs.tz.guess();
  // Convert UTC timestamps to local-time strings; Plotly treats naive datetime strings as local time
  const x = timeseriesData.map((point: TimeseriesResult) =>
    dayjs.utc(point.timestamp).tz(tz).format('YYYY-MM-DDTHH:mm:ss')
  );
  const y = timeseriesData.map((point: TimeseriesResult) => point.result.count);

  return [
    {
      type: 'bar',
      x,
      y,
      name: '# Events',
      marker: { color: '#8e5ea2' },
    },
  ];
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

  function handleRelayout(eventData: Readonly<PlotRelayoutEvent>) {
    const xMin = (eventData as Record<string, unknown>)['xaxis.range[0]'] as string | undefined;
    const xMax = (eventData as Record<string, unknown>)['xaxis.range[1]'] as string | undefined;
    if (!xMin || !xMax) return;

    // Plotly does not update the values of executedQuery when the component rerenders,
    // so get it directly from the store when the handler is called.
    const { executedQuery } = useQueryStore.getState();

    // dayjs parses naive local-time strings as local time; .toISOString() gives UTC
    updateExecutedQuery({
      ...executedQuery,
      interval: 'custom',
      start: dayjs(xMin).toISOString(),
      end: dayjs(xMax).toISOString(),
    });
    // uirevision (keyed on start/end) resets Plotly's zoom state when the query re-renders
  }

  const chartData = getChartData(timeseriesData);

  const layout: Partial<Layout> = {
    height: 300,
    margin: { t: 0, r: 16, b: 60, l: 50 },
    xaxis: {
      type: 'date',
      tickformat: getDateFormatForGranularity(granularity),
      tickangle: -45,
      showgrid: true,
      title: { text: '' },
    },
    yaxis: {
      rangemode: 'tozero',
      tickformat: ',.0f',
      title: { text: '' },
    },
    showlegend: false,
    bargap: 0,
    // Changing uirevision resets Plotly's internal zoom/pan state when the query changes,
    // so the chart always shows the full new data range after re-fetching.
    uirevision: `${start}-${end}`,
  };

  const config: Partial<Config> = {
    displayModeBar: false,
    responsive: true,
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
            <Plot
              data={chartData}
              layout={layout}
              config={config}
              onRelayout={handleRelayout}
              style={{ width: '100%' }}
              useResizeHandler={true}
            />
          </EmptyOverlay>
        </div>
      </Spin>
    </Panel>
  );
};

export default Timeseries;
