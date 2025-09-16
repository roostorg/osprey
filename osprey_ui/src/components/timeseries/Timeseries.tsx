import * as React from 'react';
import { Select, Spin } from 'antd';
import Highcharts, { SeriesOptionsType } from 'highcharts';
import HighchartsReact from 'highcharts-react-official';
import moment from 'moment-timezone';
import shallow from 'zustand/shallow';

import { getTimeseriesQueryResults } from '../../actions/EventActions';
import useQueryStore from '../../stores/QueryStore';
import { TimeseriesResult } from '../../types/QueryTypes';
import Text, { TextSizes } from '../../uikit/Text';
import TimeseriesIcon from '../../uikit/icons/TimeseriesIcon';
import Panel from '../common/Panel';

import styles from './Timeseries.module.css';
import { makeEntityRoute } from '../../utils/RouteUtils';

const CHART_TYPE = 'column';
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
  // https://api.highcharts.com/class-reference/Highcharts#.dateFormat
  switch (granularity) {
    case 'minute':
      // 4:23PM
      return '%l:%M%p';
    case 'fifteen_minute':
    case 'thirty_minute':
    case 'hour':
      // Jun 1 4PM
      return '%b %e %l%p';
    case 'day':
    case 'week':
      // Jun 1, 2020
      return '%b %e, %Y';
    case 'month':
      // Jun 2020
      return '%b %Y';
    default:
      // Jun 1, 2020 4:23PM
      return '%b %e, %Y %l:%M%p';
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
  const startDate = moment(start);
  const endDate = moment(end);
  const duration = Math.abs(moment.duration(endDate.diff(startDate)).asMilliseconds());

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

function getChartData(timeseriesData: TimeseriesResult[]): SeriesOptionsType[] {
  if (timeseriesData.length <= 1) {
    return [];
  }

  const data = timeseriesData.map((point: TimeseriesResult) => [Date.parse(point.timestamp), point.result.count]);

  return [
    {
      type: CHART_TYPE,
      data: data,
      name: '# Events',
      color: '#8e5ea2',
      // By default, Highcarts groups multiple series for the same x together;
      // because we only have one data series, we don't want to have any padding between the lines.
      groupPadding: 0,
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

  function handleChartSelection(event: Highcharts.ChartSelectionContextObject): false {
    const newRange = event.xAxis?.[0];
    if (newRange == null) return false;

    // Highcharts does not update the values of executedQuery when the component rerenders,
    // so get it directly from the store when handler is called.
    const { executedQuery } = useQueryStore.getState();

    updateExecutedQuery({
      ...executedQuery,
      interval: 'custom',
      start: new Date(newRange.min).toISOString(),
      end: new Date(newRange.max).toISOString(),
    });
    // return false because we don't want highcharts to apply the zoom, as we'll just re-query the data instead
    return false;
  }

  const chartData = getChartData(timeseriesData);
  const chartOptions: Highcharts.Options = {
    chart: {
      type: CHART_TYPE,
      height: 300,
      // allows selection of a subset of date
      zoomType: 'x',
      events: {
        // event handler for the selection
        selection: handleChartSelection,
      },
    },
    title: {
      text: undefined,
    },
    credits: {
      enabled: false,
    },
    xAxis: {
      type: 'datetime',
      // adjusts granularity of each tick
      tickPixelInterval: 10,
      labels: {
        format: `{value:${getDateFormatForGranularity(granularity)}}`,
        rotation: -45,
      },
      // shows x-axis grid
      gridLineWidth: 1,
      title: {
        text: undefined,
      },
    },
    yAxis: {
      type: 'linear',
      // always start at 0 events
      min: 0,
      tickPixelInterval: 10,
      labels: {
        format: '{value:,.0f}',
      },
      title: {
        text: undefined,
      },
    },
    tooltip: {
      // date format for tooltip's x value (in our case, datetime)
      xDateFormat: getDateFormatForGranularity('other'),
    },
    time: {
      // our time data is UTC, but we'll convert it to whatever timezone moment
      // thinks the user is in
      timezone: moment.tz.guess(),
      // don't render time as UTC
      useUTC: false,
    },
    legend: {
      enabled: false,
    },
    series: chartData,
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
            <HighchartsReact highcharts={Highcharts} options={chartOptions} />
          </EmptyOverlay>
        </div>
      </Spin>
    </Panel>
  );
};

export default Timeseries;
