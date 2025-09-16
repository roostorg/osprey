import type { unitOfTime } from 'moment';
import { FeatureLocation } from './ConfigTypes';

interface IntervalOption {
  label: string;
  durationConstructor: [number, unitOfTime.DurationConstructor];
}

function intervalOption(duration: number, timeUnit: unitOfTime.DurationConstructor): IntervalOption {
  const timeUnitTitleCased = `${timeUnit[0].toUpperCase()}${timeUnit.substring(1)}`;
  const label = duration > 1 ? `Last ${duration} ${timeUnitTitleCased}s` : `Last ${timeUnitTitleCased}`;

  return {
    label,
    durationConstructor: [duration, timeUnit],
  };
}

export const IntervalOptions = {
  oneSecond: intervalOption(1, 'second'),
  tenMinutes: intervalOption(10, 'minute'),
  thirtyMinutes: intervalOption(30, 'minute'),
  hour: intervalOption(1, 'hour'),
  twoHours: intervalOption(2, 'hour'),
  fourHours: intervalOption(4, 'hour'),
  eightHours: intervalOption(8, 'hour'),
  twelveHours: intervalOption(12, 'hour'),
  day: intervalOption(1, 'day'),
  twoDays: intervalOption(2, 'day'),
  week: intervalOption(1, 'week'),
  twoWeeks: intervalOption(2, 'week'),
  month: intervalOption(1, 'month'),
  twoMonths: intervalOption(2, 'month'),
  threeMonths: intervalOption(3, 'month'),
};

export type MomentRangeValues = keyof typeof IntervalOptions;
export type DefaultIntervals = MomentRangeValues | 'custom' | null;

export interface BaseQuery {
  interval: DefaultIntervals;
  start: string;
  end: string;
  queryFilter: string;
}

export type BaseQueryRequest = BaseQuery & {
  entityFeatureFilters?: Set<string>;
};

export class Chart {
  query: string;
  deleted: boolean;

  constructor(query: string, deleted: boolean) {
    this.query = query;
    this.deleted = deleted;
  }

  static fromQueryParam(data: string): Chart {
    return new Chart(data, false);
  }
}

export class TopNTable {
  dimension: string;
  showPrecision: boolean;
  precision: number;
  isPoPEnabled: boolean;

  constructor(dimension: string, precision: number = 0, showPrecision: boolean = true, isPoPEnabled: boolean = false) {
    this.dimension = dimension;
    this.showPrecision = showPrecision;
    this.precision = precision;
    this.isPoPEnabled = isPoPEnabled;
  }

  asQueryParam() {
    let res = this.dimension;
    if (this.isPoPEnabled) {
      res += '.PoP';
    }
    if (this.precision === 0) {
      return res;
    }
    return res + ',' + String(this.precision);
  }

  static fromQueryParam(data: string | TopNTable): TopNTable {
    if (typeof data !== 'string') {
      return data;
    }

    let isPoPEnabled = false;
    if (data.includes('.PoP')) {
      isPoPEnabled = true;
      data = data.replaceAll('.PoP', '');
    }

    if (data.indexOf(',') != -1) {
      const splitData = data.split(',');

      let precision = Number.parseFloat(splitData[1]);
      if (isNaN(precision)) {
        precision = 0;
      }

      return new TopNTable(splitData[0], precision, true, isPoPEnabled);
    } else {
      return new TopNTable(data, undefined, undefined, isPoPEnabled);
    }
  }
}

export enum ScanQueryOrder {
  ASCENDING = 'ASCENDING',
  DESCENDING = 'DESCENDING',
}

export interface QueryState {
  executedQuery: BaseQuery;
  topNTables: Map<string, TopNTable>;
  sortOrder: ScanQueryOrder;
  charts: Array<Chart>;
}

interface EntityRequestData {
  id: string;
  type: string;
  feature_filters: string[] | null;
}

export interface QueryRequest {
  start: string;
  end: string;
  query_filter: string;
  entity: EntityRequestData | null;
}

export interface QueryRecordRequest {
  query_filter: string;
  date_range: [string, string];
  top_n: string[];
  sort_order: ScanQueryOrder;
}

export interface OspreyEvent {
  timestamp: string;
  id: string;
  extracted_features: { [key: string]: any };
}

export interface ScanResult {
  events: OspreyEvent[];
  offset: string | null;
  queryStart: string | null;
}

export type ScanQueryRequest = QueryRequest & {
  limit: number;
  next_page?: string;
  order: ScanQueryOrder;
};

export interface DimensionResult {
  count: number;
  [key: string]: string | number;
}

export interface TopNResult {
  timestamp: string;
  result: DimensionResult[];
}

export interface DimensionDifference {
  [key: string]: string | number;
  current_count: number;
  previous_count: number;
  difference: number;
  percentage_change: number;
}

export interface TopNComparison {
  differences: DimensionDifference[];
}

export interface TopNComparisonResult {
  comparison: TopNComparison[];
  current_period: TopNResult[];
  previous_period: TopNResult[];
}

export interface TimeseriesResult {
  timestamp: string;
  result: {
    count: number;
    [key: string]: number;
  };
}

export interface EventCountsByFeatureForEntityQuery {
  [featureName: string]: number;
}

export interface StoredExecutionResult {
  id: string;
  extracted_features: { [key: string]: any };
  timestamp: string;
  action_data: { [key: string]: any };
  error_traces: Array<{
    rules_source_location: string;
    traceback: string;
  }>;
}

export interface QueryRecord {
  id: string;
  parent_id?: string;
  executed_at: number;
  executed_by: string;
  query_filter: string;
  date_range: { start: string; end: string };
  top_n: TopNTable[];
  sort_order: ScanQueryOrder;
}

export interface SavedQuery {
  id: string;
  name: string;
  query_id: string;
  saved_by: string;
  saved_at: number;
  query: QueryRecord;
}

export type CustomSummaryFeatures = string[] | null;
export type EntityFeatureFilters = Set<string>;

export interface FeatureLocations {
  locations: Array<FeatureLocation> | undefined;
}
