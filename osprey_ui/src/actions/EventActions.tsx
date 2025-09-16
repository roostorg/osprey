import { saveAs } from 'file-saver';
import { matchPath } from 'react-router';

import { history } from '../stores/QueryStore';
import { LabelMutation, LabelStatusAPIMapping } from '../types/LabelTypes';
import {
  TopNComparisonResult,
  ScanResult,
  ScanQueryRequest,
  TimeseriesResult,
  StoredExecutionResult,
  QueryRequest,
  ScanQueryOrder,
  BaseQueryRequest,
  FeatureLocations,
} from '../types/QueryTypes';
import HTTPUtils, { HTTPResponse } from '../utils/HTTPUtils';

import { BULK_LABEL_DEFAULT_LIMIT, Routes } from '../Constants';

export function getBaseRequest({
  start,
  end,
  queryFilter,
  entityFeatureFilters = new Set(),
}: BaseQueryRequest): QueryRequest {
  const entityViewMatch = matchPath<{ entityId: string; entityType: string }>(history.location.pathname, {
    path: Routes.ENTITY,
  });

  /* eslint-disable */
  const queryRequest: QueryRequest = {
    start,
    end,
    query_filter: queryFilter,
    entity: null,
  };

  if (entityViewMatch != null) {
    queryRequest.entity = {
      id: decodeURIComponent(entityViewMatch.params.entityId),
      type: decodeURIComponent(entityViewMatch.params.entityType),
      feature_filters: entityFeatureFilters.size > 0 ? [...entityFeatureFilters] : null,
    };
  }
  /* eslint-enable */

  return queryRequest;
}

export async function getTopNQueryResults(
  query: BaseQueryRequest,
  dimension: string | null,
  limit: number = 100,
  precision: number = 0
): Promise<TopNComparisonResult> {
  if (dimension === '' || dimension == null) {
    return { comparison: [], current_period: [], previous_period: [] };
  }

  const response = await HTTPUtils.post('events/topn', {
    ...getBaseRequest(query),
    dimension,
    limit,
    precision,
  });

  if (response.data.length > 0) {
    return response.data[0].result;
  }

  return response.data;
}

export async function getGroupByApproximateCountResults(
  query: BaseQueryRequest,
  dimension: string | null
): Promise<number | null> {
  if (dimension === '' || dimension == null) {
    return null;
  }

  const response = await HTTPUtils.post('events/groupby/approximate-count', {
    ...getBaseRequest(query),
    dimension,
  });
  return response.data?.count ?? null;
}

export async function getScanQueryResults(
  query: BaseQueryRequest,
  sortOrder: ScanQueryOrder,
  limit: number,
  offset?: string | null
): Promise<ScanResult> {
  const requestData: ScanQueryRequest = {
    ...getBaseRequest(query),
    order: sortOrder,
    limit,
  };

  if (offset != null) {
    requestData.next_page = offset; /* eslint-disable-line */
  }

  const response: HTTPResponse = await HTTPUtils.post('events/scan', requestData);

  if (!response.ok) return { events: [], offset: null, queryStart: null };
  return {
    events: response.data.events,
    offset: response.data.next_page,
    queryStart: response.data.next_page != null ? query.start : null,
  };
}

export async function getFeatureLocations(): Promise<FeatureLocations> {
  const response: HTTPResponse = await HTTPUtils.get('docs/feature-locations', {
    validateStatus: (status) => status === 200 || status === 401,
  });

  if (!response.ok || !response.data?.locations) return { locations: [] };
  return response.data;
}

export async function getTopNQueryResultCSV(query: BaseQueryRequest, dimension: string, limit: number): Promise<void> {
  const response: HTTPResponse = await HTTPUtils.post('events/topn/csv', {
    ...getBaseRequest(query),
    dimension,
    limit,
  });

  if (response.ok) {
    const fileBlob = new Blob([response.data], { type: 'text/csv' });
    saveAs(fileBlob, `osprey-${query.start}-${query.end}.csv`);
  } else {
    throw new Error(response.error.message);
  }
}

export async function getTimeseriesQueryResults(
  query: BaseQueryRequest,
  granularity: string
): Promise<TimeseriesResult[]> {
  const response: HTTPResponse = await HTTPUtils.post('events/timeseries', {
    ...getBaseRequest(query),
    granularity,
  });

  if (!response.ok) return [];
  return response.data;
}

export async function getDetailedEventFeatures(eventId: string): Promise<StoredExecutionResult> {
  const response: HTTPResponse = await HTTPUtils.get(`events/event/${eventId}`);

  if (!response.ok) throw new Error('Failed to load detailed event features: ' + response.error.message);
  return response.data;
}

export async function postTopBulkLabelTask(
  query: BaseQueryRequest,
  dimension: string,
  excludedEntities: Set<string>,
  entitiesToLabelCount: number,
  noLimit: boolean,
  labelMutation: LabelMutation
): Promise<number | null> {
  // The entity count is separate from the limiter; This check makes
  // sure that a limited job will expect to label the correct amount of entities.
  // If the expected count is more than 10% different from the actual count,
  // the job will fail downstream.
  if (!noLimit && entitiesToLabelCount > BULK_LABEL_DEFAULT_LIMIT) {
    entitiesToLabelCount = BULK_LABEL_DEFAULT_LIMIT;
  }
  /* eslint-disable */
  const response = await HTTPUtils.post('events/topn/bulk_label', {
    ...getBaseRequest(query),
    dimension,
    excluded_entities: [...excludedEntities],
    expected_entities: entitiesToLabelCount,
    no_limit: noLimit,
    label_name: labelMutation.label_name,
    label_status: LabelStatusAPIMapping[labelMutation.status],
    label_reason: labelMutation.reason,
    label_expiry: labelMutation.expires_at,
  });
  /* eslint-enable */
  return response.data?.task_id ?? null;
}
