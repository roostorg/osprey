import { QueryRecord, SavedQuery, QueryState, QueryRecordRequest, TopNTable } from '../types/QueryTypes';
import HTTPUtils, { HTTPResponse } from '../utils/HTTPUtils';
import { formatQueryForRequest } from './QueryActions';

export async function createSavedQuery(queryId: string, name: string): Promise<SavedQuery> {
  /* eslint-disable-next-line */
  const response: HTTPResponse = await HTTPUtils.post('saved-queries', { query_id: queryId, name });
  if (!response.ok) {
    throw Error;
  }

  let data = response.data;
  if (Array.isArray(data.query.top_n)) {
    data.query.top_n = data.query.top_n.map(TopNTable.fromQueryParam);
  }

  return data;
}

export async function getSavedQueries(userEmail?: string, before?: string): Promise<SavedQuery[]> {
  const response: HTTPResponse = await HTTPUtils.get('saved-queries', {
    params: { before, user_email: userEmail } /* eslint-disable-line */,
  });

  if (!response.ok) {
    return [];
  }

  let data = response.data;

  return data.map((d: any) => {
    if (Array.isArray(d.query.top_n)) {
      d.query.top_n = d.query.top_n.map(TopNTable.fromQueryParam);
    }
    return d;
  });
}

export async function getSavedQuery(savedQueryId: string): Promise<SavedQuery> {
  const response: HTTPResponse = await HTTPUtils.get(`saved-queries/${savedQueryId}`);

  if (!response.ok) {
    throw Error('Saved query not found');
  }

  let data = response.data;
  if (Array.isArray(data.query.top_n)) {
    data.query.top_n = data.query.top_n.map(TopNTable.fromQueryParam);
  }

  return data;
}

export async function updateSavedQuery(
  savedQueryId: string,
  name: string,
  queryState?: QueryState
): Promise<SavedQuery> {
  const updateData: { name: string; query?: QueryRecordRequest } = { name };

  if (queryState != null) {
    updateData.query = formatQueryForRequest(queryState);
  }

  const response: HTTPResponse = await HTTPUtils.patch(`saved-queries/${savedQueryId}`, updateData);

  if (!response.ok) {
    throw Error('Error updating saved query');
  }

  let data = response.data;
  if (Array.isArray(data.query.top_n)) {
    data.query.top_n = data.query.top_n.map(TopNTable.fromQueryParam);
  }

  return data;
}

export async function getSavedQueryHistory(savedQueryId: string): Promise<QueryRecord[]> {
  const response: HTTPResponse = await HTTPUtils.get(`saved-queries/${savedQueryId}/history`);

  if (!response.ok) {
    return [];
  }

  let data = response.data;

  return data.map((d: any) => {
    if (Array.isArray(d.top_n)) {
      d.top_n = d.top_n.map(TopNTable.fromQueryParam);
    }
    return d;
  });
}

export async function deleteSavedQuery(savedQueryId: string): Promise<void> {
  const response: HTTPResponse = await HTTPUtils.delete(`saved-queries/${savedQueryId}`);

  if (!response.ok) {
    throw Error('Error deleting saved query');
  }

  return response.data;
}

export async function getSavedQueryUserEmails(): Promise<string[]> {
  const response: HTTPResponse = await HTTPUtils.get('saved-queries/user-emails');

  if (!response.ok) {
    return [];
  }

  return response.data;
}
