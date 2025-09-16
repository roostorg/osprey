import { QueryRecord, QueryState, QueryRecordRequest, TopNTable } from '../types/QueryTypes';
import HTTPUtils, { HTTPResponse } from '../utils/HTTPUtils';

export function formatQueryForRequest({ executedQuery, sortOrder, topNTables }: QueryState): QueryRecordRequest {
  return {
    /* eslint-disable */
    query_filter: executedQuery.queryFilter,
    date_range: [executedQuery.start, executedQuery.end],
    top_n: [...topNTables.values()].map((table) => table.asQueryParam()),
    sort_order: sortOrder,
    /* eslint-enable */
  };
}

export async function validateQuery(queryFilter: string): Promise<boolean> {
  const response: HTTPResponse = await HTTPUtils.post('queries/validate', {
    query_filter: queryFilter /* eslint-disable-line */,
  });
  return response.ok;
}

export async function saveQueryToHistory({
  executedQuery,
  sortOrder,
  topNTables,
  charts,
}: QueryState): Promise<QueryRecord | void> {
  if (executedQuery == null) return;
  const response: HTTPResponse = await HTTPUtils.post(
    'queries/query',
    formatQueryForRequest({ executedQuery, sortOrder, topNTables, charts })
  );

  if (!response.ok) {
    return;
  }

  return response.data;
}

export async function getQueriesHistory(userEmail?: string, before?: string): Promise<QueryRecord[]> {
  const response: HTTPResponse = await HTTPUtils.get('queries', {
    params: { before, user_email: userEmail } /* eslint-disable-line */,
  });

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

export async function getQueryUserEmails(): Promise<string[]> {
  const response: HTTPResponse = await HTTPUtils.get('queries/user-emails');

  if (!response.ok) {
    return [];
  }

  return response.data;
}
