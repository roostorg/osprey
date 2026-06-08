import HTTPUtils, { HTTPResponse } from '../utils/HTTPUtils';
import { RulesListResponse } from '../types/RulesTypes';

export async function getRulesList(): Promise<RulesListResponse> {
  const response: HTTPResponse = await HTTPUtils.get('rules');
  if (response.ok) {
    return response.data;
  }
  throw new Error(response.error.message ?? 'Failed to fetch rules list');
}
