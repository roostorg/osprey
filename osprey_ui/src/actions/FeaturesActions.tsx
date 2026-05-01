import HTTPUtils, { HTTPResponse } from '../utils/HTTPUtils';
import { FeaturesListResponse } from '../types/FeaturesTypes';

export async function getFeaturesList(): Promise<FeaturesListResponse> {
  const response: HTTPResponse = await HTTPUtils.get('features');
  if (response.ok) {
    return response.data;
  }
  throw new Error('Failed to fetch features list');
}
