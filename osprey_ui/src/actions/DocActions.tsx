import { UdfCategory } from '../types/DocTypes';
import HTTPUtils, { HTTPResponse } from '../utils/HTTPUtils';

export async function getUdfDocs(): Promise<UdfCategory[]> {
  const response: HTTPResponse = await HTTPUtils.get('docs/udfs');

  if (!response.ok) {
    return [];
  }

  return response.data.udf_categories;
}
