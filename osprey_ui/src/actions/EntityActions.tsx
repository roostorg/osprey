import invariant from 'invariant';
import { sum } from 'lodash';

import useEntityStore from '../stores/EntityStore';
import { KeyedLabels, Label, LabelMutation, LabelMutationResult } from '../types/LabelTypes';
import { BaseQueryRequest, EventCountsByFeatureForEntityQuery } from '../types/QueryTypes';
import HTTPUtils, { HTTPResponse } from '../utils/HTTPUtils';
import { getBaseRequest } from './EventActions';

function transformLabels(labels: KeyedLabels): Label[] {
  return Object.keys(labels).map((labelName) => ({ ...labels[labelName], name: labelName }));
}

export async function getLabelsForEntity(entityId: string, entityType: string): Promise<Label[]> {
  const response: HTTPResponse = await HTTPUtils.get(`entities/labels`, {
    params: {
      entity_id: entityId,
      entity_type: entityType,
    },
  });

  if (response.ok && response.data != null) {
    return transformLabels(response.data.labels);
  }

  return [];
}

export async function getEventCountsByFeatureForEntityQuery(
  query: BaseQueryRequest,
  aggregationDimensions: string[]
): Promise<EventCountsByFeatureForEntityQuery> {
  const { entity, ...requestData } = getBaseRequest(query);
  invariant(entity != null, 'Event counts can only be fetched for entities');

  const response: HTTPResponse = await HTTPUtils.post(
    `entities/event-count-by-feature`,
    {
      ...requestData,
      granularity: 'all',
      aggregation_dimensions: aggregationDimensions /* eslint-disable-line */,
      entity,
    },
    {
      params: {
        entity_id: entity.id,
        entity_type: entity.type,
      },
    }
  );

  if (!response.ok) {
    return {};
  }

  useEntityStore.setState({ eventCountByFeature: response.data, totalEventCount: sum(Object.values(response.data)) });
  return response.data;
}

export async function updateLabelsForEntity(
  entityId: string,
  entityType: string,
  labels: LabelMutation[]
): Promise<LabelMutationResult> {
  const response: HTTPResponse = await HTTPUtils.post(
    `entities/labels`,
    {
      mutations: labels,
    },
    {
      params: {
        entity_id: entityId,
        entity_type: entityType,
      },
    }
  );

  if (!response.ok) {
    throw new Error('Label update request failed');
  }

  return { ...response.data, labels: transformLabels(response.data.labels) };
}
