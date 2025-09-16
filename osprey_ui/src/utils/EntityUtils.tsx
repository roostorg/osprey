import * as React from 'react';

import Feature from '../components/common/Feature';
import EntityWithPopover from '../components/entities/EntityWithPopover';
import useApplicationConfigStore, { LabelInfoMapping } from '../stores/ApplicationConfigStore';
import { SortedLabels, Label, LabelConnotation } from '../types/LabelTypes';
import { FeatureLocation } from '../types/ConfigTypes';
import EntityNameWithPopover from '../components/entities/EntityNameWithPopover';
import FeatureName from '../components/common/FeatureName';

export function wrapEntityKeysWithFeatureLocationsMenu(
  featureName: string,
  featureLocations: Array<FeatureLocation> | undefined
) {
  const location = featureLocations?.find((location) => location.name === featureName);

  return <EntityNameWithPopover name={featureName} location={location} />;
}

export function wrapEntityValuesWithLabelMenu(value: unknown, featureName: string) {
  if (value == null) {
    return <Feature featureName={featureName} value={null} />;
  }

  const { featureNameToEntityTypeMapping } = useApplicationConfigStore.getState();
  const entityType = featureNameToEntityTypeMapping.get(featureName);

  return entityType != null ? (
    <EntityWithPopover featureName={featureName} entityId={String(value)} entityType={entityType} />
  ) : (
    <Feature featureName={featureName} value={value} />
  );
}

export function sortLabels(labels: Label[], labelInfoMapping: LabelInfoMapping): SortedLabels {
  const sortedLabels: SortedLabels = { positive: [], negative: [], neutral: [] };

  for (const label of labels) {
    const connotation: LabelConnotation = labelInfoMapping.get(label.name)?.connotation ?? LabelConnotation.NEUTRAL;
    sortedLabels[connotation].push(label);
  }

  return sortedLabels;
}
