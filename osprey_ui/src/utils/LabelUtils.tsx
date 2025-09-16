import { Label } from '../types/LabelTypes';

export function sortLabelsChronologically(labels: Label[]) {
  const labelsByLatestTimestamp: Array<[string, Label]> = labels.map((label) => {
    let latestTimestamp: string = '';

    Object.values(label.reasons).forEach((reason) => {
      if (latestTimestamp === '' || Date.parse(reason.created_at) - Date.parse(latestTimestamp) > 0) {
        latestTimestamp = reason.created_at;
      }
    });

    return [latestTimestamp, label];
  });

  const sortedLabels = labelsByLatestTimestamp
    .sort(([timestampA], [timestampB]) => Date.parse(timestampB) - Date.parse(timestampA))
    .map(([_, label]) => label);

  return sortedLabels;
}
