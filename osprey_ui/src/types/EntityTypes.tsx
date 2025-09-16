import { SortedLabels } from './LabelTypes';

export interface Entity {
  type: string;
  id: string;
  labels: SortedLabels;
  hasLabels: boolean;
}

export interface Feature {
  value: string | null;
  featureName: string;
}
