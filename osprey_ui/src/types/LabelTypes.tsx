export enum LabelStatus {
  REMOVED,
  ADDED,
  MANUALLY_REMOVED,
  MANUALLY_ADDED,
}

export const LabelStatusAPIMapping: Record<LabelStatus, string> = {
  [LabelStatus.ADDED]: 'ADDED',
  [LabelStatus.REMOVED]: 'REMOVED',
  [LabelStatus.MANUALLY_ADDED]: 'MANUALLY_ADDED',
  [LabelStatus.MANUALLY_REMOVED]: 'MANUALLY_REMOVED',
};

export interface KeyedLabels {
  [labelName: string]: Label;
}

export interface KeyedReasons {
  [reasonName: string]: LabelReason;
}

export type SortedLabels = Record<LabelConnotation, Label[]>;

export interface LabelState {
  status: LabelStatus;
  reasons: KeyedReasons;
}

export interface Label {
  name: string;
  status: LabelStatus;
  // key here is the rule name.
  reasons: KeyedReasons;
  previous_states: LabelState[];
}

export interface LabelMutation {
  label_name: string;
  status: LabelStatus;
  reason: string;
  expires_at?: string;
}

export interface LabelMutationDetails {
  added: string[];
  updated: string[];
  removed: string[];
  unchanged: string[];
}

export interface LabelMutationResult {
  labels: Label[];
  mutation_result: LabelMutationDetails;
}

export interface EntityLabels {
  expires_at: Date | null;
  labels: KeyedLabels;
}

export interface LabelReason {
  pending: boolean;
  // For example, this could be `Signed up with suspicious domain {EmailDomain}`.
  description: string;
  // And this would be {"EmailDomain": "shadywebsite.com"}
  features: { [entityName: string]: string };
  created_at: string;
  expires_at: string | null;
}

export enum LabelConnotation {
  POSITIVE = 'positive',
  NEGATIVE = 'negative',
  NEUTRAL = 'neutral',
}

export const LabelConnotations: readonly LabelConnotation[] = [
  LabelConnotation.NEGATIVE,
  LabelConnotation.POSITIVE,
  LabelConnotation.NEUTRAL,
];
