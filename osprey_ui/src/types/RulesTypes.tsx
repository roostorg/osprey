export interface RuleInfo {
  name: string;
  source_file: string;
  description: string;
  when_all: string[];
  referenced_features: string[];
  referenced_by_whenrules: number;
}

export interface RulesListResponse {
  rules: RuleInfo[];
  total: number;
  when_rules_total: number;
  unused_total: number;
}

export enum SortKey {
  Name = 'name',
  MostReferenced = 'most-referenced',
  LeastReferenced = 'least-referenced',
}

export interface RuleDraftValidationMessage {
  message: string;
  hint: string;
  source_path: string;
  line: number;
  column: number;
  rendered: string;
  identifier?: string | null;
  defined_in_source_paths?: string[];
}

export interface RuleDraftValidationResponse {
  ok: boolean;
  errors: RuleDraftValidationMessage[];
  warnings: RuleDraftValidationMessage[];
  suggested_imports?: string[];
}

export interface RuleDraftSourceResponse {
  path: string;
  contents: string;
}

export interface RuleDraftVocabularyFeature {
  name: string;
  source_path: string;
  source_line: number;
}

export interface RuleDraftVocabularyUdfArgument {
  name: string;
  type_name: string;
}

export interface RuleDraftVocabularyUdf {
  name: string;
  return_type: string;
  arguments: RuleDraftVocabularyUdfArgument[];
}

export interface RuleDraftVocabulary {
  features: RuleDraftVocabularyFeature[];
  udfs: RuleDraftVocabularyUdf[];
  effects: string[];
  source_files: string[];
}

export interface RuleDraftSubmitResponse {
  // Backend-neutral fields produced by every RuleSubmissionBackend.
  title: string;
  url: string | null;
  main_sml_updated: boolean;
  // Backend-specific extras (e.g., pr_number, branch for the GitHub backend;
  // path_on_disk for the local backend).
  [extra: string]: unknown;
}

export type ConditionOperator = '==' | '!=' | '>' | '<' | '>=' | '<=' | 'includes' | 'excludes';

export interface RuleBuilderCondition {
  feature: string;
  operator: ConditionOperator;
  rhs: string;
  rhsIsFeature: boolean;
}

export interface RuleBuilderOutcomeArg {
  name: string;
  value: string;
  isFeature: boolean;
}

export interface RuleBuilderOutcome {
  effect: string;
  args: RuleBuilderOutcomeArg[];
}

export interface RuleBuilderModel {
  ruleName: string;
  description: string;
  conditions: RuleBuilderCondition[];
  outcomes: RuleBuilderOutcome[];
}

export type ParseIntoBuilderResponse =
  | { supported: true; model: RuleBuilderModel }
  | { supported: false; reason: string };

export interface PendingDraft {
  title: string;
  url: string;
  author: string;
  created_at: string;
  touched_files: string[];
  [extra: string]: unknown;
}

export interface PendingDraftsResponse {
  pending: PendingDraft[];
  error?: string;
}
