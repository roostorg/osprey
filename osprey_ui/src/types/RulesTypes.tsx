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
