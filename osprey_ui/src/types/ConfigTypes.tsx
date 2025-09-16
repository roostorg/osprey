import { LabelConnotation } from './LabelTypes';

interface RawDefaultSummaryFeature {
  actions: string[];
  features: string[];
}

interface RawExternalLinkMapping {
  [entityType: string]: string;
}
interface RawLabelInfo {
  valid_for: string[];
  connotation: LabelConnotation;
  description: string;
}
interface RawLabelInfoMapping {
  [labelName: string]: RawLabelInfo;
}
interface RawFeatureNameToEntityTypeMapping {
  [featureName: string]: string;
}
interface RawFeatureNameToValueTypeMapping {
  [featureName: string]: string;
}
interface RawRuleInfoMapping {
  [ruleName: string]: string;
}

export interface FeatureLocation {
  name: string;
  source_path: string;
  source_line: number;
  source_snippet: string;
}

export interface RawUIConfig {
  default_summary_features: RawDefaultSummaryFeature[];
  feature_name_to_entity_type_mapping: RawFeatureNameToEntityTypeMapping;
  feature_name_to_value_type_mapping: RawFeatureNameToValueTypeMapping;
  external_links: RawExternalLinkMapping;
  known_feature_locations: FeatureLocation[];
  known_action_names: string[];
  label_info_mapping: RawLabelInfoMapping;
  rule_info_mapping: RawRuleInfoMapping;
  current_user: { email: string };
}
