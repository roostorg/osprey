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
  // The two independent things that both have to hold before deploying a rule draft can
  // succeed, reported separately because they want different UI. A deployment with no
  // rules directory has no deploy story at all and hides the control; a user without the
  // ability is looking at a deployment that does deploy, and is better served by a
  // disabled control that says why than by one that silently isn't there.
  rule_deployment_enabled: boolean;
  can_deploy_rules: boolean;
  // Whether the current user may author drafts at all. Separate from deploying because
  // the blast radius differs: a draft is reversible and private to the UI, a deploy is
  // neither. There is no deployment-level counterpart the way `rule_deployment_enabled`
  // pairs with `can_deploy_rules` — every deployment can hold drafts.
  can_edit_rules: boolean;
}
