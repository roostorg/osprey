import create from 'zustand';

import DefaultFeature from '../models/DefaultFeature';
import { LabelConnotation } from '../types/LabelTypes';

/// similar to ConfigType's `LabelInfo`, but with camel casing.
export interface LabelInfo {
  validFor: Set<string>;
  connotation: LabelConnotation;
  description: string;
}

export type LabelInfoMapping = Map<string, LabelInfo>;
export type FeatureNameToEntityTypeMapping = Map<string, string>;
export type FeatureNameToValueTypeMapping = Map<string, string>;
export type EntityToFeatureSetMapping = Map<string, Set<string>>;
export type ExternalLinkMapping = Map<string, string>;
export type KnownFeatureCategoriesMapping = Map<string, string[]>;
export type OptionInfoMapping = Map<string, string>;

export interface FeatureLocation {
  name: string;
  source_path: string;
  source_line: number;
  source_snippet: string;
}

export interface KnownFeatureCategories {
  [category: string]: string[];
}

export interface ApplicationConfig {
  defaultSummaryFeatures: DefaultFeature[];
  featureNameToEntityTypeMapping: FeatureNameToEntityTypeMapping;
  featureNameToValueTypeMapping: FeatureNameToValueTypeMapping;
  entityToFeatureSetMapping: EntityToFeatureSetMapping;
  externalLinks: ExternalLinkMapping;
  labelInfoMapping: LabelInfoMapping;
  ruleInfoMapping: OptionInfoMapping;
  knownFeatureNames: Set<string>;
  knownFeatureCategories: KnownFeatureCategories;
  knownActionNames: Set<string>;
  currentUser: { email?: string };
  // Whether this deployment has a usable rules directory, and whether the current user
  // holds CAN_DEPLOY_RULES. Kept apart rather than collapsed into one "can I deploy?"
  // flag: the first hides the deploy control, the second disables it with a reason.
  ruleDeploymentEnabled: boolean;
  canDeployRules: boolean;
  // Whether the current user may author rule drafts. Gates the authoring entry points;
  // reading the rules catalog needs only CAN_VIEW_RULES.
  canEditRules: boolean;
}

type ApplicationConfigStore = {
  updateApplicationConfig: (config: ApplicationConfig) => void;
  isRecordingClicks: boolean;
} & ApplicationConfig;

const useApplicationConfigStore = create<ApplicationConfigStore>((set) => ({
  defaultSummaryFeatures: [],
  featureNameToEntityTypeMapping: new Map(),
  featureNameToValueTypeMapping: new Map(),
  entityToFeatureSetMapping: new Map(),
  externalLinks: new Map(),
  labelInfoMapping: new Map(),
  knownFeatureNames: new Set(),
  knownFeatureCategories: {},
  knownActionNames: new Set(),
  ruleInfoMapping: new Map(),
  updateApplicationConfig: (config: ApplicationConfig) => set(() => ({ ...config })),
  isRecordingClicks: false,
  currentUser: {},
  // Off until the real config lands, so nothing offers a deploy during the first render.
  ruleDeploymentEnabled: false,
  canDeployRules: false,
  canEditRules: false,
}));

export default useApplicationConfigStore;
