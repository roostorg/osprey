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
}));

export default useApplicationConfigStore;
