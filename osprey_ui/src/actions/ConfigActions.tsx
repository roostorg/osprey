import DefaultFeature from '../models/DefaultFeature';
import { ApplicationConfig } from '../stores/ApplicationConfigStore';
import { RawUIConfig } from '../types/ConfigTypes';
import HTTPUtils, { HTTPResponse } from '../utils/HTTPUtils';

export async function getApplicationConfig(): Promise<ApplicationConfig> {
  const response: HTTPResponse = await HTTPUtils.get('config');
  if (!response.ok) {
    throw new Error('Failed to load initial application config: ' + response.error.message);
  }

  const rawConfigData: RawUIConfig = response.data;
  const featureNameToEntityTypeMapping = new Map(Object.entries(rawConfigData.feature_name_to_entity_type_mapping));
  const entityToFeatureSetMapping = new Map();

  featureNameToEntityTypeMapping.forEach((val, key) => {
    const newFeatureSet = entityToFeatureSetMapping.has(val) ? [...entityToFeatureSetMapping.get(val), key] : [key];
    entityToFeatureSetMapping.set(val, new Set(newFeatureSet));
  });

  const knownFeatureCategories: { [key: string]: string[] } = {};

  rawConfigData.known_feature_locations.forEach((feature) => {
    const { source_path: category, name: featureName } = feature;

    if (knownFeatureCategories.hasOwnProperty(category)) {
      knownFeatureCategories[category].push(featureName);
    } else {
      knownFeatureCategories[category] = [featureName];
    }
  });

  return {
    defaultSummaryFeatures: rawConfigData.default_summary_features.map(
      (feature) => new DefaultFeature(feature.actions, feature.features)
    ),
    featureNameToEntityTypeMapping,
    featureNameToValueTypeMapping: new Map(Object.entries(rawConfigData.feature_name_to_value_type_mapping)),
    entityToFeatureSetMapping,
    externalLinks: new Map(Object.entries(rawConfigData.external_links)),
    labelInfoMapping: new Map(
      Object.entries(rawConfigData.label_info_mapping).map(([name, info]) => [
        name,
        {
          validFor: new Set(info.valid_for),
          connotation: info.connotation,
          description: info.description,
        },
      ])
    ),
    knownFeatureNames: new Set(rawConfigData.known_feature_locations.map((feature) => feature.name).sort()),
    knownFeatureCategories,
    knownActionNames: new Set(rawConfigData.known_action_names.sort()),
    currentUser: rawConfigData.current_user,
    ruleInfoMapping: new Map(Object.entries(rawConfigData.rule_info_mapping)),
  };
}
