import DefaultFeature from '../../models/DefaultFeature';
import { OspreyEvent } from '../../types/QueryTypes';

/**
 * Resolves which feature blocks to render in an EventStreamCard when the
 * user has not chosen a custom selection.
 *
 * Order of precedence:
 *   1. Any DefaultFeature whose action-name pattern matches the event's
 *      ActionName — the existing deployment-configured behavior.
 *   2. Fall back to a single block containing every key present on the
 *      event (except ActionName, which is already rendered as the card
 *      title). This is the "first-load isn't empty" default for fresh
 *      deployments that have not curated default_summary_features.
 */
export const getSummaryFeaturesForEvent = (
  event: OspreyEvent,
  defaultSummaryFeatures: DefaultFeature[]
): Array<readonly string[]> => {
  const actionName = event.extracted_features.ActionName;
  const matched = defaultSummaryFeatures.filter((f) => {
    return f.appliesTo(actionName);
  });

  if (matched.length > 0) {
    return matched.map((f) => {
      return f.features;
    });
  }

  const fallback = Object.keys(event.extracted_features).filter((key) => {
    return key !== 'ActionName';
  });

  if (fallback.length === 0) return [];

  return [fallback];
};
