import * as React from 'react';

import useEntityStore from '../../stores/EntityStore';
import useQueryStore from '../../stores/QueryStore';
import OspreyButton, { ButtonColors } from '../../uikit/OspreyButton';
import Text, { TextColors, TextSizes, TextWeights } from '../../uikit/Text';
import { pluralize } from '../../utils/StringUtils';

import styles from './FeatureFiltersDetailBar.module.css';

const FeatureFiltersDetailBar = () => {
  const entityFeatureFilters = useQueryStore((state) => state.entityFeatureFilters);
  const updateEntityFeatureFilters = useQueryStore((state) => state.updateEntityFeatureFilters);

  const eventCountByFeature = useEntityStore((state) => state.eventCountByFeature);
  const totalEventCount = useEntityStore((state) => state.totalEventCount);

  const eventsShown = Object.entries(eventCountByFeature).reduce((acc, [featureName, eventCount]) => {
    if (entityFeatureFilters.has(featureName)) {
      acc += eventCount;
    }

    return acc;
  }, 0);

  const handleClearFilters = () => {
    updateEntityFeatureFilters(new Set());
  };

  return (
    <div className={entityFeatureFilters.size === 0 ? styles.detailBarHidden : styles.detailBarShown}>
      <Text
        className={styles.filtersApplied}
        size={TextSizes.SMALL}
        weight={TextWeights.SEMIBOLD}
      >{`${entityFeatureFilters.size.toLocaleString()} ${pluralize(
        'filter',
        entityFeatureFilters.size
      )} applied`}</Text>
      <Text
        size={TextSizes.SMALL}
        weight={TextWeights.SEMIBOLD}
        color={TextColors.LIGHT_SECONDARY}
      >{`${eventsShown.toLocaleString()} ${pluralize(
        'event',
        eventsShown
      )} of ${totalEventCount.toLocaleString()}`}</Text>
      <OspreyButton onClick={handleClearFilters} color={ButtonColors.LINK_BRAND} className={styles.clearFilterButton}>
        Clear Filters
      </OspreyButton>
    </div>
  );
};

export default FeatureFiltersDetailBar;
