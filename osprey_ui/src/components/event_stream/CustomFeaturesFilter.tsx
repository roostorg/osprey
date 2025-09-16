import * as React from 'react';

import Collapse from '../../uikit/Collapse';
import OspreyButton, { ButtonColors, ButtonWeights } from '../../uikit/OspreyButton';
import Text, { TextColors, TextSizes, TextWeights } from '../../uikit/Text';
import { pluralize } from '../../utils/StringUtils';
import FilterPill from '../common/FilterPill';

import styles from './CustomFeaturesFilter.module.css';

interface CustomFeaturesFilterProps {
  selectedCustomFeatures: Set<string>;
  useCustomFeatures: boolean;
  onClearFilters: () => void;
  onRemoveSelectedFeature: (features: Set<string>) => void;
}

const CustomFeaturesFilter = ({
  selectedCustomFeatures,
  useCustomFeatures,
  onClearFilters,
  onRemoveSelectedFeature,
}: CustomFeaturesFilterProps) => {
  const [showHidden, setShowHidden] = React.useState(false);

  const isFilterActive = selectedCustomFeatures.size > 0;

  const removeSelectedFeature = (feature: string) => {
    const updatedFeatures = new Set([...selectedCustomFeatures]);
    updatedFeatures.delete(feature);
    if (updatedFeatures.size === 0) {
      setShowHidden(false);
    }
    onRemoveSelectedFeature(updatedFeatures);
  };

  const clearFilters = (e: React.SyntheticEvent) => {
    e.stopPropagation();
    onClearFilters();
    setShowHidden(false);
  };

  const header = (
    <div className={styles.collapseHeader}>
      <Text size={TextSizes.SMALL} weight={TextWeights.SEMIBOLD} color={TextColors.LIGHT_PRIMARY}>
        {useCustomFeatures
          ? `${selectedCustomFeatures.size.toLocaleString()} ${pluralize(
              'filter',
              selectedCustomFeatures.size
            )} selected`
          : 'The default features shown vary by event type'}
      </Text>
      {isFilterActive ? (
        <OspreyButton
          onClick={clearFilters}
          color={ButtonColors.LINK_BRAND}
          className={styles.clearFilterButton}
          weight={ButtonWeights.BOLD}
        >
          Clear Filters
        </OspreyButton>
      ) : null}
    </div>
  );

  const renderSelectedCustomFeatures = () => {
    return [...selectedCustomFeatures].map((featureName) => (
      <FilterPill
        className={styles.filterPill}
        key={featureName}
        filterName={featureName}
        isSelected
        onClick={() => removeSelectedFeature(featureName)}
      />
    ));
  };

  return (
    <div className={styles.featureFilter}>
      <Collapse
        header={header}
        isActive={showHidden && isFilterActive}
        onClick={isFilterActive ? () => setShowHidden(!showHidden) : () => {}}
        contentClassName={showHidden ? styles.contentWrapperActive : styles.contentWrapper}
        arrowWrapperClassName={isFilterActive ? styles.arrowIconWrapper : styles.arrowIconWrapperHidden}
        defaultArrowPosition={false}
      >
        {renderSelectedCustomFeatures()}
      </Collapse>
    </div>
  );
};

export default CustomFeaturesFilter;
