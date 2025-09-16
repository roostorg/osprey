import * as React from 'react';
import { useParams } from 'react-router-dom';

import { EntityViewParams } from '../../stores/EntityStore';
import Text, { TextColors, TextSizes, TextWeights } from '../../uikit/Text';
import BrandDot from '../../uikit/icons/BrandDot';

import styles from './EntityViewBar.module.css';

const EntityViewBar = () => {
  const { entityType, entityId } = useParams<EntityViewParams>();

  return (
    <div className={styles.entityViewBar}>
      <Text
        className={styles.entityType}
        size={TextSizes.LARGE}
        color={TextColors.LIGHT_HEADINGS_SECONDARY}
        weight={TextWeights.LIGHT}
      >
        {decodeURIComponent(entityType)}
      </Text>
      <Text
        className={styles.entityId}
        size={TextSizes.LARGE}
        color={TextColors.LIGHT_HEADINGS_PRIMARY}
        weight={TextWeights.BOLD}
      >
        {decodeURIComponent(entityId)}
      </Text>
      <BrandDot />
    </div>
  );
};

export default EntityViewBar;
