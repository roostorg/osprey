import * as React from 'react';
import classNames from 'classnames';
import shallow from 'zustand/shallow';

import useQueryStore from '../../stores/QueryStore';
import OspreyButton, { ButtonColors, ButtonHeights } from '../../uikit/OspreyButton';
import Text, { TextSizes, TextWeights } from '../../uikit/Text';

import styles from './FeatureName.module.css';
import { TopNTable } from '../../types/QueryTypes';

interface FeatureNameProps {
  featureName: string;
}

const FeatureName = ({ featureName }: FeatureNameProps): React.ReactElement => {
  const [topNTables, updateTopNTables] = useQueryStore((state) => [state.topNTables, state.updateTopNTables], shallow);

  const handleAddTopNTable = (e: React.MouseEvent<HTMLButtonElement>) => {
    if (e.shiftKey) {
      const newTopNTables = new Map();
      // Maps retain ordering based on insertion; We are basing our insertion ordering off of that fact
      newTopNTables.set(featureName, new TopNTable(featureName));
      topNTables.forEach((table, key) => newTopNTables.set(key, table));
      updateTopNTables(newTopNTables);
    }
  };

  return (
    <OspreyButton
      className={classNames(styles.featureNameButton, { [styles.disabled]: topNTables.has(featureName) })}
      color={ButtonColors.LINK_GRAY}
      height={ButtonHeights.SHORT}
      onClick={handleAddTopNTable}
      title={featureName}
    >
      <Text className={styles.featureName} size={TextSizes.SMALL} weight={TextWeights.NORMAL}>
        {featureName}
      </Text>
    </OspreyButton>
  );
};

export default FeatureName;
