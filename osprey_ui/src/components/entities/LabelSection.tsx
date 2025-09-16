import * as React from 'react';
import { isEmpty } from 'lodash';

import { LabelConnotation } from '../../types/LabelTypes';
import Text, { TextSizes, TextWeights } from '../../uikit/Text';
import FrownyFaceIcon from '../../uikit/icons/FrownyFaceIcon';
import NeutralFaceIcon from '../../uikit/icons/NeutralFaceIcon';
import SmileyFaceIcon from '../../uikit/icons/SmileyFaceIcon';

import styles from './LabelSection.module.css';

const LabelConnotationIcons = {
  [LabelConnotation.NEGATIVE]: <FrownyFaceIcon />,
  [LabelConnotation.POSITIVE]: <SmileyFaceIcon />,
  [LabelConnotation.NEUTRAL]: <NeutralFaceIcon />,
};

interface LabelSectionProps {
  connotation: LabelConnotation;
  children: React.ReactNode;
}

const LabelSection = ({ connotation, children }: LabelSectionProps) => {
  if (isEmpty(children) || children == null) return null;

  return (
    <div className={styles.labelSection}>
      <div className={styles.labelConnotationContainer}>
        <Text size={TextSizes.SMALL} weight={TextWeights.SEMIBOLD} className={styles.labelConnotation}>
          <span className={styles.connotationIcon}>{LabelConnotationIcons[connotation]}</span>
          {connotation}
        </Text>
      </div>
      {children}
    </div>
  );
};

export default LabelSection;
