import * as React from 'react';
import classNames from 'classnames';

import { Label, LabelStatus, LabelConnotation } from '../../types/LabelTypes';
import Text, { TextSizes, TextWeights } from '../../uikit/Text';
import ManuallyAddedIcon from '../../uikit/icons/ManuallyAddedIcon';
import ManuallyRemovedIcon from '../../uikit/icons/ManuallyRemovedIcon';
import { isTimestampPast } from '../../utils/DateUtils';

import { StatusColors } from '../../Constants';
import styles from './LabelTag.module.css';

const LabelConnotationClassName = {
  [LabelConnotation.POSITIVE]: styles.positive,
  [LabelConnotation.NEGATIVE]: styles.negative,
  [LabelConnotation.NEUTRAL]: styles.neutral,
};

const ConnotationToColorMapping = {
  [LabelConnotation.POSITIVE]: StatusColors.SUCCESS,
  [LabelConnotation.NEGATIVE]: StatusColors.ERROR,
  [LabelConnotation.NEUTRAL]: StatusColors.NEUTRAL,
};

interface LabelTagProps {
  label: Label;
  connotation: LabelConnotation;
  className?: string;
}

function isRemoved(status: LabelStatus): boolean {
  return status === LabelStatus.REMOVED || status === LabelStatus.MANUALLY_REMOVED;
}

function isManual(status: LabelStatus): boolean {
  return status === LabelStatus.MANUALLY_ADDED || status === LabelStatus.MANUALLY_REMOVED;
}

function isExpired(label: Label): boolean {
  return Object.values(label.reasons).every(
    (reason) => reason.expires_at != null && isTimestampPast(reason.expires_at)
  );
}

function getTagIcon(status: LabelStatus, color: string): React.ReactNode {
  if (!isManual(status)) return null;

  if (isRemoved(status)) {
    return <ManuallyRemovedIcon color={color} className={styles.manualIcon} />;
  }

  return <ManuallyAddedIcon color={color} className={styles.manualIcon} />;
}

const LabelTag = ({ label, connotation, className }: LabelTagProps): React.ReactElement => {
  return (
    <Text
      size={TextSizes.SMALL}
      weight={TextWeights.SEMIBOLD}
      className={classNames(
        styles.labelTag,
        LabelConnotationClassName[connotation],
        { [styles.dottedOutline]: isRemoved(label.status), [styles.grayedOut]: isExpired(label) },
        className
      )}
    >
      {getTagIcon(label.status, ConnotationToColorMapping[connotation])}
      {label.name}
    </Text>
  );
};

export default LabelTag;
