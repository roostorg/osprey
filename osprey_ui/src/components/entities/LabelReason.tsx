import * as React from 'react';

import { LabelReason as Reason } from '../../types/LabelTypes';
import Text, { TextColors, TextSizes, TextWeights } from '../../uikit/Text';
import { isTimestampPast, localizeAndFormatTimestamp } from '../../utils/DateUtils';
import Feature from '../common/Feature';
import RichDescription from './RichDescription';

import styles from './LabelReason.module.css';

const LabelReason = ({ reasonName, reason }: { reasonName: string; reason: Reason }) => {
  const renderReasonExpiry = () => {
    if (reason.expires_at == null) return null;
    const expiryCopy = isTimestampPast(reason.expires_at) ? 'Expired at' : 'Expires at';

    return (
      <Text size={TextSizes.SMALL} color={TextColors.LIGHT_SECONDARY} className={styles.expiresAt}>
        {expiryCopy}: {localizeAndFormatTimestamp(reason.expires_at)}
      </Text>
    );
  };

  return (
    <div className={styles.reasonCard} key={reasonName}>
      {reasonName.startsWith('_') ? (
        <Text weight={TextWeights.SEMIBOLD} color={TextColors.LIGHT_HEADINGS_PRIMARY}>
          {reasonName}
        </Text>
      ) : (
        <Feature value featureName={reasonName} isFeatureNameOnly />
      )}
      <RichDescription description={reason.description} features={reason.features} />
      <Text size={TextSizes.SMALL} color={TextColors.LIGHT_SECONDARY}>
        {localizeAndFormatTimestamp(reason.created_at)}
      </Text>
      {renderReasonExpiry()}
    </div>
  );
};

export default LabelReason;
