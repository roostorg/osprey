import { FeatureLocation } from '../../types/ConfigTypes';
import Tooltip, { TooltipSizes } from '../../uikit/Tooltip';
import FeatureName from '../common/FeatureName';

import styles from './EntityNameWithPopover.module.scss';

interface EntityNameWithPopoverProps {
  location: FeatureLocation | undefined;
  name: string;
}

const EntityNameWithPopover = ({ location, name }: EntityNameWithPopoverProps) => {
  const renderContent = () => {
    if (location) {
      return (
        <div className={styles.content}>
          <div className={styles.title}>
            <span className={styles.name}>{location.name}</span>
            <span className={styles.origin}>
              {location.source_path}:{location.source_line}
            </span>
          </div>
          <div className={styles.snippet}>{location.source_snippet}</div>
        </div>
      );
    } else {
      return (
        <div className={styles.content}>
          <div className={styles.title}>{name}</div>
          <div>Could not fetch feature locations. Do you have access to CanViewDocs ACL ability?</div>
        </div>
      );
    }
  };

  return (
    <div className={styles.root}>
      <FeatureName featureName={name} />
      <Tooltip content={renderContent()} size={TooltipSizes.SMALL}>
        <div>
          <span className={styles.trigger}>ⓘ</span>
        </div>
      </Tooltip>
    </div>
  );
};

export default EntityNameWithPopover;
