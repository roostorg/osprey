import * as React from 'react';
import classNames from 'classnames';

import Text, { TextSizes } from '../../uikit/Text';

import styles from './IconHeader.module.css';

interface IconHeaderProps {
  icon: React.ReactElement;
  size: (typeof TextSizes)[keyof typeof TextSizes];
  title: string;
  className?: string;
}

const IconHeader = ({ icon, size, title, className }: IconHeaderProps) => {
  return (
    <div>
      <Text size={size} className={classNames(styles.header, className)}>
        <span className={styles.iconWrapper}>{icon}</span>
        {title}
      </Text>
    </div>
  );
};

export default IconHeader;
