import * as React from 'react';
import { Empty } from 'antd';
import classNames from 'classnames';

import { TextSizes } from '../../uikit/Text';
import IconHeader from './IconHeader';

import styles from './Panel.module.css';

interface PanelProps {
  children?: React.ReactNode;
  className?: string;
  title: string;
  titleRight?: React.ReactNode;
  icon: React.ReactElement;
  collapsible?: boolean;
}

const Panel = ({ children, className, title, icon: Icon, titleRight }: PanelProps) => {
  const panelContent = children == null ? <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} /> : children;

  return (
    <div className={classNames(styles.panel, className)}>
      <div className={styles.panelHeader}>
        <IconHeader className={styles.titleLeft} size={TextSizes.H5} icon={Icon} title={title} />
        {titleRight}
      </div>
      {panelContent}
    </div>
  );
};

export default Panel;
