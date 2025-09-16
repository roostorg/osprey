import * as React from 'react';
import classNames from 'classnames';

import useResizeObserver from '../hooks/useResizeObserver';
import ArrowIcon from './icons/ArrowIcon';

import { Colors } from '../Constants';
import styles from './Collapse.module.css';

interface CollapseProps {
  header: React.ReactNode;
  children: React.ReactNode;
  isActive: boolean;
  onClick: () => void;
  headerClassName?: string;
  contentClassName?: string;
  arrowWrapperClassName?: string;
  defaultArrowPosition?: boolean;
}

const Collapse = ({
  header,
  children,
  isActive,
  onClick,
  headerClassName,
  contentClassName,
  arrowWrapperClassName,
  defaultArrowPosition = true,
}: CollapseProps) => {
  const contentRef = React.useRef(null);
  const contentRect = useResizeObserver(contentRef.current);

  const maxHeight = contentRect?.height ?? 0;

  return (
    <div className={styles.__invalid_collapse}>
      {/* eslint-disable-next-line */}
      <div className={classNames(styles.header, headerClassName)} onClick={onClick}>
        <div
          className={classNames(
            defaultArrowPosition ? styles.arrowIconWrapperLeft : styles.arrowIconWrapperRight,
            arrowWrapperClassName
          )}
        >
          <ArrowIcon
            color={Colors.ICON_MUTED}
            className={classNames(styles.arrowIcon, { [styles.arrowIconActive]: isActive })}
          />
        </div>
        {header}
      </div>
      <div className={styles.contentWrapper} style={{ maxHeight: isActive ? maxHeight : 0 }}>
        <div ref={contentRef} className={classNames(styles.__invalid_content, contentClassName)}>
          {children}
        </div>
      </div>
    </div>
  );
};

export default Collapse;
