import * as React from 'react';
import classNames from 'classnames';
import ReactDOM from 'react-dom';

import useResizeObserver from '../hooks/useResizeObserver';
import { getPopoutStyle, Placements } from '../utils/DOMUtils';

import styles from './PopoutBase.module.css';

interface PopoutBaseProps {
  isVisible: boolean;
  content: React.ReactNode;
  children: React.ReactNode;
  className?: string;
  outerClassName?: string;
  useArrow?: boolean;
  placement: Placements;
}

const PopoutBase = ({
  isVisible,
  content,
  children,
  className,
  outerClassName,
  placement,
  useArrow = false,
}: PopoutBaseProps) => {
  const [childNodeBoundingRect, setChildNodeBoundingRect] = React.useState<DOMRect | null>(null);
  const [popoutBaseRef, setPopoutBaseRef] = React.useState<HTMLElement | null>(null);

  const popoutBaseRefCallback = React.useCallback((node: HTMLElement | null) => {
    if (node != null) {
      setPopoutBaseRef(node);
    }
  }, []);
  const childNodeRef = React.useRef<HTMLDivElement>(null);
  const popoutClientRect = useResizeObserver(popoutBaseRef);

  React.useLayoutEffect(() => {
    if (childNodeRef.current == null) return;

    if (isVisible) {
      setChildNodeBoundingRect(childNodeRef.current.getBoundingClientRect());
    } else {
      setChildNodeBoundingRect(null);
    }
    setChildNodeBoundingRect(childNodeRef.current.getBoundingClientRect());
  }, [isVisible]);

  const renderPopoutBaseContent = () => {
    if (childNodeBoundingRect == null) return null;
    const { popoutStyle, arrowStyle } = getPopoutStyle(childNodeBoundingRect, popoutClientRect, placement);

    return (
      <div className={outerClassName} style={{ ...popoutStyle, position: 'absolute', zIndex: 2000 }}>
        <div className={classNames(styles.popoutBase, className)} ref={popoutBaseRefCallback}>
          {content}
        </div>
        {useArrow ? <div className={styles.arrow} style={arrowStyle} /> : null}
      </div>
    );
  };

  const renderPopoutBase = () => {
    if (!isVisible) return null;
    return ReactDOM.createPortal(renderPopoutBaseContent(), document.body);
  };

  return (
    <>
      <div ref={childNodeRef}>{children}</div>
      {renderPopoutBase()}
    </>
  );
};

export default PopoutBase;
