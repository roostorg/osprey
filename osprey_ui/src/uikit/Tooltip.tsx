import * as React from 'react';
import classNames from 'classnames';

import { Placements } from '../utils/DOMUtils';
import PopoutBase from './PopoutBase';

import styles from './Tooltip.module.css';

export const TooltipSizes = {
  SMALL: styles.small,
  LARGE: styles.large,
};

export const TooltipColors = {
  DEFAULT: styles.default,
  BRAND: styles.brand,
};

interface BaseTooltipProps {
  children: React.ReactElement;
  content: React.ReactNode;
  className?: string;
  contentClassName?: string;
  size?: (typeof TooltipSizes)[keyof typeof TooltipSizes];
  color?: (typeof TooltipColors)[keyof typeof TooltipColors];
  placement?: Placements;
  defaultVisible?: boolean;
}

type IsDisabledFunction = (e: React.MouseEvent<HTMLElement>) => boolean;

type TooltipProps = BaseTooltipProps & {
  isDisabled?: boolean | IsDisabledFunction;
};

const Tooltip = ({
  children,
  content,
  isDisabled = false,
  className,
  contentClassName,
  placement = Placements.RIGHT_TOP,
  size = TooltipSizes.LARGE,
  color = TooltipColors.DEFAULT,
  defaultVisible = false,
}: TooltipProps) => {
  const [isTooltipVisible, setIsTooltipVisible] = React.useState(defaultVisible);
  const [isTooltipHovered, setIsTooltipHovered] = React.useState(defaultVisible);

  const isHovered = React.useRef(false);

  React.useEffect(() => {
    let timeoutId: null | NodeJS.Timeout = null;

    if (isTooltipVisible && !isTooltipHovered) {
      timeoutId = setTimeout(() => {
        if (!isHovered.current) {
          setIsTooltipVisible(false);
        }
      }, 150);
    }

    return () => {
      if (timeoutId != null) {
        clearTimeout(timeoutId);
      }
    };
  }, [isTooltipVisible, isTooltipHovered, defaultVisible]);

  const handleShowPopover = (e: React.MouseEvent<HTMLSpanElement>) => {
    if ((typeof isDisabled === 'function' && isDisabled(e)) || (typeof isDisabled === 'boolean' && isDisabled)) return;

    if (!isTooltipVisible) {
      setIsTooltipVisible(true);
    }

    setIsTooltipHovered(true);
    isHovered.current = true;
  };

  const handleHidePopover = () => {
    isHovered.current = false;
    setIsTooltipHovered(false);
  };

  const renderPopoverContent = () => {
    if (!isTooltipVisible) return null;

    return <div className={styles.tooltipContent}>{content}</div>;
  };

  return (
    <span className={contentClassName} onMouseEnter={handleShowPopover} onMouseLeave={handleHidePopover}>
      <PopoutBase
        outerClassName={color}
        className={classNames(size, className)}
        isVisible={isTooltipVisible}
        content={renderPopoverContent()}
        placement={placement}
        useArrow
      >
        {children}
      </PopoutBase>
    </span>
  );
};

export default Tooltip;
