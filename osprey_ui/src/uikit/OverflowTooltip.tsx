import * as React from 'react';

import Tooltip, { TooltipSizes, TooltipColors } from './Tooltip';

interface OverflowTooltipProps {
  children: React.ReactNode;
  className?: string;
  tooltipContent: React.ReactNode;
  tooltipClassName?: string;
}

const OverflowTooltip = ({ children, className, tooltipContent, tooltipClassName }: OverflowTooltipProps) => {
  const [showTooltip, setShowTooltip] = React.useState(false);

  const divRef = React.useCallback((node: HTMLElement | null) => {
    if (node != null && node.scrollWidth > node.offsetWidth) {
      setShowTooltip(true);
    }
  }, []);

  return (
    <Tooltip
      className={tooltipClassName}
      content={tooltipContent}
      isDisabled={!showTooltip}
      size={TooltipSizes.SMALL}
      color={TooltipColors.BRAND}
    >
      <div ref={divRef} className={className}>
        {children}
      </div>
    </Tooltip>
  );
};

export default OverflowTooltip;
