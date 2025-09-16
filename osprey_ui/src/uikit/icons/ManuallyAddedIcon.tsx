import * as React from 'react';

import { Colors } from '../../Constants';

interface ManuallyAddedIconProps {
  height?: number;
  width?: number;
  color?: string;
  className?: string;
}

const ManuallyAddedIcon = ({
  height = 16,
  width = 20,
  color = Colors.TEXT_LIGHT_PRIMARY,
  className,
}: ManuallyAddedIconProps) => {
  return (
    <svg
      className={className}
      width={width}
      height={height}
      viewBox="0 0 20 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
    >
      <g clipPath="url(#clip0)">
        <path d="M16.51 10.03V7.03H14.51V10.03H11.51V12.03H14.51V15.03H16.51V12.03H19.51V10.03H16.51Z" fill={color} />
        <path
          d="M13.3201 5.26L13.8801 4.7C14.7401 3.84 14.7401 2.45 13.8801 1.59C13.0501 0.759999 11.6001 0.759999 10.7701 1.59L10.2001 2.16L13.3201 5.26Z"
          fill={color}
        />
        <path
          d="M12.3201 6.25999L5.93006 12.65C5.88006 12.7 5.82006 12.75 5.75006 12.78L1.51006 14.9C1.24006 15.04 0.910061 14.98 0.700061 14.77C0.490061 14.56 0.430061 14.23 0.570061 13.96L2.69006 9.71999C2.72006 9.64999 2.77006 9.58999 2.82006 9.53999L9.21006 3.14999L12.3201 6.25999Z"
          fill={color}
        />
      </g>
      <defs>
        <clipPath id="clip0">
          <rect width="19.01" height="14.06" fill="white" transform="translate(0.5 0.970001)" />
        </clipPath>
      </defs>
    </svg>
  );
};

export default ManuallyAddedIcon;
