import * as React from 'react';

import { Colors } from '../../Constants';

interface ArrowIconProps {
  width?: number;
  height?: number;
  color?: string;
  className?: string;
}

const ArrowIcon = ({ width = 16, height = 16, color = Colors.ICON_PRIMARY, className }: ArrowIconProps) => {
  return (
    <svg
      className={className}
      width={width}
      height={height}
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
    >
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M4.29504 3.7051L8.87504 8.2951L4.29504 12.8851L5.70504 14.2951L11.705 8.2951L5.70504 2.2951L4.29504 3.7051Z"
        fill={color}
      />
    </svg>
  );
};

export default ArrowIcon;
