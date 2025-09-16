import * as React from 'react';

import { Colors } from '../../Constants';

interface ManuallyRemovedIconProps {
  height?: number;
  width?: number;
  color?: string;
  className?: string;
}

const ManuallyRemovedIcon = ({
  height = 14,
  width = 19,
  color = Colors.TEXT_LIGHT_PRIMARY,
  className,
}: ManuallyRemovedIconProps) => {
  return (
    <svg
      className={className}
      width={width}
      height={height}
      viewBox="0 0 19 14"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
    >
      <g clipPath="url(#clip0)">
        <path
          d="M13.3301 4.29L13.8901 3.73C14.7501 2.87 14.7501 1.48 13.8901 0.619998C13.0601 -0.210002 11.6101 -0.210002 10.7801 0.619998L10.2101 1.19L13.3301 4.29Z"
          fill={color}
        />
        <path
          d="M12.3301 5.28999L5.94007 11.68C5.89007 11.73 5.83007 11.78 5.76007 11.81L1.52007 13.93C1.25007 14.07 0.920071 14.01 0.710071 13.8C0.500071 13.59 0.440071 13.26 0.580071 12.99L2.70007 8.74999C2.73007 8.67999 2.78007 8.61999 2.83007 8.56999L9.22007 2.17999L12.3301 5.28999Z"
          fill={color}
        />
        <path d="M18.66 9.06H12.66V11.06H18.66V9.06Z" fill={color} />
      </g>
      <defs>
        <clipPath id="clip0">
          <rect width="18.15" height="14" fill="white" transform="translate(0.51001)" />
        </clipPath>
      </defs>
    </svg>
  );
};

export default ManuallyRemovedIcon;
