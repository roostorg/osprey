import * as React from 'react';

import { Colors } from '../../Constants';

interface HorizontalMenuIconProps {
  width?: number;
  height?: number;
  color?: string;
}

const HorizontalMenuIcon = ({ width = 27, height = 24, color = Colors.ICON_MUTED }: HorizontalMenuIconProps) => {
  return (
    <svg width={width} height={height} viewBox="0 0 27 24" fill="none" xmlns="http://www.w3.org/2000/svg">
      <g clipPath="url(#clip0)">
        <path
          d="M5.45452 14C6.65951 14 7.63634 13.1046 7.63634 12C7.63634 10.8954 6.65951 10 5.45452 10C4.24954 10 3.27271 10.8954 3.27271 12C3.27271 13.1046 4.24954 14 5.45452 14Z"
          fill={color}
        />
        <path
          d="M13.0909 14C14.2959 14 15.2727 13.1046 15.2727 12C15.2727 10.8954 14.2959 10 13.0909 10C11.8859 10 10.9091 10.8954 10.9091 12C10.9091 13.1046 11.8859 14 13.0909 14Z"
          fill={color}
        />
        <path
          d="M20.7272 14C21.9322 14 22.909 13.1046 22.909 12C22.909 10.8954 21.9322 10 20.7272 10C19.5222 10 18.5454 10.8954 18.5454 12C18.5454 13.1046 19.5222 14 20.7272 14Z"
          fill={color}
        />
      </g>
      <defs>
        <clipPath id="clip0">
          <rect width="26.1818" height={height} fill="white" />
        </clipPath>
      </defs>
    </svg>
  );
};

export default HorizontalMenuIcon;
