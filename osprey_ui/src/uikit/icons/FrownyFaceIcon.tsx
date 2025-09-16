import * as React from 'react';

import { Colors } from '../../Constants';

interface FrownyFaceIconProps {
  width?: number;
  height?: number;
  color?: string;
}

const FrownyFaceIcon = ({ width = 14, height = 14, color = Colors.TEXT_LIGHT_PRIMARY }: FrownyFaceIconProps) => {
  return (
    <svg width={width} height={height} viewBox="0 0 14 14" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path
        d="M7 0C3.1 0 0 3.1 0 7C0 10.9 3.1 14 7 14C10.9 14 14 10.9 14 7C14 3.1 10.9 0 7 0ZM4.5 3C5.3 3 6 3.7 6 4.5C6 5.3 5.3 6 4.5 6C3.7 6 3 5.3 3 4.5C3 3.7 3.7 3 4.5 3ZM3 11C3 8.8 4.8 7 7 7C9.2 7 11 8.8 11 11H3ZM9.5 6C8.7 6 8 5.3 8 4.5C8 3.7 8.7 3 9.5 3C10.3 3 11 3.7 11 4.5C11 5.3 10.3 6 9.5 6Z"
        fill={color}
      />
    </svg>
  );
};

export default FrownyFaceIcon;
