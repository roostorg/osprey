import * as React from 'react';

import { Colors } from '../../Constants';

interface ExpandIconProps {
  width?: number;
  height?: number;
  color?: string;
}

const ExpandIcon = ({ width = 16, height = 16, color = Colors.ICON_MUTED }: ExpandIconProps) => {
  return (
    <svg width={width} height={height} viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path d="M19 18C19 18.6 18.6 19 18 19H11V21H18C19.7 21 21 19.7 21 18V11H19V18Z" fill={color} />
      <path d="M5 6C5 5.4 5.4 5 6 5H13V3H6C4.3 3 3 4.3 3 6V13H5V6Z" fill={color} />
      <path d="M15 3V5H17.6L5 17.6V15H3V21H9V19H6.4L19 6.4V9H21V3H15Z" fill={color} />
    </svg>
  );
};

export default ExpandIcon;
