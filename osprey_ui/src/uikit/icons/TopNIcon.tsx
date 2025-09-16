import * as React from 'react';

interface TopNIconProps {
  width?: number;
  height?: number;
}

const TopNIcon = ({ width = 32, height = 32 }: TopNIconProps) => {
  return (
    <svg width={width} height={height} viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="16" cy="16" r="16" fill="#CAE0F9" />
      <rect x="7" y="9" width="18" height="2" rx="1" fill="#1227CE" />
      <rect x="7" y="15" width="14" height="2" rx="1" fill="#1227CE" />
      <rect x="7" y="21" width="10" height="2" rx="1" fill="#1227CE" />
    </svg>
  );
};

export default TopNIcon;
