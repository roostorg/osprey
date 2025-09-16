import * as React from 'react';

interface QueryIconProps {
  width?: number;
  height?: number;
}

const QueryIcon = ({ width = 32, height = 32 }: QueryIconProps) => {
  return (
    <svg width={width} height={height} viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="16" cy="16" r="16" fill="#CAE0F9" />
      <circle cx="14" cy="14" r="8" stroke="#1227CE" strokeWidth="2" />
      <path d="M20.5 20L24.5 24" stroke="#1227CE" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
};

export default QueryIcon;
