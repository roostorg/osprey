import * as React from 'react';

interface ExitIconProps {
  width?: number;
  height?: number;
}

const ExitIcon = ({ width = 14, height = 14 }: ExitIconProps) => {
  return (
    <svg width={width} height={height} viewBox="0 0 14 14" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path
        d="M13.304 2.60933L11.418 0.723999L7.02801 5.11467L2.63734 0.723999L0.751343 2.60933L5.14201 7L0.751343 11.3907L2.63734 13.276L7.02801 8.88534L11.418 13.276L13.304 11.3907L8.91335 7L13.304 2.60933Z"
        fill="#535A65"
      />
    </svg>
  );
};

export default ExitIcon;
