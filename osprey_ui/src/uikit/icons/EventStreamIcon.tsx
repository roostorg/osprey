import * as React from 'react';

interface EventStreamIconProps {
  width?: number;
  height?: number;
}

const EventStreamIcon = ({ width = 32, height = 32 }: EventStreamIconProps) => {
  return (
    <svg width={width} height={height} viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="16" cy="16" r="16" fill="#CAE0F9" />
      <rect x="9" y="8" width="14" height="6" rx="1" stroke="#1227CE" strokeWidth="2" />
      <rect x="9" y="18" width="14" height="6" rx="1" stroke="#1227CE" strokeWidth="2" />
    </svg>
  );
};

export default EventStreamIcon;
