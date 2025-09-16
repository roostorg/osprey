import * as React from 'react';

interface TimeseriesIconProps {
  width?: number;
  height?: number;
}

const TimeseriesIcon = ({ width = 32, height = 32 }: TimeseriesIconProps) => {
  return (
    <svg width={width} height={height} viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path
        d="M16 32C24.8366 32 32 24.8366 32 16C32 7.16344 24.8366 0 16 0C7.16344 0 0 7.16344 0 16C0 24.8366 7.16344 32 16 32Z"
        fill="#CAE0F9"
      />
      <path
        d="M6 23L8.6 19.1C9.3 18 10.9 17.9 11.8 18.8L12.2 19.2C13.1 20.1 14.7 20 15.4 18.9L16.6 17.1C17.3 16 18.9 15.9 19.8 16.8C20.8 17.8 22.6 17.5 23.2 16.1L26 9"
        stroke="#1227CE"
        strokeWidth="2"
        strokeMiterlimit="10"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
};

export default TimeseriesIcon;
