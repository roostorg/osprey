import * as React from 'react';

interface QueryHistoryIconProps {
  width?: number;
  height?: number;
}

const QueryHistoryIcon = ({ width = 32, height = 32 }: QueryHistoryIconProps) => {
  return (
    <svg width={width} height={height} viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="16" cy="16" r="16" fill="#CAE0F9" />
      <g clipPath="url(#clip0)">
        <path
          d="M21.01 18H20.21L19.94 17.73C20.92 16.59 21.51 15.12 21.51 13.5C21.51 9.91 18.6 7 15.01 7C11.42 7 8.51 10 8.51 13.5H6L9.84 17.5L14 13.5H10.51C10.51 11 12.53 9 15.01 9C17.49 9 19.51 11.01 19.51 13.5C19.51 15.98 17.49 18 15.01 18C14.36 18 13.75 17.86 13.19 17.62L11.71 19.1C12.68 19.67 13.8 20 15.01 20C16.62 20 18.09 19.41 19.23 18.43L19.5 18.7V19.49L24.51 24.48L26 23L21.01 18Z"
          fill="#1227CE"
        />
      </g>
      <defs>
        <clipPath id="clip0">
          <rect width="24" height="24" fill="white" transform="translate(4 4)" />
        </clipPath>
      </defs>
    </svg>
  );
};

export default QueryHistoryIcon;
