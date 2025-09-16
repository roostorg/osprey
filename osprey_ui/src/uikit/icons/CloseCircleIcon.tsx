import * as React from 'react';

interface CloseCircleIconProps {
  width?: number;
  height?: number;
  className?: string;
}

const CloseCircleIcon = ({ width = 16, height = 16, className }: CloseCircleIconProps) => {
  return (
    <svg
      width={width}
      height={height}
      className={className}
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
    >
      <g clipPath="url(#clip0)">
        <path
          d="M8.028 1.33325C4.346 1.33325 1.36133 4.31792 1.36133 7.99992C1.36133 11.6819 4.346 14.6666 8.028 14.6666C11.71 14.6666 14.6947 11.6819 14.6947 7.99992C14.6947 4.31792 11.7093 1.33325 8.028 1.33325ZM11.166 10.1953L10.2233 11.1379L8.028 8.94326L5.83266 11.1379L4.89 10.1953L7.08466 7.99992L4.88933 5.80459L5.832 4.86259L8.02733 7.05792L10.2227 4.86259L11.1653 5.80459L8.97067 7.99992L11.166 10.1953Z"
          fill="#1227CE"
        />
      </g>
      <defs>
        <clipPath id="clip0">
          <rect width="16" height="16" fill="white" />
        </clipPath>
      </defs>
    </svg>
  );
};

export default CloseCircleIcon;
