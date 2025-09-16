import * as React from 'react';

import { Colors } from '../../Constants';

interface StarIconProps {
  width?: number;
  height?: number;
  color?: string;
}

const StarIcon = ({ width = 12, height = 12, color = Colors.ICON_PRIMARY }: StarIconProps) => {
  return (
    <svg width={width} height={height} viewBox="0 0 12 12" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path
        d="M6.66225 0.513353L7.68895 3.76459H11.1113C11.8813 3.76459 12.138 4.70573 11.5391 5.13353L8.71566 7.18694L9.74236 10.4382C9.99904 11.1226 9.14345 11.7215 8.54454 11.2938L5.97778 9.3259L3.15434 11.3793C2.55543 11.8071 1.7854 11.2082 1.95652 10.5237L2.98322 7.27249L0.330901 5.13353C-0.26801 4.70573 -0.0113338 3.76459 0.758695 3.76459H4.18105L5.20775 0.513353C5.46443 -0.171118 6.49113 -0.171118 6.66225 0.513353Z"
        fill={color}
      />
    </svg>
  );
};

export default StarIcon;
