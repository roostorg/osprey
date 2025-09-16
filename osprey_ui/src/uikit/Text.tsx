import * as React from 'react';
import classNames from 'classnames';

import styles from './Text.module.css';

export const TextSizes = {
  H1: styles.h1,
  H2: styles.h2,
  H3: styles.h3,
  H4: styles.h4,
  H5: styles.h5,
  H6: styles.h6,
  H7: styles.h7,
  LARGE: styles.large,
  SMALL: styles.small,
  NORMAL: styles.normal,
  XSMALL: styles.xsmall,
};

export const TextWeights = {
  LIGHT: styles.light,
  NORMAL: styles.normal,
  SEMIBOLD: styles.semibold,
  BOLD: styles.bold,
};

export const TextColors = {
  LIGHT_PRIMARY: styles.primary,
  LIGHT_SECONDARY: styles.secondary,
  LIGHT_HEADINGS_SECONDARY: styles.lightHeadingsSecondary,
  LIGHT_HEADINGS_PRIMARY: styles.lightHeadingsPrimary,
  LINK_BLUE: styles.linkBlue,
};

interface TextProps {
  children: React.ReactNode;
  className?: string;
  size?: (typeof TextSizes)[keyof typeof TextSizes];
  color?: (typeof TextColors)[keyof typeof TextColors];
  weight?: (typeof TextWeights)[keyof typeof TextWeights];
  italic?: boolean;
  tag?: string | React.ComponentType<React.PropsWithChildren<{ className: string; style?: React.CSSProperties }>>;
}

const Text = ({ children, className, size, color, weight, tag: Tag = 'div', italic = false }: TextProps) => {
  return <Tag className={classNames(size, color, weight, { [styles.italic]: italic }, className)}>{children}</Tag>;
};

export default Text;
