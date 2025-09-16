import * as React from 'react';
import { Button } from 'antd';
import { ButtonProps } from 'antd/lib/button';
import classNames from 'classnames';

import styles from './OspreyButton.module.css';

export const ButtonColors = {
  DARK_BLUE: styles.brand,
  GHOST: styles.ghost,
  LINK_BRAND: styles.linkBrand,
  LINK_GRAY: styles.linkGray,
  LINK_DISABLED: styles.linkDisabled,
  LINK_BLUE: styles.linkBlue,
  DANGER_RED: styles.dangerRed,
};

export const ButtonHeights = {
  SHORT: styles.short,
  DEFAULT: styles.default,
  TALL: styles.tall,
};

export const ButtonWeights = {
  NORMAL: styles.normal,
  SEMIBOLD: styles.semibold,
  BOLD: styles.bold,
};

interface OspreyButtonProps {
  color?: (typeof ButtonColors)[keyof typeof ButtonColors];
  height?: (typeof ButtonHeights)[keyof typeof ButtonHeights];
  weight?: (typeof ButtonWeights)[keyof typeof ButtonWeights];
  textSelectable?: boolean;
}

const OspreyButton = ({
  className,
  color = ButtonColors.GHOST,
  height = ButtonHeights.DEFAULT,
  weight = ButtonWeights.SEMIBOLD,
  textSelectable = false,
  ...props
}: ButtonProps & OspreyButtonProps) => {
  return (
    <Button
      className={classNames(
        styles.ospreyButton,
        { [styles.selectable]: textSelectable },
        color,
        height,
        weight,
        className
      )}
      {...props}
    />
  );
};

export default OspreyButton;
