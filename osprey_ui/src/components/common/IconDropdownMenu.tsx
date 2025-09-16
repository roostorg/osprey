import * as React from 'react';

import DropdownMenu, { MenuOption } from '../../uikit/DropdownMenu';
import OspreyButton, { ButtonColors } from '../../uikit/OspreyButton';
import HorizontalMenuIcon from '../../uikit/icons/HorizontalMenuIcon';

import styles from './IconDropdownMenu.module.css';

interface IconDropdownMenuProps {
  options: MenuOption[];
}

const IconDropdownMenu = ({ options }: IconDropdownMenuProps) => {
  return (
    <DropdownMenu options={options}>
      <OspreyButton className={styles.menuIcon} color={ButtonColors.LINK_GRAY}>
        <HorizontalMenuIcon />
      </OspreyButton>
    </DropdownMenu>
  );
};

export default IconDropdownMenu;
