import * as React from 'react';
import classNames from 'classnames';

import { Placements } from '../utils/DOMUtils';
import PopoutBase from './PopoutBase';
import Text, { TextWeights } from './Text';

import styles from './DropdownMenu.module.css';

interface BaseMenuOption {
  label: React.ReactNode;
  onClick?: () => void;
}

interface CustomMenuOption<T> {
  label: React.ReactNode;
  onClick?: () => void;
  data: T;
}

export type MenuOption<T = void> = T extends void ? BaseMenuOption : CustomMenuOption<T>;

interface DropdownMenuProps<T = void> {
  children: React.ReactNode;
  options: Array<MenuOption<T>>;
  renderOption?: (option: MenuOption<T>) => React.ReactNode;
  placement?: Placements;
  className?: string;
}

function DropdownMenu<T = void>({ children, options, renderOption, placement, className }: DropdownMenuProps<T>) {
  const [isDropdownVisible, setIsDropdownVisible] = React.useState(false);

  React.useEffect(() => {
    let isOpenClick = true;

    function handleWindowClick() {
      if (isOpenClick) {
        isOpenClick = false;
        return;
      }

      setIsDropdownVisible(false);
    }

    if (isDropdownVisible) {
      window.addEventListener('click', handleWindowClick);
    }

    return () => window.removeEventListener('click', handleWindowClick);
  }, [isDropdownVisible]);

  const handleShowDropdownMenu = () => {
    setIsDropdownVisible(true);
  };

  const renderMenuItem = (option: MenuOption<T>, index: number) => {
    if (renderOption != null) {
      return renderOption(option);
    }

    return (
      <button onClick={option.onClick} className={styles.dropdownMenuItem} key={index}>
        <Text weight={TextWeights.SEMIBOLD}>{option.label}</Text>
      </button>
    );
  };

  const renderDropdownMenu = () => {
    return <div className={classNames(styles.dropdownMenu, className)}>{options.map(renderMenuItem)}</div>;
  };

  return (
    // eslint-disable-next-line
    <span onClick={handleShowDropdownMenu}>
      <PopoutBase
        isVisible={isDropdownVisible}
        content={renderDropdownMenu()}
        placement={placement ?? Placements.BOTTOM_LEFT}
      >
        {children}
      </PopoutBase>
    </span>
  );
}

export default DropdownMenu;
