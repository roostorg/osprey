import * as React from 'react';
import classNames from 'classnames';

import OspreyButton from '../../uikit/OspreyButton';
import CloseCircleIcon from '../../uikit/icons/CloseCircleIcon';

import styles from './FilterPill.module.css';

interface FilterPillProps {
  filterName: string;
  count?: number;
  onClick?: (filterName: string) => void;
  isSelected: boolean;
  className?: string;
}

const FilterPill = ({ filterName, count, onClick, isSelected, className }: FilterPillProps) => {
  if (count === 0) return null;

  const handleFeatureClick = () => {
    onClick?.(filterName);
  };

  return (
    <OspreyButton
      onClick={handleFeatureClick}
      className={classNames(isSelected ? styles.filterPillSelected : styles.filterPill, className)}
    >
      {`${filterName} ${count != null ? `(${count.toLocaleString()})` : ''}`}
      {isSelected ? <CloseCircleIcon className={styles.closeCircleIcon} /> : null}
    </OspreyButton>
  );
};

export default FilterPill;
