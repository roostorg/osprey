import * as React from 'react';

import OspreyButton, { ButtonColors, ButtonWeights } from '../../uikit/OspreyButton';

import styles from './ModalFooter.module.css';

interface ModalFooterProps {
  onOK: () => void;
  onCancel: () => void;
  isSubmitting?: boolean;
  isDisabled?: boolean;
}

const ModalFooter = ({ onOK, onCancel, isSubmitting = false, isDisabled = false }: ModalFooterProps) => {
  return (
    <div className={styles.footer}>
      <OspreyButton key="cancel" onClick={onCancel} weight={ButtonWeights.BOLD}>
        Cancel
      </OspreyButton>
      <OspreyButton
        disabled={isDisabled}
        loading={isSubmitting}
        key="ok"
        color={ButtonColors.DARK_BLUE}
        weight={ButtonWeights.BOLD}
        onClick={onOK}
      >
        Apply
      </OspreyButton>
    </div>
  );
};

export default ModalFooter;
