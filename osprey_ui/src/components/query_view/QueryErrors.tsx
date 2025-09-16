import * as React from 'react';
import { Alert } from 'antd';

import useErrorStore from '../../stores/ErrorStore';
import Text, { TextColors, TextWeights } from '../../uikit/Text';
import Tooltip from '../../uikit/Tooltip';
import { Placements } from '../../utils/DOMUtils';

import styles from './QueryErrors.module.css';

const QueryErrors: React.FC = () => {
  const errors = useErrorStore((state) => state.errors);

  const renderErrors = () => {
    const errorList = [...errors].map((error: string, index: number) => (
      <div className={styles.error} key={index}>
        {error}
      </div>
    ));

    return (
      <Tooltip placement={Placements.BOTTOM_LEFT} className={styles.errorTooltip} content={errorList} defaultVisible>
        <Text className={styles.errorCount} tag="span" color={TextColors.LINK_BLUE} weight={TextWeights.SEMIBOLD}>
          {`Error${errors.size > 1 ? 's' : ''}`}
        </Text>
      </Tooltip>
    );
  };

  return errors.size === 0 ? null : (
    <div className={styles.errorAlertWrapper}>
      <Alert
        type="error"
        message={
          <>
            {`${errors.size} `}
            {renderErrors()}
            {` Found`}
          </>
        }
      />
    </div>
  );
};

export default QueryErrors;
