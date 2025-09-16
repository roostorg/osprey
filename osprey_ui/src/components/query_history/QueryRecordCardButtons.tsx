import * as React from 'react';
import { Link } from 'react-router-dom';

import { QueryRecord, MomentRangeValues } from '../../types/QueryTypes';
import OspreyButton from '../../uikit/OspreyButton';
import StarIcon from '../../uikit/icons/StarIcon';
import { getSearchParamsForQueryRecord } from '../../utils/SearchParamUtils';

import { Routes } from '../../Constants';
import styles from './QueryRecordCardButtons.module.css';

interface QueryRecordCardButtonsProps {
  query: QueryRecord;
  interval: MomentRangeValues | null;
  intervalCopy: string;
  onSaveQuery?: () => void;
}

export const QueryRecordCardButtons = ({ query, interval, intervalCopy, onSaveQuery }: QueryRecordCardButtonsProps) => {
  const renderQueryIntervalButton = () => {
    if (interval == null) return null;
    return (
      <OspreyButton>
        <Link to={{ pathname: Routes.HOME, search: getSearchParamsForQueryRecord(query, true) }}>
          Query {intervalCopy}
        </Link>
      </OspreyButton>
    );
  };

  const renderSaveQueryButton = () => {
    if (onSaveQuery == null) return null;

    return (
      <button onClick={onSaveQuery} className={styles.saveQueryButton}>
        <StarIcon />
      </button>
    );
  };

  return (
    <div className={styles.buttonContainer}>
      <div className={styles.buttonsLeft}>
        <OspreyButton className={styles.originalQueryButton}>
          <Link to={{ pathname: Routes.HOME, search: getSearchParamsForQueryRecord(query) }}>View Original Query</Link>
        </OspreyButton>
        {renderQueryIntervalButton()}
      </div>
      {renderSaveQueryButton()}
    </div>
  );
};

export default QueryRecordCardButtons;
