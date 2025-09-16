import * as React from 'react';
import { Link } from 'react-router-dom';

import { SavedQuery, MomentRangeValues } from '../../types/QueryTypes';
import OspreyButton from '../../uikit/OspreyButton';
import { makeLatestSavedQueryRoute, makeSavedQueryRoute } from '../../utils/RouteUtils';

import styles from './SavedQueryCardButtons.module.css';

const SavedQueryCardButtons = ({
  savedQuery,
  interval,
  intervalCopy,
}: {
  savedQuery: SavedQuery;
  interval: MomentRangeValues | null;
  intervalCopy: string;
}) => {
  const renderQueryIntervalButton = () => {
    if (interval == null) return null;

    return (
      <OspreyButton>
        <Link to={{ pathname: makeLatestSavedQueryRoute(savedQuery) }}>Query {intervalCopy}</Link>
      </OspreyButton>
    );
  };

  return (
    <div className={styles.savedQueryCardButtons}>
      <OspreyButton className={styles.originalQueryButton}>
        <Link to={{ pathname: makeSavedQueryRoute(savedQuery) }}>View Original Query</Link>
      </OspreyButton>
      {renderQueryIntervalButton()}
    </div>
  );
};

export default SavedQueryCardButtons;
