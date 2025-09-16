import * as React from 'react';
import { Link } from 'react-router-dom';

import { getQueryUserEmails } from '../../actions/QueryActions';
import OspreyButton, { ButtonColors } from '../../uikit/OspreyButton';
import Text, { TextSizes } from '../../uikit/Text';
import QueryHistoryIcon from '../../uikit/icons/QueryHistoryIcon';
import IconHeader from '../common/IconHeader';
import UserSelect from '../common/UserSelect';
import QueryHistoryList from '../query_history/QueryHistoryList';
import { SavedQueryCardTypes } from '../saved_queries/SavedQueryCard';
import SavedQueryList from '../saved_queries/SavedQueryList';

import { Routes } from '../../Constants';
import styles from './QueryHistory.module.css';

const QueryHistory = () => {
  const [userEmail, setUserEmail] = React.useState<string | undefined>();

  return (
    <div className={styles.viewContainer}>
      <div className={styles.content}>
        <div className={styles.queryHistoryContainer}>
          <div className={styles.header}>
            <IconHeader size={TextSizes.H4} icon={<QueryHistoryIcon />} title="Query History" />
            <div>
              <UserSelect onSelect={setUserEmail} userResolver={getQueryUserEmails} />
            </div>
          </div>
          <QueryHistoryList userEmail={userEmail} />
        </div>
        <div className={styles.compactSavedQueriesContainer}>
          <div className={styles.compactHeader}>
            <Text size={TextSizes.H5}>Saved Queries</Text>
          </div>
          <SavedQueryList cardType={SavedQueryCardTypes.COMPACT} />
          <div className={styles.footer}>
            <OspreyButton className={styles.viewDetailButton} color={ButtonColors.LINK_GRAY}>
              <Link to={Routes.SAVED_QUERIES}>View all saved queries</Link>
            </OspreyButton>
          </div>
        </div>
      </div>
    </div>
  );
};

export default QueryHistory;
