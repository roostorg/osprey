import * as React from 'react';

import { getSavedQueryUserEmails } from '../../actions/SavedQueryActions';
import { TextSizes } from '../../uikit/Text';
import SavedQueriesIcon from '../../uikit/icons/SavedQueriesIcon';
import IconHeader from '../common/IconHeader';
import UserSelect from '../common/UserSelect';
import { SavedQueryCardTypes } from './SavedQueryCard';
import SavedQueryList from './SavedQueryList';

import styles from './SavedQueries.module.css';

const SavedQueries = () => {
  const [userEmail, setUserEmail] = React.useState<string | undefined>();

  return (
    <div className={styles.viewContainer}>
      <div className={styles.savedQueriesContainer}>
        <div className={styles.header}>
          <IconHeader icon={<SavedQueriesIcon />} title="Saved Queries" size={TextSizes.H4} />
          <div>
            <UserSelect onSelect={setUserEmail} userResolver={getSavedQueryUserEmails} />
          </div>
        </div>
        <SavedQueryList userEmail={userEmail} cardType={SavedQueryCardTypes.DETAILED} />
      </div>
    </div>
  );
};

export default SavedQueries;
