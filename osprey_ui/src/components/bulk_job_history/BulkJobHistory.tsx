import * as React from 'react';

import { Input } from 'antd';
import SavedQueriesIcon from '../../uikit/icons/SavedQueriesIcon';
import IconHeader from '../common/IconHeader';
import Text, { TextSizes } from '../../uikit/Text';
import useErrorStore from '../../stores/ErrorStore';
import BulkHistorySingleView from './BulkHistorySingleView';
import BulkHistoryMultiView from './BulkHistoryMultiView';

import styles from './BulkJobHistory.module.css';

const BulkJobHistoryView = () => {
  const [taskId, setTaskId] = React.useState<string | undefined>();

  const clearErrors = useErrorStore((state) => state.clearErrors);

  const handleSearch = async (taskId: string) => {
    clearErrors();
    if (!isNaN(Number(taskId))) {
      setTaskId(taskId);
    }
  };

  return (
    <div className={styles.viewContainer}>
      <div className={styles.content}>
        <div className={styles.bulkHistoryContainer}>
          <div className={styles.header}>
            <IconHeader size={TextSizes.H4} icon={<SavedQueriesIcon />} title="Bulk Job History" />
            <Input.Search
              className={styles.searchBar}
              placeholder="Please enter a valid integer ID"
              onSearch={handleSearch}
              enterButton
            />
          </div>
          <BulkHistoryMultiView />
        </div>
        <div className={styles.compactBulkHistoryContainer}>
          <div className={styles.compactHeader}>
            <Text size={TextSizes.H5}>Find Single Bulk Job</Text>
          </div>
          <BulkHistorySingleView taskId={taskId} />
        </div>
      </div>
    </div>
  );
};

export default BulkJobHistoryView;
