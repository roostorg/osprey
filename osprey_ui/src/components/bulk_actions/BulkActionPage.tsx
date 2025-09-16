import styles from './BulkActionPage.module.css';
import { BulkActionJobTable } from './BulkActionTable';
import BulkActionStartModalContainer from './BulkActionStartModal';
import useBulkActionStore from '../../stores/BulkActionStore';
import { useEffect } from 'react';

export const BulkActionPage = () => {
  const { getJobs, jobs, cancelJob, jobPollingInProgress } = useBulkActionStore();

  useEffect(() => {
    let cleanup: (() => void) | undefined;

    getJobs().then((cleanupFn) => {
      if (cleanupFn) {
        cleanup = cleanupFn;
      }
    });

    return () => {
      if (cleanup) {
        cleanup();
      }
    };
  }, []);

  return (
    <div className={styles.viewContainer}>
      <div className={styles.content}>
        <div className={styles.header}>
          <span>Start a new Bulk Action Job</span>
          <BulkActionStartModalContainer />
        </div>
        <BulkActionJobTable jobs={jobs} onCancelJob={cancelJob} jobPollingInProgress={jobPollingInProgress} />
      </div>
    </div>
  );
};
