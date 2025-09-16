import usePromiseResult from '../../hooks/usePromiseResult';
import { renderFromPromiseResult } from '../../utils/PromiseResultUtils';
import { getLastNBulkJobs } from '../../actions/BulkJobActions';
import styles from './BulkJobHistory.module.css';
import BulkHistoryCard from './BulkHistoryCard';

const BulkHistoryMultiView = () => {
  const viewContent = usePromiseResult(getLastNBulkJobs);

  return renderFromPromiseResult(viewContent, (bulkHistoryCards) => (
    <div className={styles.listContainer}>
      {bulkHistoryCards.map((value, index) => (
        <BulkHistoryCard task={value} index={index} />
      ))}
    </div>
  ));
};

export default BulkHistoryMultiView;
