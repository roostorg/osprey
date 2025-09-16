import usePromiseResult from '../../hooks/usePromiseResult';
import { renderFromPromiseResult } from '../../utils/PromiseResultUtils';
import { getBulkJob } from '../../actions/BulkJobActions';
import styles from './BulkJobHistory.module.css';
import BulkHistoryCard from './BulkHistoryCard';

interface BulkHistoryPanelProps {
  taskId: string | undefined;
}

const BulkHistorySingleView = ({ taskId }: BulkHistoryPanelProps) => {
  const viewContent = usePromiseResult(() => getBulkJob(taskId), [taskId]);
  return renderFromPromiseResult(viewContent, (bulkHistoryCards) => (
    <div className={styles.listContainer}>
      {bulkHistoryCards.map((value, index) => (
        <BulkHistoryCard task={value} index={index} />
      ))}
    </div>
  ));
};

export default BulkHistorySingleView;
