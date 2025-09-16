import styles from './BulkJobHistory.module.css';
import { BulkJobTask, FormattedBulkJobTask } from '../../types/BulkJobTypes';
import Text, { TextSizes, TextColors } from '../../uikit/Text';
import PropertyTable from '../common/PropertyTable';

interface BulkJobTitleProps {
  user: string;
  action: string;
  timestamp: Date;
}

export const BulkJobTitle = ({ user, action, timestamp }: BulkJobTitleProps) => {
  return (
    <div className={styles.cardTitle}>
      <Text size={TextSizes.SMALL} color={TextColors.LIGHT_HEADINGS_SECONDARY}>
        <b>{user}</b> {action} {timestamp.toString()}
      </Text>
    </div>
  );
};

interface BulkHistoryCardProps {
  task: BulkJobTask;
  index: number;
}

const BulkHistoryCard = ({ task, index }: BulkHistoryCardProps) => {
  if (task == null) return null;
  const formatted_task: FormattedBulkJobTask = new FormattedBulkJobTask(task);

  const body: Record<string, string> = {};
  // We need to use the FormattedBulkJobTask to make sure the properties are ordered for the UI.
  // Normal typescript properties in interfaces aren't consistently ordered :(
  for (const [key, val] of formatted_task.getMap()) {
    body[key] = val;
  }

  return (
    <div key={index} className={styles.bulkJobHistoryCard}>
      <BulkJobTitle user={task.initiated_by} action="created job on " timestamp={new Date(task.created_at * 1000)} />
      <PropertyTable
        className={styles.contentTable}
        shouldWrapText={true}
        entries={[body]}
        renderValue={([key, value]) => {
          if (key.toUpperCase().endsWith('FILTER')) {
            return (
              <a title="Click to view in Osprey" href={formatted_task.getQueryUrl()}>
                {value}
              </a>
            );
          }
          return value;
        }}
      />
    </div>
  );
};

export default BulkHistoryCard;
