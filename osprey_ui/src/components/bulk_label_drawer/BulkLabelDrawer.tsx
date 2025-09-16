import * as React from 'react';
import { ExclamationCircleOutlined } from '@ant-design/icons';
import { Drawer, notification, Modal } from 'antd';
import shallow from 'zustand/shallow';

import { getGroupByApproximateCountResults, postTopBulkLabelTask } from '../../actions/EventActions';
import useLabelStore from '../../stores/LabelStore';
import useQueryStore from '../../stores/QueryStore';
import { LabelMutation } from '../../types/LabelTypes';
import LabelForm from '../common/LabelForm';
import EntityDrawer from '../entities/LabelDrawer';

import styles from './BulkLabelDrawer.module.css';

interface BulkLabelDrawerContentProps {
  onCancel: (supressConfirmationModal: boolean) => void;
  onEntityUnchecked: (entityUnchecked: boolean) => void;
}

const BulkLabelDrawerContent = ({ onCancel, onEntityUnchecked }: BulkLabelDrawerContentProps) => {
  const [bulkLabelEntityType, bulkLabelFeatureName] = useLabelStore(
    (state) => [state.bulkLabelEntityType, state.bulkLabelFeatureName],
    shallow
  );
  const executedQuery = useQueryStore((state) => state.executedQuery);
  const entityFeatureFilters = useQueryStore((state) => state.entityFeatureFilters);
  const [entityCount, setEntityCount] = React.useState(0);
  const [entityCountString, setEntityCountString] = React.useState('[CALCULATING...]');
  const calculateApproximateCount = () => {
    getGroupByApproximateCountResults({ ...executedQuery, entityFeatureFilters }, bulkLabelFeatureName)
      .then((value) => {
        if (!value) {
          setEntityCountString('[COULD NOT CALCULATE]');
        } else {
          setEntityCountString(value.toLocaleString());
          setEntityCount(value);
        }
      })
      .catch((err) => {
        console.error(err);
        setEntityCountString('[COULD NOT CALCULATE]');
      });
  };
  React.useEffect(calculateApproximateCount, []);

  const [showChildDrawer, setShowChildDrawer] = React.useState(false);

  const handleChildDrawerClose = () => {
    setShowChildDrawer(false);
  };

  const handleSubmit = async (labelMutation: LabelMutation, noLimit: boolean) => {
    if (bulkLabelFeatureName == null) return;

    noLimit = noLimit ?? false;
    const taskId = await postTopBulkLabelTask(
      { ...executedQuery, entityFeatureFilters },
      bulkLabelFeatureName,
      new Set(), // excluded entities; removing for optimization purposes
      entityCount,
      noLimit,
      labelMutation
    );

    if (taskId === null) {
      notification.error({
        message: 'Bulk Label Task Failed to Start',
        description: `Task Failed`,
      });
    } else {
      notification.success({
        message: 'Bulk Label Task Started',
        description: `A bulk label task with id ${taskId} has been started.`,
      });
      onCancel(true);
    }
  };

  return (
    <>
      <>
        <div className={styles.__invalid_headerLeft}>
          <ExclamationCircleOutlined className={styles.exclamationIcon} />
          <span>This contains approximately {entityCountString} unique entities.</span>
        </div>
        <LabelForm
          entityType={bulkLabelEntityType}
          onCancel={() => onCancel(false)}
          onSubmit={(labelMutation, no_limit) => handleSubmit(labelMutation, no_limit)}
          isBulkLabel={true}
        />
      </>
      <EntityDrawer childDrawerProps={{ isVisible: showChildDrawer, onDrawerClose: handleChildDrawerClose }} />
    </>
  );
};

const BulkLabelDrawer: React.FC = () => {
  const [showBulkLabelDrawer, updateShowBulkLabelDrawer] = useLabelStore(
    (state) => [state.showBulkLabelDrawer, state.updateShowBulkLabelDrawer],
    shallow
  );

  const [showConfirmModal, setShowConfirmModal] = React.useState(false);
  const [entitiesHaveBeenUnchecked, setEntitiesHaveBeenUnchecked] = React.useState(false);

  const handleDrawerClose = (supressConfirmationModal: boolean) => {
    if (!entitiesHaveBeenUnchecked || supressConfirmationModal) {
      updateShowBulkLabelDrawer(false, null, null);
    } else {
      setShowConfirmModal(true);
    }
  };

  const handleConfirmationOk = () => {
    setShowConfirmModal(false);
    updateShowBulkLabelDrawer(false, null, null);
  };

  const handleConfirmationCancel = () => {
    setShowConfirmModal(false);
  };

  return (
    <Drawer
      width={450}
      title="Bulk Edit Labels"
      visible={showBulkLabelDrawer}
      onClose={() => {
        handleDrawerClose(true);
      }}
      destroyOnClose
    >
      <BulkLabelDrawerContent onCancel={handleDrawerClose} onEntityUnchecked={setEntitiesHaveBeenUnchecked} />
      <Modal
        title="Are you sure you want to close?"
        visible={showConfirmModal}
        onOk={handleConfirmationOk}
        onCancel={handleConfirmationCancel}
      >
        <p>You have some entities deselected and you'll lose the state if you close!</p>
      </Modal>
    </Drawer>
  );
};

export default BulkLabelDrawer;
