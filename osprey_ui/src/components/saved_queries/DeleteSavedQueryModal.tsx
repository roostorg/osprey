import * as React from 'react';
import { Modal } from 'antd';

import { deleteSavedQuery } from '../../actions/SavedQueryActions';
import useSavedQueryStore from '../../stores/SavedQueryStore';
import { SavedQuery } from '../../types/QueryTypes';
import Text, { TextSizes, TextWeights } from '../../uikit/Text';
import ModalFooter from '../common/ModalFooter';

interface DeleteSavedQueryModalProps {
  savedQuery: SavedQuery;
  onCancel: () => void;
}

const DeleteSavedQueryModal = ({ savedQuery, onCancel }: DeleteSavedQueryModalProps) => {
  const { savedQueries, updateSavedQueries } = useSavedQueryStore();
  const [isSubmitting, setIsSubmitting] = React.useState(false);

  const handleDeleteSavedQuery = async () => {
    setIsSubmitting(true);
    await deleteSavedQuery(savedQuery.id);

    const newSavedQueries = savedQueries.filter((query) => query.id !== savedQuery.id);
    updateSavedQueries(newSavedQueries);
    setIsSubmitting(false);

    onCancel();
  };

  return (
    <Modal
      title={<Text size={TextSizes.H5}>Deleting: {savedQuery.name}</Text>}
      footer={<ModalFooter onOK={handleDeleteSavedQuery} onCancel={onCancel} isSubmitting={isSubmitting} />}
      visible
    >
      <Text tag="span" weight={TextWeights.BOLD}>
        Hey! Listen!
      </Text>{' '}
      Deleting this saved query is permanent and global, please be sure you have the right one before pressing OK.
    </Modal>
  );
};

export default DeleteSavedQueryModal;
