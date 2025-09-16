import * as React from 'react';
import { Modal, Form, Input } from 'antd';

import { updateSavedQuery } from '../../actions/SavedQueryActions';
import { SavedQuery } from '../../types/QueryTypes';
import Text, { TextSizes } from '../../uikit/Text';
import ModalFooter from '../common/ModalFooter';

interface EditSavedQueryNameModalProps {
  savedQuery: SavedQuery;
  onClose: () => void;
  onUpdate: (savedQuery: SavedQuery) => void;
}

const EditSavedQueryNameModal = ({ savedQuery, onClose, onUpdate }: EditSavedQueryNameModalProps) => {
  const [isSubmitting, setIsSubmitting] = React.useState(false);
  const [updatedName, setUpdatedName] = React.useState(savedQuery.name);

  const handleValuesChange = (changedValues: { [key: string]: string }) => {
    setUpdatedName(changedValues.name);
  };

  const handleUpdateQueryName = async () => {
    setIsSubmitting(true);
    const updatedSavedQuery = await updateSavedQuery(savedQuery.id, updatedName);
    setIsSubmitting(false);
    onUpdate(updatedSavedQuery);
    onClose();
  };

  return (
    <Modal
      onCancel={onClose}
      title={<Text size={TextSizes.H5}>Editing Query: {savedQuery.name}</Text>}
      footer={
        <ModalFooter
          isDisabled={updatedName === savedQuery.name}
          onOK={handleUpdateQueryName}
          onCancel={onClose}
          isSubmitting={isSubmitting}
        />
      }
      visible
    >
      <Form onValuesChange={handleValuesChange} onFinish={handleUpdateQueryName} layout="vertical">
        <Form.Item
          initialValue={savedQuery.name}
          label="New Name"
          name="name"
          rules={[{ required: true, message: 'Please enter new name for this query' }]}
        >
          <Input />
        </Form.Item>
      </Form>
    </Modal>
  );
};

export default EditSavedQueryNameModal;
