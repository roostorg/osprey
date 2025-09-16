import * as React from 'react';
import { Modal, Form, Input } from 'antd';

import { createSavedQuery } from '../../actions/SavedQueryActions';
import useSavedQueryStore from '../../stores/SavedQueryStore';
import Text, { TextSizes } from '../../uikit/Text';
import ModalFooter from '../common/ModalFooter';

interface SaveQueryModalProps {
  queryId: string;
  onCancel: () => void;
}

const SaveQueryModal = ({ queryId, onCancel }: SaveQueryModalProps) => {
  const { savedQueries, updateSavedQueries } = useSavedQueryStore();
  const [form] = Form.useForm();

  const handleSaveQuery = async () => {
    const name = form.getFieldValue('name');
    const newSavedQuery = await createSavedQuery(queryId, name);

    updateSavedQueries([newSavedQuery, ...savedQueries]);
    onCancel();
  };

  return (
    <Modal
      title={<Text size={TextSizes.H5}>Save Query</Text>}
      onCancel={onCancel}
      footer={<ModalFooter onOK={handleSaveQuery} onCancel={onCancel} />}
      visible
    >
      <Form form={form} onFinish={handleSaveQuery} layout="vertical">
        <Form.Item label="Name" name="name" rules={[{ required: true, message: 'Please name this query' }]}>
          <Input />
        </Form.Item>
      </Form>
    </Modal>
  );
};

export default SaveQueryModal;
