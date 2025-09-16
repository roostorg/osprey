import React, { useState } from 'react';
import { Modal, Form, Input, Select, Upload, Button } from 'antd';
import { UploadOutlined } from '@ant-design/icons';
import type { UploadFile, UploadProps } from 'antd/es/upload/interface';
import type { FormInstance } from 'antd/es/form';
import styles from './BulkActionStartModal.module.css';
import { max, set } from 'lodash';
import useBulkActionStore from '../../stores/BulkActionStore';

const { TextArea } = Input;
const { Option } = Select;

interface WorkflowOption {
  value: string;
  label: string;
}

interface JobFormValues {
  jobName: string;
  jobDescription: string;
  workflow: string;
  customWorkflowInput?: string;
  file: File;
  entityType: string;
}

interface JobUploadModalProps {
  visible: boolean;
  onCancel: () => void;
  onSubmit: (values: JobFormValues) => void;
  initialValues?: Partial<JobFormValues>;
}

const workflowOptions: WorkflowOption[] = [
  { value: 'workflow1', label: 'Test Workflow' },
  { value: 'custom', label: 'Custom Workflow' },
];

const BulkActionStartModal: React.FC<JobUploadModalProps> = ({ visible, onCancel, onSubmit, initialValues }) => {
  const [form] = Form.useForm<JobFormValues>();
  const [customWorkflow, setCustomWorkflow] = useState<boolean>(false);
  const [fileSelected, setFileSelected] = useState<File | null>(null);

  const handleWorkflowChange = (value: string): void => {
    setCustomWorkflow(value === 'custom');
  };

  const handleSubmit = async (): Promise<void> => {
    try {
      const values: JobFormValues = (await form.validateFields()) as JobFormValues;
      if (fileSelected != null) {
        values.file = fileSelected;
      }
      onSubmit(values as JobFormValues);

      form.resetFields();
      setFileSelected(null);
      setCustomWorkflow(false);
    } catch (error) {
      console.error('Validation failed:', error);
    }
  };

  const normFile = (e: any): UploadFile[] => {
    if (Array.isArray(e)) {
      return e;
    }
    return e?.fileList || [];
  };

  // Custom validation rules
  const validationRules = {
    jobName: [
      { required: true, message: 'Please enter job name' },
      { max: 100, message: 'Job name cannot exceed 100 characters' },
    ],
    jobDescription: [
      { required: true, message: 'Please enter job description' },
      { max: 500, message: 'Job description cannot exceed 500 characters' },
    ],
    workflow: [{ required: true, message: 'Please select or enter workflow' }],
    customWorkflowInput: [
      { required: true, message: 'Please enter custom workflow' },
      { max: 100, message: 'Custom workflow cannot exceed 100 characters' },
    ],
    file: [],
    entityType: [{ required: true, message: 'Please select entity type' }],
  };

  // File upload configuration
  const uploadProps: UploadProps = {
    beforeUpload: (file) => {
      // ~10 million rows of bigint entity-ids in a CSV file would be around 250MB
      const isValidSize = file.size ? file.size / 1024 / 1024 / 1024 < 250 : false; // 250MB limit
      if (!isValidSize) {
        Modal.error({
          title: 'File too large',
          content: 'File size must be less than 250MB',
        });
      }
      setFileSelected(file);
      return false; // Prevent automatic upload
    },
    accept: '.csv', // Restrict file types,
    multiple: false,
    showUploadList: false,
    type: 'drag',
  };

  return (
    <Modal
      title="Create New Job"
      visible={visible}
      onCancel={() => {
        form.resetFields();
        setFileSelected(null);
        setCustomWorkflow(false);
        onCancel();
      }}
      onOk={handleSubmit}
      width={600}
      className={styles.modal}
    >
      <Form<JobFormValues> form={form} layout="vertical" requiredMark="optional" initialValues={initialValues}>
        <Form.Item name="jobName" label="Job Name" rules={validationRules.jobName}>
          <Input placeholder="Enter job name" />
        </Form.Item>

        <Form.Item name="jobDescription" label="Job Description" rules={validationRules.jobDescription}>
          <TextArea placeholder="Enter job description" rows={4} />
        </Form.Item>

        <Form.Item name="entityType" label="Entity Type" rules={validationRules.entityType}>
          <Select placeholder="Select entity type">
            <Option value="user">User</Option>
            <Option value="guild">Guild</Option>
          </Select>
        </Form.Item>

        <Form.Item name="workflow" label="Workflow" rules={validationRules.workflow}>
          <Select onChange={handleWorkflowChange} placeholder="Select workflow">
            {workflowOptions.map((option) => (
              <Option key={option.value} value={option.value}>
                {option.label}
              </Option>
            ))}
          </Select>
        </Form.Item>

        {customWorkflow && (
          <Form.Item name="customWorkflowInput" label="Custom Workflow" rules={validationRules.customWorkflowInput}>
            <Input placeholder="Enter custom workflow" />
          </Form.Item>
        )}

        <Form.Item name="file" label="Upload File" getValueFromEvent={normFile} rules={validationRules.file}>
          <Upload {...uploadProps}>
            <Button icon={<UploadOutlined />}>Click to Upload</Button>
          </Upload>
          <div>{fileSelected?.name}</div>
        </Form.Item>
      </Form>
    </Modal>
  );
};

const BulkActionStartModalContainer = () => {
  const [visible, setVisible] = useState(false);
  const bulkActions = useBulkActionStore();

  const handleOpenModal = () => {
    setVisible(true);
  };

  const handleCloseModal = () => {
    setVisible(false);
  };

  const handleSubmit = async (values: JobFormValues) => {
    const workflow = values.workflow === 'custom' ? values.customWorkflowInput : values.workflow;
    if (!workflow) {
      Modal.error({
        title: 'Workflow is required',
        content: 'Please select or enter a workflow',
      });
      return;
    }
    await bulkActions.startBulkAction({
      job_name: values.jobName,
      job_description: values.jobDescription,
      workflow_name: workflow,
      file_name: values.file.name,
      file: values.file,
      entity_type: values.entityType,
    });

    setVisible(false);

    await bulkActions.getJobs();
  };

  return (
    <>
      <Button type="primary" onClick={handleOpenModal}>
        Create New Job
      </Button>
      <BulkActionStartModal visible={visible} onCancel={handleCloseModal} onSubmit={handleSubmit} />
    </>
  );
};

export default BulkActionStartModalContainer;
