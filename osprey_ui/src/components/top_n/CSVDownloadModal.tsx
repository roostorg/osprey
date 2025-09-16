import * as React from 'react';
import { DownloadOutlined } from '@ant-design/icons';
import { Alert, Form, Input, Modal, Radio } from 'antd';
import { Store } from 'antd/lib/form/interface';
import { RadioChangeEvent } from 'antd/lib/radio';

import { getTopNQueryResultCSV } from '../../actions/EventActions';
import useQueryStore from '../../stores/QueryStore';
import OspreyButton, { ButtonColors } from '../../uikit/OspreyButton';
import Text, { TextSizes } from '../../uikit/Text';

import styles from './CSVDownloadModal.module.css';

interface CSVDownloadModalProps {
  disabled: boolean;
  dimension: string;
}

enum RadioOptions {
  MAXIMUM,
  CUSTOM,
}

const MAXIMUM_ROWS = 100000;

function validateLimitInput(_: any, limitStr: string): Promise<void> {
  const limit = Number(limitStr);

  if (isNaN(limit) || limit <= 0) {
    return Promise.reject('Invalid number'); /* eslint-disable-line */
  } else if (limit > MAXIMUM_ROWS) {
    return Promise.reject('Maximum 100,000 rows allowed per CSV'); /* eslint-disable-line */
  }

  return Promise.resolve();
}

const CSVDownloadModal = ({ disabled, dimension }: CSVDownloadModalProps) => {
  const [isCSVDownloading, setIsCSVDownloading] = React.useState(false);
  const [isModalOpen, setIsModalOpen] = React.useState(false);
  const [limitOption, setLimitOption] = React.useState(RadioOptions.MAXIMUM);

  const executedQuery = useQueryStore((state) => state.executedQuery);
  const entityFeatureFilters = useQueryStore((state) => state.entityFeatureFilters);
  const [form] = Form.useForm();

  const handleDownload = async (values?: Store) => {
    const maybeLimit = Number(values?.limit);
    const limit = isNaN(maybeLimit) ? MAXIMUM_ROWS : maybeLimit;

    setIsCSVDownloading(true);
    setIsModalOpen(false);
    await getTopNQueryResultCSV({ ...executedQuery, entityFeatureFilters }, dimension, limit);
    setIsCSVDownloading(false);

    form.resetFields();
    setLimitOption(RadioOptions.MAXIMUM);
  };

  const handleRadioClick = (e: RadioChangeEvent) => {
    setLimitOption(e.target.value);
  };

  const renderLimitForm = () => {
    return (
      <Form form={form} onFinish={handleDownload}>
        <Form.Item name="limit" rules={[{ required: true }, { validator: validateLimitInput }]} validateFirst>
          <Input className={styles.limitInput} placeholder="Number of rows (maximum 100,000)" />
        </Form.Item>
      </Form>
    );
  };

  return (
    <>
      <OspreyButton
        color={ButtonColors.LINK_GRAY}
        onClick={() => setIsModalOpen(true)}
        disabled={disabled}
        loading={isCSVDownloading}
        key="download"
      >
        <DownloadOutlined />
        Download CSV
      </OspreyButton>
      <Modal
        title={<Text size={TextSizes.H5}>Download CSV</Text>}
        width={400}
        visible={isModalOpen}
        onCancel={() => setIsModalOpen(false)}
        footer={
          <div className={styles.footer}>
            <OspreyButton key="cancel" onClick={() => setIsModalOpen(false)}>
              Cancel
            </OspreyButton>
            <OspreyButton
              key="ok"
              color={ButtonColors.DARK_BLUE}
              onClick={limitOption === RadioOptions.CUSTOM ? form.submit : handleDownload}
            >
              OK
            </OspreyButton>
          </div>
        }
      >
        <Alert
          style={{ marginBottom: 12 }}
          message="Large CSV downloads can take several minutes."
          type="warning"
          showIcon
        />
        <Radio.Group value={limitOption} onChange={handleRadioClick}>
          <Radio className={styles.radioOption} value={RadioOptions.MAXIMUM}>
            Download Maximum Rows (up to 100,000)
          </Radio>
          <Radio className={styles.radioOption} value={RadioOptions.CUSTOM}>
            Set Row Limit
          </Radio>
        </Radio.Group>
        {limitOption === RadioOptions.CUSTOM ? renderLimitForm() : null}
      </Modal>
    </>
  );
};

export default CSVDownloadModal;
