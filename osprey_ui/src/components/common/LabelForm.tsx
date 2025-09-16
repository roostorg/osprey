import * as React from 'react';
import { Alert, Checkbox, Form, Input, Select } from 'antd';
import { Store } from 'antd/lib/form/interface';
import moment from 'moment-timezone';

import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import { LabelStatus, LabelMutation, Label } from '../../types/LabelTypes';
import { IntervalOptions } from '../../types/QueryTypes';
import OspreyButton, { ButtonColors } from '../../uikit/OspreyButton';
import Text, { TextSizes, TextWeights } from '../../uikit/Text';

import styles from './LabelForm.module.css';

const ExpiryOptions = {
  ONE_SECOND: 'oneSecond',
  ONE_HOUR: 'hour',
  ONE_DAY: 'day',
  ONE_WEEK: 'week',
  TWO_WEEKS: 'twoWeeks',
  ONE_MONTH: 'month',
  TWO_MONTHS: 'twoMonths',
  THREE_MONTHS: 'threeMonths',
  PERMANENT: 'permanent',
};

interface LabelFormProps {
  label?: Label;
  title?: React.ReactNode;
  onCancel: () => void;
  onSubmit: (labelMutation: LabelMutation, noLimit: boolean) => void;
  entityType?: string | null;
  isSubmitting?: boolean;
  isBulkLabel?: boolean;
}

const LabelForm = ({
  label,
  title,
  onSubmit,
  onCancel,
  entityType,
  isSubmitting = false,
  isBulkLabel = false,
}: LabelFormProps) => {
  const labelInfoMapping = useApplicationConfigStore((state) => state.labelInfoMapping);
  const isAddLabelForm = label == null;
  const initialExpiry = isAddLabelForm ? ExpiryOptions.PERMANENT : ExpiryOptions.TWO_WEEKS;

  const [expiry, setExpiry] = React.useState(initialExpiry);
  const [checked, setChecked] = React.useState(false);
  const [noLimit, setNolimit] = React.useState(false);
  const [labelStatus, setLabelStatus] = React.useState(LabelStatus.MANUALLY_ADDED);

  const [form] = Form.useForm();

  const handleSubmit = (values: Store) => {
    const name = label?.name ?? values.name;
    const status = isAddLabelForm ? values.status : LabelStatus.MANUALLY_REMOVED;

    const labelMutation: LabelMutation = {
      label_name: name /* eslint-disable-line */,
      status,
      reason: values.reason,
    };

    if (values.expires_at !== ExpiryOptions.PERMANENT) {
      const expiresAt: keyof Partial<typeof IntervalOptions> = values.expires_at;
      const {
        durationConstructor: [number, unit],
      } = IntervalOptions[expiresAt];
      /* eslint-disable-next-line */
      labelMutation.expires_at = moment.utc().add(number, unit).format();
    }

    onSubmit(labelMutation, noLimit);
  };

  const getSelectOptionsForEntity = (): Array<{ label: string; value: string }> => {
    const validLabels: Array<{ label: string; value: string }> = [];
    if (entityType == null) return validLabels;

    labelInfoMapping.forEach((value, key) => {
      if (value.validFor.has(entityType)) {
        validLabels.push({ value: key, label: key });
      }
    });

    return validLabels;
  };

  const renderAddFormFields = () => {
    if (!isAddLabelForm) return null;

    return (
      <>
        <Form.Item
          style={{ marginBottom: 8 }}
          label="Label Name"
          name="name"
          rules={[{ required: true, message: 'Please select a label' }]}
        >
          <Select showSearch options={getSelectOptionsForEntity()} />
        </Form.Item>
        <Form.Item
          label="Status"
          name="status"
          initialValue={isBulkLabel ? LabelStatus.ADDED : LabelStatus.MANUALLY_ADDED}
        >
          <Select<LabelStatus>
            onChange={setLabelStatus}
            options={[
              { value: LabelStatus.MANUALLY_ADDED, label: 'Manually Added' },
              { value: LabelStatus.MANUALLY_REMOVED, label: 'Manually Removed' },
              { value: LabelStatus.ADDED, label: 'Added' },
              { value: LabelStatus.REMOVED, label: 'Removed' },
            ]}
          />
        </Form.Item>
      </>
    );
  };

  const renderLimitField = () => {
    if (!isBulkLabel) return null;

    return (
      <Form.Item className={styles.permanentAlert}>
        <Alert
          message="Info"
          description={
            <>
              <div className={styles.permanentDescription}>
                Standard bulk jobs have a limit of 100,000 entities to label. Selecting this will remove the limit.
              </div>
              <div className={styles.permanentDescription}>
                NOTE: No limit bulk jobs may take significantly longer to process.
              </div>
              <Checkbox checked={noLimit} onChange={(e) => setNolimit(e.target.checked)}>
                <Text tag="span" weight={TextWeights.NORMAL}>
                  NO LIMIT
                </Text>
              </Checkbox>
            </>
          }
          type="info"
          showIcon
        />
      </Form.Item>
    );
  };

  const shouldShowPermanentWarning =
    expiry === ExpiryOptions.PERMANENT && (!isAddLabelForm || labelStatus !== LabelStatus.MANUALLY_ADDED);

  const renderPermanentWarning = () => {
    if (!shouldShowPermanentWarning) return null;

    return (
      <Form.Item className={styles.permanentAlert}>
        <Alert
          message="Warning"
          description={
            <>
              <div className={styles.permanentDescription}>
                A permanent expiry means that Osprey cannot apply the removed label again. Please take care when
                selecting this option.
              </div>
              <Checkbox checked={checked} onChange={(e) => setChecked(e.target.checked)}>
                <Text tag="span" weight={TextWeights.SEMIBOLD}>
                  I understand and want to remove this label permanently.
                </Text>
              </Checkbox>
            </>
          }
          type="warning"
          showIcon
        />
      </Form.Item>
    );
  };

  return (
    <div className={styles.labelForm}>
      <Form form={form} onFinish={handleSubmit} layout="vertical">
        {title}
        {renderAddFormFields()}
        <Form.Item
          style={{ marginBottom: 8 }}
          label="Reason"
          name="reason"
          rules={[{ required: true, message: 'Please input a reason' }]}
        >
          <Input />
        </Form.Item>
        <Form.Item label="Expiration" name="expires_at" initialValue={initialExpiry}>
          <Select<string>
            onChange={setExpiry}
            options={[
              { value: ExpiryOptions.ONE_DAY, label: 'One Day' },
              { value: ExpiryOptions.ONE_WEEK, label: 'One Week' },
              { value: ExpiryOptions.TWO_WEEKS, label: 'Two Weeks' },
              { value: ExpiryOptions.ONE_MONTH, label: 'One Month' },
              { value: ExpiryOptions.TWO_MONTHS, label: 'Two Months' },
              { value: ExpiryOptions.THREE_MONTHS, label: 'Three Months' },
              { value: ExpiryOptions.PERMANENT, label: 'Permanent' },
              { value: ExpiryOptions.ONE_SECOND, label: 'Instant' },
            ]}
          />
        </Form.Item>
        {renderPermanentWarning()}
        {renderLimitField()}
        <Form.Item>
          <div className={styles.buttonWrapper}>
            <OspreyButton htmlType="button" onClick={onCancel}>
              Cancel
            </OspreyButton>
            <OspreyButton
              disabled={shouldShowPermanentWarning && !checked}
              className={styles.submitButton}
              color={isAddLabelForm ? ButtonColors.DARK_BLUE : ButtonColors.DANGER_RED}
              htmlType="submit"
              loading={isSubmitting}
            >
              {isAddLabelForm ? 'Submit' : 'Remove Label'}
            </OspreyButton>
          </div>
        </Form.Item>
      </Form>
    </div>
  );
};

export default LabelForm;
