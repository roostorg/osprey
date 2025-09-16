import * as React from 'react';
import { SelectOutlined, StopOutlined } from '@ant-design/icons';

import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import useLabelStore from '../../stores/LabelStore';
import OspreyButton, { ButtonColors } from '../../uikit/OspreyButton';
import CSVDownloadModal from './CSVDownloadModal';

import styles from './TopNFooter.module.css';
import { Select } from 'antd';
const { Option } = Select;

interface TopNFooterProps {
  disabled: boolean;
  onRemoveTable: () => void;
  onPrecisionChange: (precision: number) => void;
  currentPrecision: number;
  showPrecision: boolean;
  dimension: string;
  onTogglePoP: () => void;
  isPoPEnabled: boolean;
}

const TopNFooter = ({
  disabled,
  onRemoveTable,
  onPrecisionChange,
  onTogglePoP,
  currentPrecision,
  showPrecision,
  dimension,
  isPoPEnabled,
}: TopNFooterProps) => {
  const featureNameToEntityTypeMapping = useApplicationConfigStore((state) => state.featureNameToEntityTypeMapping);
  const updateShowBulkLabelDrawer = useLabelStore((state) => state.updateShowBulkLabelDrawer);

  const renderBulkLabelButton = () => {
    const entityType = featureNameToEntityTypeMapping.get(dimension);
    if (entityType == null) return null;

    return (
      <>
        <OspreyButton
          color={ButtonColors.LINK_GRAY}
          key="bulk"
          onClick={() => updateShowBulkLabelDrawer(true, dimension, entityType)}
        >
          <SelectOutlined />
          Bulk Label
        </OspreyButton>
      </>
    );
  };

  return (
    <div className={styles.menu}>
      <CSVDownloadModal disabled={disabled} dimension={dimension} />
      {renderBulkLabelButton()}
      <OspreyButton color={ButtonColors.LINK_GRAY} key="delta-pop" onClick={onTogglePoP} disabled={disabled}>
        PoP {isPoPEnabled ? 'On' : 'Off'}
      </OspreyButton>
      <OspreyButton color={ButtonColors.LINK_GRAY} key="remove" onClick={onRemoveTable}>
        <StopOutlined />
        Yeet Table
      </OspreyButton>
      {showPrecision && (
        <div>
          <b>Precision:</b>
          <Select
            bordered={false}
            onChange={onPrecisionChange}
            defaultValue={currentPrecision}
            style={{ width: 100, marginLeft: 0 }}
          >
            <Option value={0}>Default</Option>
            <Option value={0.1}>0.1</Option>
            <Option value={0.01}>0.01</Option>
            <Option value={0.001}>0.001</Option>
            <Option value={0.0001}>0.0001</Option>
          </Select>
        </div>
      )}
    </div>
  );
};

export default TopNFooter;
