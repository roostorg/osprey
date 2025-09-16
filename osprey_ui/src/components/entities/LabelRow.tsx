import * as React from 'react';
import { DeleteOutlined } from '@ant-design/icons';

import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import { Label, LabelConnotation, LabelMutation, LabelReason as Reason, LabelStatus } from '../../types/LabelTypes';
import Collapse from '../../uikit/Collapse';
import OspreyButton, { ButtonColors } from '../../uikit/OspreyButton';
import Text, { TextColors, TextSizes } from '../../uikit/Text';
import ManuallyAddedIcon from '../../uikit/icons/ManuallyAddedIcon';
import ManuallyRemovedIcon from '../../uikit/icons/ManuallyRemovedIcon';
import LabelForm from '../common/LabelForm';
import LabelTag from '../common/LabelTag';
import LabelReason from './LabelReason';

import styles from './LabelRow.module.css';

interface LabelRowProps {
  label: Label;
  onRemove: (labelMutation: LabelMutation) => void;
  connotation: LabelConnotation;
  isSubmitting: boolean;
}

export const LabelStatusHistoryMapping: Record<LabelStatus, string> = {
  [LabelStatus.ADDED]: 'AUTOMATICALLY ADDED',
  [LabelStatus.REMOVED]: 'AUTOMATICALLY REMOVED',
  [LabelStatus.MANUALLY_ADDED]: 'MANUALLY ADDED',
  [LabelStatus.MANUALLY_REMOVED]: 'MANUALLY REMOVED',
};

const StatusIcons = {
  [LabelStatus.MANUALLY_ADDED]: <ManuallyAddedIcon width={16} />,
  [LabelStatus.MANUALLY_REMOVED]: <ManuallyRemovedIcon width={16} />,
  [LabelStatus.ADDED]: <ManuallyAddedIcon width={16} />,
  [LabelStatus.REMOVED]: <ManuallyRemovedIcon width={16} />,
};

function sortReasonsChronologically([_a, reasonA]: [string, Reason], [_b, reasonB]: [string, Reason]) {
  return Date.parse(reasonB.created_at) - Date.parse(reasonA.created_at);
}

const LabelRow = ({ label, onRemove, connotation, isSubmitting }: LabelRowProps) => {
  const [isActive, setIsActive] = React.useState(false);
  const [showRemoveForm, setShowRemoveForm] = React.useState(false);
  const labelInfoMapping = useApplicationConfigStore((state) => state.labelInfoMapping);

  const description = labelInfoMapping.get(label.name)?.description;

  const handleDeleteButtonClick = (e: React.SyntheticEvent) => {
    e.stopPropagation();
    if (!isActive) {
      setIsActive(true);
    }

    setShowRemoveForm(!showRemoveForm);
  };

  const header = (
    <div className={styles.label}>
      <div className={styles.labelInfo}>
        <LabelTag className={styles.labelTag} label={label} connotation={connotation} />
      </div>
      {label.status !== LabelStatus.MANUALLY_REMOVED ? (
        <OspreyButton
          className={styles.removeButton}
          color={ButtonColors.LINK_GRAY}
          icon={<DeleteOutlined />}
          onClick={handleDeleteButtonClick}
        />
      ) : null}
    </div>
  );

  const handleRemoveLabel = (labelMutation: LabelMutation) => {
    onRemove(labelMutation);
    setShowRemoveForm(false);
  };

  const renderLabelDescription = () => {
    if (description === null) return null;

    return <div className={styles.labelDescription}>{description}</div>;
  };

  const renderLabelHistory = () => {
    const { reasons, status } = label;
    const labelStateHistory = [{ reasons, status }, ...label.previous_states];

    return labelStateHistory.map((previousState, index) => (
      <div className={styles.historyCard} key={index}>
        <div className={styles.iconSidebar}>
          <div className={styles.iconWrapper}>{StatusIcons[previousState.status]}</div>
          {index === labelStateHistory.length - 1 ? null : <div className={styles.verticalLine} />}
        </div>
        <div className={styles.historyCardText} key={index}>
          <Text size={TextSizes.H7} color={TextColors.LIGHT_SECONDARY}>
            {LabelStatusHistoryMapping[previousState.status]}
          </Text>
          {Object.entries(previousState.reasons)
            .sort(sortReasonsChronologically)
            .map(([reasonName, reason]) => (
              <LabelReason key={reasonName} reasonName={reasonName} reason={reason} />
            ))}
        </div>
      </div>
    ));
  };

  const renderRemoveLabelForm = () => {
    if (!showRemoveForm) return null;

    return (
      <LabelForm
        label={label}
        title={<Text size={TextSizes.H5}>Remove Label</Text>}
        isSubmitting={isSubmitting}
        onCancel={() => setShowRemoveForm(false)}
        onSubmit={handleRemoveLabel}
      />
    );
  };

  return (
    <Collapse
      headerClassName={styles.header}
      header={header}
      isActive={isActive}
      onClick={() => setIsActive(!isActive)}
    >
      <div className={styles.content}>
        {renderRemoveLabelForm()}
        {renderLabelDescription()}
        {renderLabelHistory()}
      </div>
    </Collapse>
  );
};

export default LabelRow;
