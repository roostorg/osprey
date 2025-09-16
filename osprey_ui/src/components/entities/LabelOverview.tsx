import * as React from 'react';
import { Alert } from 'antd';
import { Link } from 'react-router-dom';

import { updateLabelsForEntity } from '../../actions/EntityActions';
import { Entity } from '../../types/EntityTypes';
import {
  LabelConnotations,
  LabelMutation,
  LabelMutationDetails,
  Label,
  LabelConnotation,
} from '../../types/LabelTypes';
import OspreyButton, { ButtonColors } from '../../uikit/OspreyButton';
import Text, { TextColors, TextSizes } from '../../uikit/Text';
import { sortLabelsChronologically } from '../../utils/LabelUtils';
import { makeEntityRoute } from '../../utils/RouteUtils';
import CopyLinkButton from '../common/CopyLinkButton';
import ExternalLinkButton from '../common/ExternalLinkButton';
import LabelForm from '../common/LabelForm';
import LabelRow from './LabelRow';
import LabelSection from './LabelSection';

import { Routes } from '../../Constants';
import styles from './LabelOverview.module.css';

function generateAlertMessage(labelUpdateResult: LabelMutationDetails) {
  const message = [];

  for (const updateType in labelUpdateResult) {
    const labelList = labelUpdateResult[updateType as keyof LabelMutationDetails];
    if (labelList.length > 0) {
      message.push(`Label${labelList.length > 1 ? 's' : ''} ${updateType}: ${labelList.join(', ')}`);
    }
  }

  return message;
}

interface LabelOverviewProps {
  entity: Entity;
  onEntityUpdate: (updatedLabels: Label[]) => void;
  onClose?: () => void;
}

const LabelOverview = ({ entity, onEntityUpdate, onClose }: LabelOverviewProps) => {
  const [showLabelForm, setShowLabelForm] = React.useState(false);
  const [labelUpdateResult, setLabelUpdateResult] = React.useState<LabelMutationDetails | null>(null);
  const [isSubmitting, setIsSubmitting] = React.useState(false);

  const entityRoutePath = makeEntityRoute(entity);

  const handleToggleForm = () => {
    setShowLabelForm(!showLabelForm);
    setLabelUpdateResult(null);
  };

  const handleSubmitForm = async (labelMutation: LabelMutation) => {
    setIsSubmitting(true);
    const updates = await updateLabelsForEntity(entity.id, entity.type, [labelMutation]);
    setIsSubmitting(false);

    onEntityUpdate(updates.labels);
    setShowLabelForm(false);
    setLabelUpdateResult(updates.mutation_result);
  };

  const renderLabelForm = () => {
    if (!showLabelForm) return null;
    return (
      <LabelForm
        isSubmitting={isSubmitting}
        entityType={entity.type}
        onSubmit={handleSubmitForm}
        onCancel={handleToggleForm}
      />
    );
  };

  const renderlabelUpdateResultAlert = () => {
    if (labelUpdateResult == null) return null;

    return <Alert className={styles.successAlert} type="success" message={generateAlertMessage(labelUpdateResult)} />;
  };

  const renderEntityViewButton = () => {
    // If there's no onClose, we are not in the drawer, which means we must be in
    // the Entity view already
    if (onClose == null) {
      return (
        <OspreyButton>
          <Link to={Routes.HOME}>Leave Entity View</Link>
        </OspreyButton>
      );
    }
    return (
      <OspreyButton onClick={() => onClose?.()}>
        <Link to={{ pathname: entityRoutePath }}>Go to Entity View</Link>
      </OspreyButton>
    );
  };

  const renderLabelList = (connotation: LabelConnotation) => {
    const labels = sortLabelsChronologically(entity.labels?.[connotation]) ?? [];
    if (labels.length === 0) {
      return (
        <Text size={TextSizes.SMALL} className={styles.emptyLabelList} color={TextColors.LIGHT_SECONDARY}>
          No {connotation} labels.
        </Text>
      );
    }

    return (
      <div>
        {labels.map((label) => (
          <LabelRow
            key={label.name}
            label={label}
            onRemove={handleSubmitForm}
            connotation={connotation}
            isSubmitting={isSubmitting}
          />
        ))}
      </div>
    );
  };

  return (
    <div>
      <div className={styles.drawerHeader}>
        <div className={styles.entityTypeButtonWrapper}>
          <Text size={TextSizes.H5} className={styles.labelsHeader}>
            {entity.type}
          </Text>
          <div className={styles.iconButtons}>
            <CopyLinkButton link={`${document.location.origin}${entityRoutePath}`} />
            <ExternalLinkButton
              className={styles.externalLinkButton}
              entityType={entity.type}
              entityId={entity.id}
              icon
            />
          </div>
        </div>
        <div className={styles.entityId} title={entity.id}>
          {entity.id}
        </div>
        <div className={styles.buttonContainer}>
          <OspreyButton
            color={ButtonColors.DARK_BLUE}
            onClick={handleToggleForm}
            className={styles.addLabelButton}
            size="small"
          >
            Add Label
          </OspreyButton>
          {renderEntityViewButton()}
        </div>
      </div>
      {renderlabelUpdateResultAlert()}
      {renderLabelForm()}
      {LabelConnotations.map((connotation) => (
        <LabelSection key={connotation} connotation={connotation}>
          {renderLabelList(connotation)}
        </LabelSection>
      ))}
    </div>
  );
};

export default LabelOverview;
