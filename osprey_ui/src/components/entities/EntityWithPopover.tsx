import * as React from 'react';
import { Empty } from 'antd';
import classNames from 'classnames';
import { useParams } from 'react-router-dom';

import { getLabelsForEntity } from '../../actions/EntityActions';
import usePromiseResult from '../../hooks/usePromiseResult';
import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import useEntityStore from '../../stores/EntityStore';
import { Label, LabelConnotations } from '../../types/LabelTypes';
import OspreyButton, { ButtonColors } from '../../uikit/OspreyButton';
import Text, { TextSizes } from '../../uikit/Text';
import Tooltip from '../../uikit/Tooltip';
import { sortLabels } from '../../utils/EntityUtils';
import { sortLabelsChronologically } from '../../utils/LabelUtils';
import { renderFromPromiseResult } from '../../utils/PromiseResultUtils';
import ExternalLinkButton from '../common/ExternalLinkButton';
import Feature from '../common/Feature';
import LabelTag from '../common/LabelTag';
import LabelSection from './LabelSection';

import styles from './EntityWithPopover.module.css';

const clickInfo = (
  <>
    <div>⌘/◇ + click to add this entity to current query</div>
    <div className={styles.clickInfoPoint}>⌘/◇ + ⇧ + click to query this entity in new tab</div>
  </>
);

interface PopoverContentProps {
  entityId: string;
  entityType: string;
  onEdit: () => void;
}

const PopoverContent = ({ entityId, entityType, onEdit }: PopoverContentProps) => {
  const [selectedEntity, updateSelectedEntity] = useEntityStore((state) => [
    state.selectedEntity,
    state.updateSelectedEntity,
  ]);
  const labelInfoMapping = useApplicationConfigStore((state) => state.labelInfoMapping);
  const selectedEntityFetchResult = usePromiseResult(async () => {
    if (selectedEntity?.type === entityType && selectedEntity?.id === entityId) return;

    const labels = await getLabelsForEntity(entityId, entityType);
    const hasLabels = labels.length > 0;

    updateSelectedEntity({
      type: entityType,
      id: entityId,
      labels: sortLabels(labels, labelInfoMapping),
      hasLabels,
    });
  });

  const renderLabelSections = () => {
    if (!selectedEntity?.hasLabels) return <Empty />;

    return LabelConnotations.map((connotation) => {
      const labels = sortLabelsChronologically(selectedEntity.labels?.[connotation]);
      if (labels.length === 0) return null;

      return (
        <LabelSection key={connotation} connotation={connotation}>
          <div className={styles.labelContainer}>
            {labels.map((label: Label) => (
              <LabelTag className={styles.labelTag} key={label.name} label={label} connotation={connotation} />
            ))}
          </div>
        </LabelSection>
      );
    });
  };

  return (
    <div className={styles.labelPopover}>
      {renderFromPromiseResult(selectedEntityFetchResult, () => (
        <div className={styles.labelSectionsContainer}>{renderLabelSections()}</div>
      ))}
      <div className={styles.popoverInfo}>{clickInfo}</div>
      <div className={styles.buttonsContainer}>
        <OspreyButton color={ButtonColors.DARK_BLUE} style={{ marginRight: 6 }} onClick={onEdit}>
          Edit Labels
        </OspreyButton>
        <ExternalLinkButton entityType={entityType} entityId={entityId} />
      </div>
    </div>
  );
};

interface EntityWithPopoverProps {
  featureName: string;
  entityId: string;
  entityType: string;
}

const EntityWithPopover = ({ entityId, featureName, entityType }: EntityWithPopoverProps) => {
  const updateShowLabelDrawer = useEntityStore((state) => state.updateShowLabelDrawer);
  const { entityType: entityRouteType, entityId: entityRouteId } = useParams<{
    entityType: string;
    entityId: string;
  }>();

  const isRouteEntity = entityRouteType === entityType && entityRouteId === entityId;
  const decodedEntityType = decodeURIComponent(entityType);
  const decodedEntityId = decodeURIComponent(entityId);

  const handleShowLabelDrawer = () => {
    updateShowLabelDrawer(true);
  };

  const isPopoverDisabled = (e: React.MouseEvent<HTMLElement>) => {
    return e.shiftKey;
  };

  const renderPopoverContent = () => (
    <div className={styles.popoverContent}>
      <div className={styles.popoverTitleWrapper}>
        <div className={styles.entityInfo}>
          {decodedEntityType}: {decodedEntityId}
        </div>
        <Text size={TextSizes.H5}>Labels</Text>
      </div>
      <PopoverContent entityId={decodedEntityId} entityType={decodedEntityType} onEdit={handleShowLabelDrawer} />
    </div>
  );

  const renderPopoverOrFeature = () => {
    return (
      <>
        <Tooltip
          className={styles.tooltip}
          contentClassName={styles.tooltipOrigin}
          content={renderPopoverContent()}
          isDisabled={isPopoverDisabled}
        >
          <Feature
            className={classNames(styles.entityId, { [styles.routeEntityId]: isRouteEntity })}
            featureName={featureName}
            value={decodedEntityId}
            onClick={handleShowLabelDrawer}
            isEntity
          />
        </Tooltip>
        <ExternalLinkButton entityType={decodedEntityType} entityId={decodedEntityId} icon />
      </>
    );
  };

  return <div className={styles.entityIdWrapper}>{renderPopoverOrFeature()}</div>;
};

export default EntityWithPopover;
