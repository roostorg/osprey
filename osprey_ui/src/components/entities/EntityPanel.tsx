import * as React from 'react';
import { useParams } from 'react-router-dom';

import { getLabelsForEntity } from '../../actions/EntityActions';
import usePromiseResult, { PromiseResult } from '../../hooks/usePromiseResult';
import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import useEntityStore, { EntityViewParams } from '../../stores/EntityStore';
import { Label } from '../../types/LabelTypes';
import { sortLabels } from '../../utils/EntityUtils';
import { renderFromPromiseResult } from '../../utils/PromiseResultUtils';
import LabelOverview from './LabelOverview';

import styles from './EntityPanel.module.css';

const EntityPanel = ({ entityType, entityId }: EntityViewParams) => {
  const labelInfoMapping = useApplicationConfigStore((state) => state.labelInfoMapping);
  const [labels, setLabels] = React.useState<Label[]>([]);

  const [selectedEntity, updateSelectedEntity] = useEntityStore((state) => [
    state.selectedEntity,
    state.updateSelectedEntity,
  ]);

  const entityLabelResults: PromiseResult<void> = usePromiseResult(async () => {
    const entityLabels = await getLabelsForEntity(entityId, entityType);
    setLabels(entityLabels);
  }, [entityId, entityType]);

  const handleEntityUpdate = (updatedLabels: Label[]) => {
    setLabels(updatedLabels);

    // If our selected entity is the same as our entity view, reset it so it will refetch
    // labels if reselected.
    if (selectedEntity != null && selectedEntity.type === entityType && selectedEntity.id === entityId) {
      updateSelectedEntity(null);
    }
  };

  return (
    <div className={styles.entityPanel}>
      {renderFromPromiseResult(entityLabelResults, () => {
        const entity = {
          type: entityType,
          id: entityId,
          labels: sortLabels(labels, labelInfoMapping),
          hasLabels: labels.length > 0,
        };
        return <LabelOverview entity={entity} onEntityUpdate={handleEntityUpdate} />;
      })}
    </div>
  );
};

const EntityPanelContainer: React.FC = () => {
  const { entityType, entityId } = useParams<{ entityType: string; entityId: string }>();
  const featureNameToEntityTypeMapping = useApplicationConfigStore((state) => state.featureNameToEntityTypeMapping);
  const isValidEntityRoute = new Set(featureNameToEntityTypeMapping.values()).has(entityType) && entityId != null;

  if (!isValidEntityRoute) return null;

  return <EntityPanel entityType={decodeURIComponent(entityType)} entityId={decodeURIComponent(entityId)} />;
};

export default EntityPanelContainer;
