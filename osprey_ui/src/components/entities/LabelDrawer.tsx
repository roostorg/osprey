import * as React from 'react';
import { Drawer } from 'antd';

import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import useEntityStore from '../../stores/EntityStore';
import { Label } from '../../types/LabelTypes';
import { sortLabels } from '../../utils/EntityUtils';
import LabelOverview from './LabelOverview';

interface LabelDrawerProps {
  childDrawerProps?: {
    isVisible: boolean;
    onDrawerClose: () => void;
  };
}

const LabelDrawer = ({ childDrawerProps }: LabelDrawerProps) => {
  const { selectedEntity, updateSelectedEntity, updateShowLabelDrawer, showLabelDrawer } = useEntityStore();
  const labelInfoMapping = useApplicationConfigStore((state) => state.labelInfoMapping);

  const handleDrawerClose = () => {
    if (childDrawerProps != null) {
      childDrawerProps.onDrawerClose();
    } else {
      updateShowLabelDrawer(false);
    }
  };

  const handleEntityUpdate = (updatedLabels: Label[]) => {
    if (selectedEntity == null) return;

    updateSelectedEntity({
      ...selectedEntity,
      labels: sortLabels(updatedLabels, labelInfoMapping),
      hasLabels: updatedLabels.length > 0,
    });
  };

  const renderDrawerContent = () => {
    if (selectedEntity == null) return <div>No entity selected</div>;

    return <LabelOverview entity={selectedEntity} onEntityUpdate={handleEntityUpdate} onClose={handleDrawerClose} />;
  };

  return (
    <Drawer
      placement="right"
      width={400}
      visible={childDrawerProps == null ? showLabelDrawer : childDrawerProps.isVisible}
      onClose={handleDrawerClose}
      closable={false}
      destroyOnClose
    >
      {renderDrawerContent()}
    </Drawer>
  );
};

export default LabelDrawer;
