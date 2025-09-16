import create from 'zustand';

import { Entity } from '../types/EntityTypes';
import { EventCountsByFeatureForEntityQuery } from '../types/QueryTypes';

export const DEFAULT_LABELS = { positive: [], negative: [], neutral: [] };

export interface EntityViewParams {
  entityType: string;
  entityId: string;
}

interface EntityStore {
  selectedEntity: Entity | null;
  updateSelectedEntity: (entity: Entity | null) => void;
  showLabelDrawer: boolean;
  updateShowLabelDrawer: (isVisible: boolean) => void;
  eventCountByFeature: EventCountsByFeatureForEntityQuery;
  totalEventCount: number;
}

const useEntityStore = create<EntityStore>((set) => ({
  selectedEntity: null,
  updateSelectedEntity: (newSelectedEntity: Entity | null) => set(() => ({ selectedEntity: newSelectedEntity })),
  showLabelDrawer: false,
  updateShowLabelDrawer: (isVisible: boolean) => set(() => ({ showLabelDrawer: isVisible })),
  eventCountByFeature: {},
  totalEventCount: 0,
}));

export default useEntityStore;
