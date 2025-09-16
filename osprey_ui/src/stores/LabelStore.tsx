import create from 'zustand';

interface LabelStore {
  bulkLabelEntityType: string | null;
  bulkLabelFeatureName: string | null;
  showBulkLabelDrawer: boolean;
  updateShowBulkLabelDrawer: (isVisible: boolean, featureName: string | null, entityType: string | null) => void;
}

const useLabelStore = create<LabelStore>((set) => ({
  bulkLabelEntityType: null,
  bulkLabelFeatureName: null,
  showBulkLabelDrawer: false,
  updateShowBulkLabelDrawer: (isVisible: boolean, featureName: string | null, entityType: string | null) =>
    set(() => ({ showBulkLabelDrawer: isVisible, bulkLabelFeatureName: featureName, bulkLabelEntityType: entityType })),
}));

export default useLabelStore;
