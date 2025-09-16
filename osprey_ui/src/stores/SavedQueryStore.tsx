import create from 'zustand';

import { SavedQuery } from '../types/QueryTypes';

interface SavedQueryStore {
  savedQueries: SavedQuery[];
  updateSavedQueries: (savedQueries: SavedQuery[]) => void;
}

const useSavedQueryStore = create<SavedQueryStore>((set) => ({
  savedQueries: [],
  updateSavedQueries: (savedQueries: SavedQuery[]) => set(() => ({ savedQueries })),
}));

export default useSavedQueryStore;
