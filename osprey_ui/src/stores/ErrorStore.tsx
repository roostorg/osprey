import create from 'zustand';

interface ErrorStore {
  errors: Set<string>;
  clearErrors(): void;
}

const useErrorStore = create<ErrorStore>((set) => ({
  errors: new Set(),
  clearErrors: () => set(() => ({ errors: new Set() })),
}));

export default useErrorStore;
