import create from 'zustand';

export type ThemeMode = 'light' | 'dark';

const STORAGE_KEY = 'osprey-ui-dark-theme';

const getInitialMode = (): ThemeMode => {
  if (typeof window === 'undefined') return 'light';
  const stored = window.localStorage.getItem(STORAGE_KEY);
  if (stored === 'true') return 'dark';
  if (stored === 'false') return 'light';
  // No localStorage value: honor the OS preference.
  if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
    return 'dark';
  }
  return 'light';
};

type ThemeStore = {
  mode: ThemeMode;
  setMode: (mode: ThemeMode) => void;
  toggleMode: () => void;
};

const persist = (mode: ThemeMode) => {
  window.localStorage.setItem(STORAGE_KEY, String(mode === 'dark'));
};

const useThemeStore = create<ThemeStore>((set) => ({
  mode: getInitialMode(),
  setMode: (mode) => {
    persist(mode);
    set({ mode });
  },
  toggleMode: () => {
    return set((state) => {
      const next: ThemeMode = state.mode === 'dark' ? 'light' : 'dark';
      persist(next);
      return { mode: next };
    });
  },
}));

export default useThemeStore;
