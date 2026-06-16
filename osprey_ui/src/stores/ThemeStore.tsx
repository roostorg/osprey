import create from 'zustand';

export type ThemeMode = 'light' | 'dark';
export type ThemePreference = 'system' | 'light' | 'dark';

const STORAGE_KEY = 'osprey-ui-theme-preference';

const getSystemMode = (): ThemeMode => {
  if (typeof window === 'undefined' || !window.matchMedia) return 'light';
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
};

const getInitialPreference = (): ThemePreference => {
  if (typeof window === 'undefined') return 'system';
  const stored = window.localStorage.getItem(STORAGE_KEY);
  if (stored === 'light' || stored === 'dark') return stored;
  return 'system';
};

const resolveMode = (pref: ThemePreference): ThemeMode =>
  pref === 'system' ? getSystemMode() : pref;

type ThemeStore = {
  preference: ThemePreference;
  mode: ThemeMode;
  setPreference: (preference: ThemePreference) => void;
};

const useThemeStore = create<ThemeStore>((set) => {
  const preference = getInitialPreference();
  return {
    preference,
    mode: resolveMode(preference),
    setPreference: (next) => {
      if (next === 'system') {
        window.localStorage.removeItem(STORAGE_KEY);
      } else {
        window.localStorage.setItem(STORAGE_KEY, next);
      }
      set({ preference: next, mode: resolveMode(next) });
    },
  };
});

if (typeof window !== 'undefined' && window.matchMedia) {
  const mql = window.matchMedia('(prefers-color-scheme: dark)');
  const handleChange = () => {
    if (useThemeStore.getState().preference === 'system') {
      useThemeStore.setState({ mode: getSystemMode() });
    }
  };
  if (mql.addEventListener) {
    mql.addEventListener('change', handleChange);
  } else if (mql.addListener) {
    // Safari < 14 fallback.
    mql.addListener(handleChange);
  }
}

export default useThemeStore;
