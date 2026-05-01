import * as React from 'react';
import { Switch } from 'antd';
import { MoonOutlined, SunOutlined } from '@ant-design/icons';

import useThemeStore, { THEME_STORAGE_KEY } from '../stores/ThemeStore';

const DARK_CLASS = 'dark-theme';

const ThemeToggle = () => {
  const mode = useThemeStore((state) => state.mode);
  const toggleMode = useThemeStore((state) => state.toggleMode);

  React.useLayoutEffect(() => {
    // AC3.5 / AC3.8: apply class synchronously before paint to prevent FOUC.
    const root = document.documentElement;
    if (mode === 'dark') {
      root.classList.add(DARK_CLASS);
    } else {
      root.classList.remove(DARK_CLASS);
    }
    // AC3.2: persist the user's explicit choice. Note that this also runs on first
    // render — that's intentional, because if the OS prefers dark and we honored that
    // (AC3.3), we want subsequent visits to keep returning dark even after the user
    // changes their OS preference, until they explicitly toggle.
    window.localStorage.setItem(THEME_STORAGE_KEY, String(mode === 'dark'));
  }, [mode]);

  return (
    <Switch
      checked={mode === 'dark'}
      onChange={toggleMode}
      checkedChildren={<MoonOutlined />}
      unCheckedChildren={<SunOutlined />}
      aria-label={mode === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
    />
  );
};

export default ThemeToggle;
