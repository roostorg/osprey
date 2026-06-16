import { Segmented } from 'antd';
import { DesktopOutlined, MoonOutlined, SunOutlined } from '@ant-design/icons';

import useThemeStore, { ThemePreference } from '../stores/ThemeStore';

const OPTIONS = [
  { value: 'light' as const, icon: <SunOutlined />, title: 'Light theme' },
  { value: 'system' as const, icon: <DesktopOutlined />, title: 'Use system theme' },
  { value: 'dark' as const, icon: <MoonOutlined />, title: 'Dark theme' },
];

const ThemeToggle = () => {
  const preference = useThemeStore((state) => state.preference);
  const setPreference = useThemeStore((state) => state.setPreference);

  return (
    <Segmented
      value={preference}
      onChange={(value) => setPreference(value as ThemePreference)}
      options={OPTIONS}
      aria-label="Theme"
    />
  );
};

export default ThemeToggle;
