import { Switch } from 'antd';
import { MoonOutlined, SunOutlined } from '@ant-design/icons';

import useThemeStore from '../stores/ThemeStore';

const ThemeToggle = () => {
  const mode = useThemeStore((state) => state.mode);
  const toggleMode = useThemeStore((state) => state.toggleMode);

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
