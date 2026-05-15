import * as React from 'react';
import { Menu, MenuProps } from 'antd';
import {
  HistoryOutlined,
  SaveOutlined,
  ApartmentOutlined,
  FunctionOutlined,
  ThunderboltOutlined,
  ClockCircleOutlined,
  SearchOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
} from '@ant-design/icons';
import { Link, useLocation } from 'react-router-dom';

import Logo from '../../assets/Logo';
import useApplicationConfigStore from '../../stores/ApplicationConfigStore';

import { Routes } from '../../Constants';
import ThemeToggle from '../../uikit/ThemeToggle';
import styles from './NavBar.module.css';

const SIDEBAR_STORAGE_KEY = 'osprey-sidebar-expanded';

type NavItem = {
  key: string;
  icon: React.ReactNode;
  label: string;
};

type NavGroup = {
  key: string;
  label: string;
  children: NavItem[];
};

const NAV_GROUPS: NavGroup[] = [
  {
    key: 'investigate',
    label: 'Investigate',
    children: [
      { key: Routes.HOME, icon: <SearchOutlined />, label: 'Query' },
      { key: Routes.QUERY_HISTORY, icon: <HistoryOutlined />, label: 'Query History' },
      { key: Routes.SAVED_QUERIES, icon: <SaveOutlined />, label: 'Saved Queries' },
    ],
  },
  {
    key: 'manage',
    label: 'Manage',
    children: [
      { key: Routes.RULES_VISUALIZER, icon: <ApartmentOutlined />, label: 'Rules Visualizer' },
      { key: Routes.DOCS_UDFS, icon: <FunctionOutlined />, label: 'UDF Registry' },
    ],
  },
  {
    key: 'operate',
    label: 'Operate',
    children: [
      { key: Routes.BULK_ACTION, icon: <ThunderboltOutlined />, label: 'Bulk Actions' },
      { key: Routes.BULK_JOB_HISTORY, icon: <ClockCircleOutlined />, label: 'Bulk Job History' },
    ],
  },
];

const NAV_KEYS: string[] = NAV_GROUPS.flatMap((group) => {
  return group.children.map((item) => {
    return item.key;
  });
});

const NavBar = ({ children }: { children: React.ReactNode }) => {
  const location = useLocation();
  const isRecordingClicks = useApplicationConfigStore((state) => {
    return state.isRecordingClicks;
  });

  const [isExpanded, setIsExpanded] = React.useState<boolean>(() => {
    if (typeof window === 'undefined') return true;
    const stored = window.localStorage.getItem(SIDEBAR_STORAGE_KEY);
    if (stored === null) return true; // default to expanded on first visit
    return stored === 'true';
  });

  const toggleSidebar = React.useCallback(() => {
    setIsExpanded((prev) => {
      const next = !prev;
      window.localStorage.setItem(SIDEBAR_STORAGE_KEY, String(next));
      return next;
    });
  }, []);

  const menuItems: MenuProps['items'] = NAV_GROUPS.map((group) => {
    return {
      key: group.key,
      type: 'group',
      label: group.label,
      children: group.children.map((item) => {
        return {
          key: item.key,
          icon: item.icon,
          label: (
            <Link to={item.key} aria-current={location.pathname === item.key ? 'page' : undefined}>
              {item.label}
            </Link>
          ),
        };
      }),
    };
  });

  const selectedKeys = React.useMemo(() => {
    // Match the menu item whose key is the longest prefix of pathname.
    const exact = NAV_KEYS.find((k) => {
      return k === location.pathname;
    });
    if (exact) return [exact];
    // pathname like /entity/... or /events/... — fall back to no selection
    // unless the path starts with one of the known top-level routes (other than '/').
    const prefix = NAV_KEYS.filter((k) => {
      return k !== Routes.HOME;
    }).find((k) => {
      return location.pathname.startsWith(`${k}/`);
    });
    return prefix ? [prefix] : [];
  }, [location.pathname]);

  return (
    <div className={`${styles.appWrapper}${isRecordingClicks ? ` ${styles.isRecording}` : ''}`}>
      <aside
        className={`${styles.sidebar} ${isExpanded ? styles.sidebarExpanded : styles.sidebarCollapsed}`}
        aria-label="Primary navigation"
      >
        <div className={styles.sidebarHeader}>
          <Link to={Routes.HOME} aria-label="Osprey home" className={styles.logoLink}>
            <Logo variant={isExpanded ? 'full' : 'mark'} />
          </Link>
          <button
            type="button"
            className={styles.collapseButton}
            onClick={toggleSidebar}
            aria-label={isExpanded ? 'Collapse sidebar' : 'Expand sidebar'}
            aria-expanded={isExpanded}
          >
            {isExpanded ? <MenuFoldOutlined /> : <MenuUnfoldOutlined />}
          </button>
        </div>
        <Menu
          className={styles.sidebarMenu}
          mode="inline"
          items={menuItems}
          selectedKeys={selectedKeys}
          inlineCollapsed={!isExpanded}
        />
      </aside>
      <div className={styles.mainColumn}>
        <header className={styles.topBar}>
          <div className={styles.topBarLeft} />
          <div className={styles.topBarRight}>
            <ThemeToggle />
          </div>
        </header>
        <main className={styles.contentWrapper}>{children}</main>
      </div>
    </div>
  );
};

export default NavBar;
