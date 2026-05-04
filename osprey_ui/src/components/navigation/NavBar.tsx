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
import { Routes } from '../../Constants';
import styles from './NavBar.module.css';

const SIDEBAR_STORAGE_KEY = 'osprey-sidebar-expanded';

const NavBar = ({ children }: { children: React.ReactNode }) => {
  const location = useLocation();

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

  const menuItems: MenuProps['items'] = [
    {
      key: 'investigate',
      type: 'group',
      label: 'Investigate',
      children: [
        {
          key: Routes.HOME,
          icon: <SearchOutlined />,
          label: <Link to={Routes.HOME}>Query</Link>,
        },
        {
          key: Routes.QUERY_HISTORY,
          icon: <HistoryOutlined />,
          label: <Link to={Routes.QUERY_HISTORY}>Query History</Link>,
        },
        {
          key: Routes.SAVED_QUERIES,
          icon: <SaveOutlined />,
          label: <Link to={Routes.SAVED_QUERIES}>Saved Queries</Link>,
        },
      ],
    },
    {
      key: 'manage',
      type: 'group',
      label: 'Manage',
      children: [
        {
          key: Routes.RULES_VISUALIZER,
          icon: <ApartmentOutlined />,
          label: <Link to={Routes.RULES_VISUALIZER}>Rules Visualizer</Link>,
        },
        {
          key: Routes.DOCS_UDFS,
          icon: <FunctionOutlined />,
          label: <Link to={Routes.DOCS_UDFS}>UDF Registry</Link>,
        },
      ],
    },
    {
      key: 'operate',
      type: 'group',
      label: 'Operate',
      children: [
        {
          key: Routes.BULK_ACTION,
          icon: <ThunderboltOutlined />,
          label: <Link to={Routes.BULK_ACTION}>Bulk Actions</Link>,
        },
        {
          key: Routes.BULK_JOB_HISTORY,
          icon: <ClockCircleOutlined />,
          label: <Link to={Routes.BULK_JOB_HISTORY}>Bulk Job History</Link>,
        },
      ],
    },
  ];

  const selectedKeys = React.useMemo(() => {
    // Match the menu item whose key is the longest prefix of pathname.
    const candidateKeys = [
      Routes.HOME,
      Routes.QUERY_HISTORY,
      Routes.SAVED_QUERIES,
      Routes.RULES_VISUALIZER,
      Routes.DOCS_UDFS,
      Routes.BULK_ACTION,
      Routes.BULK_JOB_HISTORY,
    ];
    const exact = candidateKeys.find((k) => k === location.pathname);
    if (exact) return [exact];
    // pathname like /entity/... or /events/... — fall back to no selection
    // unless the path starts with one of the known top-level routes (other than '/').
    const prefix = candidateKeys.filter((k) => k !== Routes.HOME).find((k) => location.pathname.startsWith(`${k}/`));
    return prefix ? [prefix] : [];
  }, [location.pathname]);

  return (
    <div className={styles.appWrapper}>
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
          <div className={styles.topBarRight} />
        </header>
        <main className={styles.contentWrapper}>{children}</main>
      </div>
    </div>
  );
};

export default NavBar;
