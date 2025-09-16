import * as React from 'react';
import { MenuOutlined } from '@ant-design/icons';
import { Menu } from 'antd';
import classNames from 'classnames';
import { Link, useLocation } from 'react-router-dom';

import Logo from '../../assets/Logo';
import useApplicationConfigStore from '../../stores/ApplicationConfigStore';

import { Routes } from '../../Constants';
import styles from './NavBar.module.css';

const NavBar = ({ children }: React.PropsWithChildren) => {
  const [showMenu, setShowMenu] = React.useState(false);
  const location = useLocation();
  const isRecordingClicks = useApplicationConfigStore((state) => state.isRecordingClicks);
  const menuContainerRef = React.useRef<HTMLDivElement>(null);

  React.useEffect(() => {
    let isOpenClick = true;

    function handleWindowClick(e: MouseEvent) {
      // Skip initial click on menu icon that opens menu
      if (isOpenClick) {
        isOpenClick = false;
        return;
      }
      if (
        menuContainerRef.current != null &&
        e.target != null &&
        e.target instanceof Node &&
        !menuContainerRef.current.contains(e.target)
      ) {
        setShowMenu(false);
      }
    }

    if (showMenu) {
      window.addEventListener('click', handleWindowClick);
    }

    return () => window.removeEventListener('click', handleWindowClick);
  }, [menuContainerRef, showMenu]);

  const handleToggleMenu = () => {
    setShowMenu(!showMenu);
  };

  const getSelectedKeys = (): string[] => {
    return [location.pathname];
  };

  return (
    <div className={classNames(styles.appWrapper, { [styles.isRecording]: isRecordingClicks })}>
      <div className={styles.navBar}>
        <MenuOutlined onClick={handleToggleMenu} className={styles.menuOutlinedIcon} />
        <Link to={Routes.HOME}>
          <Logo />
        </Link>
      </div>
      <div className={styles.contentWrapper}>
        <div ref={menuContainerRef} className={classNames(styles.navMenuContainer, { [styles.expanded]: showMenu })}>
          <Menu
            className={styles.navMenu}
            onSelect={handleToggleMenu}
            selectedKeys={getSelectedKeys()}
            mode="inline"
            inlineCollapsed={!showMenu}
          >
            <Menu.Item key={Routes.HOME}>
              <Link to={Routes.HOME}>Home</Link>
            </Menu.Item>
            <Menu.Item key={Routes.QUERY_HISTORY}>
              <Link to={Routes.QUERY_HISTORY}>Query History</Link>
            </Menu.Item>
            <Menu.Item key={Routes.SAVED_QUERIES}>
              <Link to={Routes.SAVED_QUERIES}>Saved Queries</Link>
            </Menu.Item>
            <Menu.Item key={Routes.DOCS_UDFS}>
              <Link to={Routes.DOCS_UDFS}>UDF Documentation</Link>
            </Menu.Item>
            <Menu.Item key={Routes.BULK_JOB_HISTORY}>
              <Link to={Routes.BULK_JOB_HISTORY}>Bulk Job History</Link>
            </Menu.Item>
            <Menu.Item key={Routes.RULES_VISUALIZER}>
              <Link to={Routes.RULES_VISUALIZER}>Rules Visualizer</Link>
            </Menu.Item>
          </Menu>
        </div>
        {children}
      </div>
    </div>
  );
};

export default NavBar;
