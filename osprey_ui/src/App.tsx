import * as React from 'react';
import { App as AntdApp, ConfigProvider, Spin, theme } from 'antd';
import { Router, Switch, Route } from 'react-router-dom';

import { getApplicationConfig } from './actions/ConfigActions';
import UdfDocsView from './components/docs/UdfDocsView';
import BulkJobHistoryView from './components/bulk_job_history/BulkJobHistory';
import { FeaturesPage } from './components/features/FeaturesPage';
import RulesVisualizerView from './components/rules_visualizer/RulesVisualizer';
import EntityViewBar from './components/entities/EntityViewBar';
import EventPage from './components/event_stream/EventPage';
import NavBar from './components/navigation/NavBar';
import QueryHistory from './components/query_history/QueryHistory';
import QueryView from './components/query_view/QueryView';
import SavedQueries from './components/saved_queries/SavedQueries';
import SavedQueryBar from './components/saved_queries/SavedQueryBar';
import usePromiseResult from './hooks/usePromiseResult';
import useApplicationConfigStore from './stores/ApplicationConfigStore';
import useThemeStore, { THEME_STORAGE_KEY } from './stores/ThemeStore';
import { history } from './stores/QueryStore';
import { renderFromPromiseResult } from './utils/PromiseResultUtils';

import './stores/SearchParamsStateListener';

import { Routes } from './Constants';
import styles from './App.module.css';
import { BulkActionPage } from './components/bulk_actions/BulkActionPage';

const AppRouter: React.FC = () => {
  const updateApplicationConfig = useApplicationConfigStore((state) => state.updateApplicationConfig);
  const themeMode = useThemeStore((state) => state.mode);

  const applicationConfigResult = usePromiseResult(async () => {
    const appConfig = await getApplicationConfig();
    updateApplicationConfig(appConfig);
  });

  const isDark = themeMode === 'dark';
  // Mirror of --brand-primary in Colors.module.css. Antd's algorithm needs a
  // concrete color string at render time to compute its derivatives, so we
  // can't pass var(--brand-primary) directly. Keep these two values in sync.
  const brandPrimary = isDark ? '#4858e0' : '#1227ce';

  React.useLayoutEffect(() => {
    // Cold-start theme is set by the inline script in public/index.html so the
    // first paint matches the stored preference. This effect keeps the class in
    // sync when the user toggles themes.
    const root = document.documentElement;
    if (isDark) {
      root.classList.add('dark-theme');
    } else {
      root.classList.remove('dark-theme');
    }
    window.localStorage.setItem(THEME_STORAGE_KEY, String(isDark));
  }, [isDark]);

  return renderFromPromiseResult(applicationConfigResult, () => (
    <ConfigProvider
      theme={{
        token: { colorPrimary: brandPrimary },
        algorithm: isDark ? theme.darkAlgorithm : theme.defaultAlgorithm,
        components: {
          Menu: {
            collapsedWidth: 56,
            ...(isDark ? { itemSelectedColor: '#ebebeb' } : {}),
          },
        },
      }}
    >
      <AntdApp>
        <Router history={history}>
          <Switch>
            <Route path="/events/:eventId">
              <EventPage />
            </Route>
            <Route>
              <NavBar>
                <Route exact path={[Routes.SAVED_QUERY, Routes.SAVED_QUERY_LATEST]}>
                  <SavedQueryBar />
                </Route>
                <Route exact path={Routes.ENTITY}>
                  <EntityViewBar />
                </Route>
                <Switch>
                  <Route path={Routes.QUERY_HISTORY}>
                    <QueryHistory />
                  </Route>
                  <Route path={Routes.SAVED_QUERIES}>
                    <SavedQueries />
                  </Route>
                  <Route path={Routes.DOCS_UDFS}>
                    <UdfDocsView />
                  </Route>
                  <Route path={Routes.BULK_JOB_HISTORY}>
                    <BulkJobHistoryView />
                  </Route>
                  <Route path={Routes.RULES_VISUALIZER}>
                    <RulesVisualizerView />
                  </Route>
                  <Route path={Routes.FEATURES}>
                    <FeaturesPage />
                  </Route>
                  <Route exact path={[Routes.ENTITY, Routes.HOME, Routes.SAVED_QUERY]}>
                    <QueryView />
                  </Route>
                  <Route exact path={Routes.SAVED_QUERY_LATEST}>
                    <div className={styles.spinner}>
                      <Spin size="large" />
                    </div>
                  </Route>
                  <Route exact path={Routes.BULK_ACTION} component={BulkActionPage} />
                </Switch>
              </NavBar>
            </Route>
          </Switch>
        </Router>
      </AntdApp>
    </ConfigProvider>
  ));
};

export default AppRouter;
