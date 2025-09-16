export const Routes = {
  HOME: '/',
  QUERY_HISTORY: '/query-history',
  SAVED_QUERIES: '/saved-queries',
  DOCS_UDFS: '/docs/udfs',
  ENTITY: '/entity/:entityType/:entityId',
  SAVED_QUERY: '/saved-query/:savedQueryId',
  SAVED_QUERY_LATEST: '/saved-query/:savedQueryId/latest',
  BULK_JOB_HISTORY: '/bulk-job-history',
  RULES_VISUALIZER: '/rules-visualizer',
  BULK_ACTION: '/bulk-action',
};

export const DATE_FORMAT = 'M/D/YYYY h:mm:ssa zz';

// These should mirror the `--status-primary` colors in Colors.module.css
export const StatusColors = {
  SUCCESS: '#3e7025',
  ERROR: '#b23a32',
  NEUTRAL: '#45464a',
};

// These should mirror colors in `Colors.module.css`
export const Colors = {
  BACKGROUND_SECONDARY_ALT: '#f2f3f5',

  TEXT_LIGHT_PRIMARY: '#45464a',

  ICON_PRIMARY: '#535a65',
  ICON_MUTED: '#9aa1ac',
};

export const BULK_LABEL_DEFAULT_LIMIT = 100000;
