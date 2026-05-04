import * as React from 'react';
import {
  Card,
  Collapse,
  Descriptions,
  Empty,
  Input,
  Pagination,
  Select,
  Space,
  Statistic,
  Switch,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import { SearchOutlined } from '@ant-design/icons';

import { getFeaturesList } from '../../actions/FeaturesActions';
import usePromiseResult from '../../hooks/usePromiseResult';
import { FeatureInfo, FeaturesListResponse } from '../../types/FeaturesTypes';
import { renderFromPromiseResult } from '../../utils/PromiseResultUtils';

import styles from './FeaturesPage.module.css';

const { Title, Paragraph, Text } = Typography;

enum SortKey {
  Name = 'name',
  MostReferenced = 'most-referenced',
  LeastReferenced = 'least-referenced',
}

type FiltersState = {
  search: string;
  categoryFilter: string[];
  extractionFnFilter: string[];
  unusedOnly: boolean;
  sortKey: SortKey;
  page: number;
  pageSize: number;
};

type FiltersAction =
  | { type: 'setSearch'; value: string }
  | { type: 'setCategoryFilter'; value: string[] }
  | { type: 'setExtractionFnFilter'; value: string[] }
  | { type: 'setUnusedOnly'; value: boolean }
  | { type: 'toggleUnusedOnly' }
  | { type: 'setSortKey'; value: SortKey }
  | { type: 'setPage'; page: number; pageSize: number };

const INITIAL_FILTERS: FiltersState = {
  search: '',
  categoryFilter: [],
  extractionFnFilter: [],
  unusedOnly: false,
  sortKey: SortKey.MostReferenced,
  page: 1,
  pageSize: 50,
};

// Each filter change resets page to 1; only setPage preserves it. The reducer
// folds that invariant in so we don't need a useEffect that watches every
// filter dep just to reset page.
function filtersReducer(state: FiltersState, action: FiltersAction): FiltersState {
  switch (action.type) {
    case 'setSearch': {
      return { ...state, search: action.value, page: 1 };
    }
    case 'setCategoryFilter': {
      return { ...state, categoryFilter: action.value, page: 1 };
    }
    case 'setExtractionFnFilter': {
      return { ...state, extractionFnFilter: action.value, page: 1 };
    }
    case 'setUnusedOnly': {
      return { ...state, unusedOnly: action.value, page: 1 };
    }
    case 'toggleUnusedOnly': {
      return { ...state, unusedOnly: !state.unusedOnly, page: 1 };
    }
    case 'setSortKey': {
      return { ...state, sortKey: action.value, page: 1 };
    }
    case 'setPage': {
      return { ...state, page: action.page, pageSize: action.pageSize };
    }
  }
}

export const FeaturesPage: React.FC = () => {
  const result = usePromiseResult(() => {
    return getFeaturesList();
  });

  return renderFromPromiseResult(result, (data) => {
    return <FeaturesPageContent data={data} />;
  });
};

const FeaturesPageContent: React.FC<{ data: FeaturesListResponse }> = ({ data }) => {
  const [filters, dispatch] = React.useReducer(filtersReducer, INITIAL_FILTERS);

  const features = data.features;
  const categories = data.categories;
  const extractionFns = data.extraction_fns;

  const filtered = React.useMemo(() => {
    const query = filters.search.trim().toLowerCase();
    const list = features.filter((f) => {
      if (query && !f.name.toLowerCase().includes(query)) {
        return false;
      }
      if (filters.categoryFilter.length > 0 && !filters.categoryFilter.includes(f.category)) {
        return false;
      }
      if (filters.extractionFnFilter.length > 0 && !filters.extractionFnFilter.includes(f.extraction_fn)) {
        return false;
      }
      if (filters.unusedOnly && f.total_references > 0) {
        return false;
      }
      return true;
    });
    if (filters.sortKey === SortKey.Name) {
      return [...list].sort((a, b) => {
        return a.name.localeCompare(b.name);
      });
    }
    if (filters.sortKey === SortKey.MostReferenced) {
      return [...list].sort((a, b) => {
        return b.total_references - a.total_references || a.name.localeCompare(b.name);
      });
    }
    return [...list].sort((a, b) => {
      return a.total_references - b.total_references || a.name.localeCompare(b.name);
    });
  }, [
    features,
    filters.search,
    filters.categoryFilter,
    filters.extractionFnFilter,
    filters.unusedOnly,
    filters.sortKey,
  ]);

  const paginated = React.useMemo(() => {
    return filtered.slice((filters.page - 1) * filters.pageSize, filters.page * filters.pageSize);
  }, [filtered, filters.page, filters.pageSize]);

  const unusedCount = React.useMemo(() => {
    return features.filter((f) => {
      return f.total_references === 0;
    }).length;
  }, [features]);

  const categoryOptions = React.useMemo(() => {
    return Object.entries(categories)
      .sort((a, b) => {
        return b[1] - a[1];
      })
      .map(([name, count]) => {
        return { value: name, label: `${name} (${count})` };
      });
  }, [categories]);

  const extractionFnOptions = React.useMemo(() => {
    return Object.entries(extractionFns)
      .sort((a, b) => {
        return b[1] - a[1];
      })
      .map(([name, count]) => {
        return { value: name, label: `${name} (${count})` };
      });
  }, [extractionFns]);

  const collapseItems = React.useMemo(() => {
    return paginated.map((f) => {
      return {
        key: f.name,
        label: <FeatureHeader feature={f} />,
        children: <FeatureDetail feature={f} />,
      };
    });
  }, [paginated]);

  return (
    <div className={styles.viewContainer}>
      <div className={styles.scrollArea}>
        <Title level={3} style={{ marginBottom: 4 }}>
          Features Registry
        </Title>
        <Paragraph type="secondary">
          Extracted data fields from event payloads — the queryable dimensions rules reference. Sourced from{' '}
          <code>models/</code>, <code>lib/</code>, and <code>counters/</code> in the rules engine.
        </Paragraph>

        <div className={styles.statsRow}>
          <Tooltip title="Total extracted features — named values produced by feature-extraction functions like JsonData, Entity, and SnowflakeAge.">
            <Card size="small">
              <Statistic
                title="Features"
                value={data.total}
                suffix={
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    {Object.keys(categories).length} categories
                  </Text>
                }
              />
            </Card>
          </Tooltip>
          <Tooltip title="Unique extraction functions across all features.">
            <Card size="small">
              <Statistic title="Extraction functions" value={Object.keys(extractionFns).length} />
            </Card>
          </Tooltip>
          <Tooltip
            title={
              filters.unusedOnly
                ? 'Filtering to unused only — click to clear.'
                : 'Features with no references from rules, WhenRules, or other features — cleanup candidates. Click to filter.'
            }
          >
            <Card
              size="small"
              className={`${styles.statCardClickable} ${filters.unusedOnly ? styles.statCardActive : ''}`}
              role="button"
              tabIndex={0}
              aria-pressed={filters.unusedOnly}
              onClick={() => {
                dispatch({ type: 'toggleUnusedOnly' });
              }}
              onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault();
                  dispatch({ type: 'toggleUnusedOnly' });
                }
              }}
            >
              <Statistic title="Unused features" value={unusedCount} />
            </Card>
          </Tooltip>
        </div>

        <Space wrap style={{ marginBottom: 12 }}>
          <Input
            size="small"
            prefix={<SearchOutlined />}
            placeholder="Search features..."
            value={filters.search}
            onChange={(e) => {
              dispatch({ type: 'setSearch', value: e.target.value });
            }}
            allowClear
            style={{ width: 280 }}
          />
          <Select
            size="small"
            mode="multiple"
            placeholder="Category"
            value={filters.categoryFilter}
            onChange={(value) => {
              dispatch({ type: 'setCategoryFilter', value });
            }}
            options={categoryOptions}
            allowClear
            maxTagCount="responsive"
            style={{ minWidth: 180, maxWidth: 320 }}
          />
          <Select
            size="small"
            mode="multiple"
            placeholder="Extraction fn"
            value={filters.extractionFnFilter}
            onChange={(value) => {
              dispatch({ type: 'setExtractionFnFilter', value });
            }}
            options={extractionFnOptions}
            allowClear
            maxTagCount="responsive"
            style={{ minWidth: 180, maxWidth: 320 }}
          />
          <Select<SortKey>
            size="small"
            value={filters.sortKey}
            onChange={(value) => {
              dispatch({ type: 'setSortKey', value });
            }}
            style={{ width: 170 }}
            options={[
              { value: SortKey.MostReferenced, label: 'Most referenced' },
              { value: SortKey.LeastReferenced, label: 'Least referenced' },
              { value: SortKey.Name, label: 'Name (A-Z)' },
            ]}
          />
          <Space size={6}>
            <Switch
              size="small"
              checked={filters.unusedOnly}
              onChange={(value) => {
                dispatch({ type: 'setUnusedOnly', value });
              }}
            />
            <span style={{ fontSize: 12 }}>Unused only</span>
          </Space>
        </Space>

        <Title level={5} style={{ marginTop: 8 }}>
          Features ({filtered.length})
        </Title>
        {filtered.length === 0 ? (
          <Empty description="No features match the current filters" />
        ) : (
          <>
            <Collapse items={collapseItems} bordered={false} />
            <Pagination
              current={filters.page}
              pageSize={filters.pageSize}
              total={filtered.length}
              onChange={(page, pageSize) => {
                dispatch({ type: 'setPage', page, pageSize });
              }}
              showSizeChanger
              pageSizeOptions={['25', '50', '100', '200']}
              showTotal={(total, [start, end]) => {
                return `${start}–${end} of ${total}`;
              }}
              size="small"
              align="center"
              style={{ marginTop: 20 }}
            />
          </>
        )}
      </div>
    </div>
  );
};

const FeatureHeader: React.FC<{ feature: FeatureInfo }> = ({ feature }) => {
  const isUnused = feature.total_references === 0;
  return (
    <div className={styles.headerRow}>
      <code className={styles.featureName}>{feature.name}</code>
      {feature.type_annotation && <Tag color="blue">{feature.type_annotation}</Tag>}
      <Tag color="purple">{feature.extraction_fn}</Tag>
      {isUnused && (
        <Tooltip title="This feature is defined but no rule, WhenRules, or other feature references it. Possible cleanup candidate.">
          <Tag color="orange">unused</Tag>
        </Tooltip>
      )}
      <span className={styles.featureSource}>{feature.source_file}</span>
      <Space size={6} style={{ flexShrink: 0 }}>
        <Text type="secondary" style={{ fontSize: 11 }}>
          <strong>{feature.referenced_by_rules.length}</strong> rules
        </Text>
        <Text type="secondary" style={{ fontSize: 11 }}>
          <strong>{feature.referenced_by_features.length}</strong> features
        </Text>
        {feature.referenced_by_whenrules > 0 && (
          <Text type="secondary" style={{ fontSize: 11 }}>
            <strong>{feature.referenced_by_whenrules}</strong> when-rules
          </Text>
        )}
      </Space>
    </div>
  );
};

const FeatureDetail: React.FC<{ feature: FeatureInfo }> = ({ feature }) => {
  const definition = `${feature.name}${feature.type_annotation ? `: ${feature.type_annotation}` : ''} = ${
    feature.definition
  }`;
  return (
    <Descriptions size="small" column={1} colon={false} labelStyle={{ width: 120, fontWeight: 600 }}>
      <Descriptions.Item label="Category">
        <Tag>{feature.category}</Tag>
      </Descriptions.Item>
      <Descriptions.Item label="Source file">
        <code>{feature.source_file}</code>
      </Descriptions.Item>
      <Descriptions.Item label="Definition">
        <pre className={styles.definitionBlock}>{definition}</pre>
      </Descriptions.Item>
      <Descriptions.Item label={`Rules (${feature.referenced_by_rules.length})`}>
        {feature.referenced_by_rules.length === 0 ? (
          '—'
        ) : (
          <Space size={6} wrap>
            {feature.referenced_by_rules.map((ruleName) => {
              return <code key={ruleName}>{ruleName}</code>;
            })}
          </Space>
        )}
      </Descriptions.Item>
      <Descriptions.Item label={`Features (${feature.referenced_by_features.length})`}>
        {feature.referenced_by_features.length === 0 ? (
          '—'
        ) : (
          <Space size={6} wrap>
            {feature.referenced_by_features.map((name) => {
              return <code key={name}>{name}</code>;
            })}
          </Space>
        )}
      </Descriptions.Item>
      {feature.referenced_by_whenrules > 0 && (
        <Descriptions.Item label="WhenRules">
          {feature.referenced_by_whenrules} block
          {feature.referenced_by_whenrules === 1 ? '' : 's'} (effects / apply_if)
        </Descriptions.Item>
      )}
    </Descriptions>
  );
};
