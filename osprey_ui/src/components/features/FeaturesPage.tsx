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
  Spin,
  Statistic,
  Switch,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import { SearchOutlined } from '@ant-design/icons';

import { getFeaturesList } from '../../actions/FeaturesActions';
import { FeatureInfo, FeaturesListResponse } from '../../types/FeaturesTypes';

import styles from './FeaturesPage.module.css';

const { Title, Paragraph } = Typography;

type SortKey = 'name' | 'most-referenced' | 'least-referenced';

export const FeaturesPage: React.FC = () => {
  const [data, setData] = React.useState<FeaturesListResponse | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);

  const [search, setSearch] = React.useState('');
  const [categoryFilter, setCategoryFilter] = React.useState<string[]>([]);
  const [extractionFnFilter, setExtractionFnFilter] = React.useState<string[]>([]);
  const [unusedOnly, setUnusedOnly] = React.useState(false);
  const [sortKey, setSortKey] = React.useState<SortKey>('most-referenced');
  const [page, setPage] = React.useState(1);
  const [pageSize, setPageSize] = React.useState(50);

  React.useEffect(() => {
    let cancelled = false;
    setLoading(true);
    getFeaturesList()
      .then((response) => {
        if (!cancelled) {
          setData(response);
          setError(null);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Failed to fetch features');
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const features = data?.features ?? [];
  const categories = data?.categories ?? {};
  const extractionFns = data?.extraction_fns ?? {};

  const filtered = React.useMemo(() => {
    const query = search.trim().toLowerCase();
    const list = features.filter((f) => {
      if (query && !f.name.toLowerCase().includes(query)) return false;
      if (categoryFilter.length > 0 && !categoryFilter.includes(f.category)) return false;
      if (extractionFnFilter.length > 0 && !extractionFnFilter.includes(f.extraction_fn)) return false;
      if (unusedOnly && f.total_references > 0) return false;
      return true;
    });
    if (sortKey === 'name') {
      return [...list].sort((a, b) => a.name.localeCompare(b.name));
    }
    if (sortKey === 'most-referenced') {
      return [...list].sort((a, b) => b.total_references - a.total_references || a.name.localeCompare(b.name));
    }
    return [...list].sort((a, b) => a.total_references - b.total_references || a.name.localeCompare(b.name));
  }, [features, search, categoryFilter, extractionFnFilter, unusedOnly, sortKey]);

  React.useEffect(() => {
    setPage(1);
  }, [search, categoryFilter, extractionFnFilter, unusedOnly, sortKey]);

  const paginated = React.useMemo(
    () => filtered.slice((page - 1) * pageSize, page * pageSize),
    [filtered, page, pageSize]
  );

  const unusedCount = React.useMemo(() => features.filter((f) => f.total_references === 0).length, [features]);

  const categoryOptions = React.useMemo(
    () =>
      Object.entries(categories)
        .sort((a, b) => b[1] - a[1])
        .map(([name, count]) => ({ value: name, label: `${name} (${count})` })),
    [categories]
  );

  const extractionFnOptions = React.useMemo(
    () =>
      Object.entries(extractionFns)
        .sort((a, b) => b[1] - a[1])
        .map(([name, count]) => ({ value: name, label: `${name} (${count})` })),
    [extractionFns]
  );

  const collapseItems = paginated.map((f) => ({
    key: f.name,
    label: <FeatureHeader feature={f} />,
    children: <FeatureDetail feature={f} />,
  }));

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

        {loading ? (
          <Spin size="large" style={{ display: 'block', margin: '48px auto' }} />
        ) : error ? (
          <Empty description={`Failed to load features: ${error}`} />
        ) : (
          <>
            <div className={styles.statsRow}>
              <Tooltip title="Total extracted features — named values produced by feature-extraction functions like JsonData, Entity, and SnowflakeAge.">
                <Card size="small">
                  <Statistic
                    title="Features"
                    value={data?.total ?? 0}
                    suffix={
                      <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                        {Object.keys(categories).length} categories
                      </Typography.Text>
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
                  unusedOnly
                    ? 'Filtering to unused only — click to clear.'
                    : 'Features with no references from rules, WhenRules, or other features — cleanup candidates. Click to filter.'
                }
              >
                <Card
                  size="small"
                  className={`${styles.statCardClickable} ${unusedOnly ? styles.statCardActive : ''}`}
                  role="button"
                  tabIndex={0}
                  aria-pressed={unusedOnly}
                  onClick={() => setUnusedOnly((v) => !v)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                      e.preventDefault();
                      setUnusedOnly((v) => !v);
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
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                allowClear
                style={{ width: 280 }}
              />
              <Select
                size="small"
                mode="multiple"
                placeholder="Category"
                value={categoryFilter}
                onChange={setCategoryFilter}
                options={categoryOptions}
                allowClear
                maxTagCount="responsive"
                style={{ minWidth: 180, maxWidth: 320 }}
              />
              <Select
                size="small"
                mode="multiple"
                placeholder="Extraction fn"
                value={extractionFnFilter}
                onChange={setExtractionFnFilter}
                options={extractionFnOptions}
                allowClear
                maxTagCount="responsive"
                style={{ minWidth: 180, maxWidth: 320 }}
              />
              <Select
                size="small"
                value={sortKey}
                onChange={setSortKey}
                style={{ width: 170 }}
                options={[
                  { value: 'most-referenced', label: 'Most referenced' },
                  { value: 'least-referenced', label: 'Least referenced' },
                  { value: 'name', label: 'Name (A-Z)' },
                ]}
              />
              <Space size={6}>
                <Switch size="small" checked={unusedOnly} onChange={setUnusedOnly} />
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
                  current={page}
                  pageSize={pageSize}
                  total={filtered.length}
                  onChange={(p, ps) => {
                    setPage(p);
                    setPageSize(ps);
                  }}
                  showSizeChanger
                  pageSizeOptions={['25', '50', '100', '200']}
                  showTotal={(total, [start, end]) => `${start}–${end} of ${total}`}
                  size="small"
                  align="center"
                  style={{ marginTop: 20 }}
                />
              </>
            )}
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
        <Typography.Text type="secondary" style={{ fontSize: 11 }}>
          <strong>{feature.referenced_by_rules.length}</strong> rules
        </Typography.Text>
        <Typography.Text type="secondary" style={{ fontSize: 11 }}>
          <strong>{feature.referenced_by_features.length}</strong> features
        </Typography.Text>
        {feature.referenced_by_whenrules > 0 && (
          <Typography.Text type="secondary" style={{ fontSize: 11 }}>
            <strong>{feature.referenced_by_whenrules}</strong> when-rules
          </Typography.Text>
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
            {feature.referenced_by_rules.map((ruleName) => (
              <code key={ruleName}>{ruleName}</code>
            ))}
          </Space>
        )}
      </Descriptions.Item>
      <Descriptions.Item label={`Features (${feature.referenced_by_features.length})`}>
        {feature.referenced_by_features.length === 0 ? (
          '—'
        ) : (
          <Space size={6} wrap>
            {feature.referenced_by_features.map((name) => (
              <code key={name}>{name}</code>
            ))}
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
