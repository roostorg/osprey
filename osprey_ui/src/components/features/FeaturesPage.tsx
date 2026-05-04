import * as React from 'react';
import { Card, Empty, Input, Pagination, Select, Spin, Switch, Tag, Tooltip } from 'antd';
import { SearchOutlined } from '@ant-design/icons';

import { getFeaturesList } from '../../actions/FeaturesActions';
import { FeatureInfo, FeaturesListResponse } from '../../types/FeaturesTypes';

import styles from './FeaturesPage.module.css';

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

  // Reset to page 1 on any filter/sort change.
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

  return (
    <div className={styles.viewContainer}>
      <div className={styles.scrollArea}>
        <div className={styles.pageTitle}>Features Registry</div>
        <div className={styles.pageSubtitle}>
          Extracted data fields from event payloads — the queryable dimensions rules reference. Sourced from{' '}
          <code>models/</code>, <code>lib/</code>, and <code>counters/</code> in the rules engine.
        </div>

        {loading ? (
          <div className={styles.loadingContainer}>
            <Spin size="large" />
          </div>
        ) : error ? (
          <div className={styles.emptyState}>Failed to load features: {error}</div>
        ) : (
          <>
            <div className={styles.statsRow}>
              <Tooltip title="Total extracted features — named values produced by feature-extraction functions like JsonData, Entity, and SnowflakeAge.">
                <Card className={styles.statCard} size="small">
                  <div className={styles.statLabel}>Features</div>
                  <div className={styles.statValue}>{data?.total ?? 0}</div>
                  <div className={styles.statSub}>{Object.keys(categories).length} categories</div>
                </Card>
              </Tooltip>
              <Tooltip title="Unique extraction functions across all features.">
                <Card className={styles.statCard} size="small">
                  <div className={styles.statLabel}>Extraction functions</div>
                  <div className={styles.statValue}>{Object.keys(extractionFns).length}</div>
                  <div className={styles.statSub}>JsonData, Entity, …</div>
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
                  className={`${styles.statCard} ${styles.statCardClickable} ${
                    unusedOnly ? styles.statCardActive : ''
                  }`}
                  size="small"
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
                  <div className={styles.statLabel}>Unused features</div>
                  <div className={styles.statValue}>{unusedCount}</div>
                  <div className={styles.statSub}>{unusedOnly ? 'filter active' : 'no references'}</div>
                </Card>
              </Tooltip>
            </div>

            <div className={styles.filtersBar}>
              <Input
                size="small"
                prefix={<SearchOutlined />}
                placeholder="Search features..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                allowClear
                className={styles.searchInput}
              />
              <Select
                size="small"
                mode="multiple"
                placeholder="Category"
                className={styles.filterSelect}
                value={categoryFilter}
                onChange={setCategoryFilter}
                options={categoryOptions}
                allowClear
                maxTagCount="responsive"
              />
              <Select
                size="small"
                mode="multiple"
                placeholder="Extraction fn"
                className={styles.filterSelect}
                value={extractionFnFilter}
                onChange={setExtractionFnFilter}
                options={extractionFnOptions}
                allowClear
                maxTagCount="responsive"
              />
              <Select
                size="small"
                value={sortKey}
                onChange={(v) => setSortKey(v)}
                className={styles.sortSelect}
                options={[
                  { value: 'most-referenced', label: 'Most referenced' },
                  { value: 'least-referenced', label: 'Least referenced' },
                  { value: 'name', label: 'Name (A-Z)' },
                ]}
              />
              <span className={styles.unusedToggle}>
                <Switch size="small" checked={unusedOnly} onChange={setUnusedOnly} />
                Unused only
              </span>
            </div>

            <div className={styles.sectionHeader}>Features ({filtered.length})</div>
            {filtered.length === 0 ? (
              <Empty description="No features match the current filters" />
            ) : (
              <>
                {paginated.map((f) => (
                  <FeatureCard key={f.name} feature={f} />
                ))}
                <div className={styles.paginationBar}>
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
                  />
                </div>
              </>
            )}
          </>
        )}
      </div>
    </div>
  );
};

const FeatureCard: React.FC<{ feature: FeatureInfo }> = ({ feature }) => {
  const [expanded, setExpanded] = React.useState(false);
  const isUnused = feature.total_references === 0;
  const toggle = () => setExpanded((e) => !e);
  return (
    <div
      className={`${styles.itemCard} ${expanded ? styles.itemCardExpanded : ''}`}
      role="button"
      tabIndex={0}
      aria-expanded={expanded}
      onClick={toggle}
      onKeyDown={(e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          toggle();
        }
      }}
    >
      <div className={styles.itemHeader}>
        <code className={styles.itemName}>{feature.name}</code>
        {feature.type_annotation && (
          <Tag color="blue" className={styles.inlineBadge}>
            {feature.type_annotation}
          </Tag>
        )}
        <Tag color="purple" className={styles.inlineBadge}>
          {feature.extraction_fn}
        </Tag>
        {isUnused && (
          <Tooltip title="This feature is defined but no rule, WhenRules, or other feature references it. Possible cleanup candidate.">
            <Tag color="orange" className={styles.inlineBadge}>
              unused
            </Tag>
          </Tooltip>
        )}
        <span className={styles.itemDesc}>{feature.source_file}</span>
        <div className={styles.itemMeta}>
          <span className={styles.itemStat}>
            <strong>{feature.referenced_by_rules.length}</strong> rules
          </span>
          <span className={styles.itemStat}>
            <strong>{feature.referenced_by_features.length}</strong> features
          </span>
          {feature.referenced_by_whenrules > 0 && (
            <span className={styles.itemStat}>
              <strong>{feature.referenced_by_whenrules}</strong> when-rules
            </span>
          )}
        </div>
      </div>
      {expanded && (
        <div className={styles.itemDetail} onClick={(e) => e.stopPropagation()}>
          <div className={styles.detailRow}>
            <span className={styles.detailLabel}>Category</span>
            <span className={styles.detailValue}>
              <Tag className={styles.inlineBadge}>{feature.category}</Tag>
            </span>
          </div>
          <div className={styles.detailRow}>
            <span className={styles.detailLabel}>Source file</span>
            <span className={styles.detailValue}>
              <code>{feature.source_file}</code>
            </span>
          </div>
          <div className={styles.detailRow}>
            <span className={styles.detailLabel}>Definition</span>
            <div className={styles.definitionBlock}>
              {feature.name}
              {feature.type_annotation ? `: ${feature.type_annotation}` : ''} = {feature.definition}
            </div>
          </div>
          <div className={styles.detailRow}>
            <span className={styles.detailLabel}>Rules ({feature.referenced_by_rules.length})</span>
            <span className={styles.detailValue}>
              {feature.referenced_by_rules.length === 0
                ? '—'
                : feature.referenced_by_rules.map((ruleName) => (
                    <span key={ruleName} className={styles.ruleLink}>
                      {ruleName}
                    </span>
                  ))}
            </span>
          </div>
          <div className={styles.detailRow}>
            <span className={styles.detailLabel}>Features ({feature.referenced_by_features.length})</span>
            <span className={styles.detailValue}>
              {feature.referenced_by_features.length === 0
                ? '—'
                : feature.referenced_by_features.map((name) => (
                    <span key={name} className={styles.referenceChip}>
                      {name}
                    </span>
                  ))}
            </span>
          </div>
          {feature.referenced_by_whenrules > 0 && (
            <div className={styles.detailRow}>
              <span className={styles.detailLabel}>WhenRules</span>
              <span className={styles.detailValue}>
                {feature.referenced_by_whenrules} block{feature.referenced_by_whenrules === 1 ? '' : 's'} (effects /
                apply_if)
              </span>
            </div>
          )}
        </div>
      )}
    </div>
  );
};
