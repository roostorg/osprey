import * as React from 'react';
import {
  Alert,
  Button,
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
import { EditOutlined, PlusOutlined, SearchOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';

import { getPendingRuleDrafts, getRulesList } from '../../actions/RulesActions';
import usePromiseResult, { PromiseResultStatus } from '../../hooks/usePromiseResult';
import { PendingDraft, PendingDraftsResponse, RuleInfo, RulesListResponse, SortKey } from '../../types/RulesTypes';
import { renderFromPromiseResult } from '../../utils/PromiseResultUtils';

import styles from './RulesPage.module.css';

const { Title, Paragraph, Text } = Typography;

type FiltersState = {
  search: string;
  unusedOnly: boolean;
  sortKey: SortKey;
  page: number;
  pageSize: number;
};

type FiltersAction =
  | { type: 'setSearch'; value: string }
  | { type: 'setUnusedOnly'; value: boolean }
  | { type: 'toggleUnusedOnly' }
  | { type: 'setSortKey'; value: SortKey }
  | { type: 'setPage'; page: number; pageSize: number };

const INITIAL_FILTERS: FiltersState = {
  search: '',
  unusedOnly: false,
  sortKey: SortKey.MostReferenced,
  page: 1,
  pageSize: 50,
};

// Every filter action resets page to 1; only setPage preserves it.
function filtersReducer(state: FiltersState, action: FiltersAction): FiltersState {
  switch (action.type) {
    case 'setSearch': {
      return { ...state, search: action.value, page: 1 };
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

export const RulesPage: React.FC = () => {
  const result = usePromiseResult(() => {
    return getRulesList();
  });
  const pendingResult = usePromiseResult<PendingDraftsResponse>(() => {
    return getPendingRuleDrafts();
  });

  return renderFromPromiseResult(result, (data) => {
    return <RulesPageContent data={data} pendingResult={pendingResult} />;
  });
};

const RulesPageContent: React.FC<{
  data: RulesListResponse;
  pendingResult: ReturnType<typeof usePromiseResult<PendingDraftsResponse>>;
}> = ({ data, pendingResult }) => {
  const [filters, dispatch] = React.useReducer(filtersReducer, INITIAL_FILTERS);
  const { rules, total, when_rules_total, unused_total } = data;
  const { search, unusedOnly, sortKey, page, pageSize } = filters;

  const filtered = React.useMemo(() => {
    const query = search.trim().toLowerCase();
    const list = rules.filter((r) => {
      if (
        query &&
        !r.name.toLowerCase().includes(query) &&
        !r.source_file.toLowerCase().includes(query) &&
        !r.description.toLowerCase().includes(query)
      ) {
        return false;
      }
      if (unusedOnly && r.referenced_by_whenrules !== 0) {
        return false;
      }
      return true;
    });
    if (sortKey === SortKey.Name) {
      return [...list].sort((a, b) => {
        return a.name.localeCompare(b.name);
      });
    }
    if (sortKey === SortKey.MostReferenced) {
      return [...list].sort((a, b) => {
        return b.referenced_by_whenrules - a.referenced_by_whenrules || a.name.localeCompare(b.name);
      });
    }
    return [...list].sort((a, b) => {
      return a.referenced_by_whenrules - b.referenced_by_whenrules || a.name.localeCompare(b.name);
    });
  }, [rules, search, unusedOnly, sortKey]);

  const paginated = React.useMemo(() => {
    return filtered.slice((page - 1) * pageSize, page * pageSize);
  }, [filtered, page, pageSize]);

  const collapseItems = React.useMemo(() => {
    return paginated.map((r) => {
      return {
        key: r.name,
        label: <RuleHeader rule={r} />,
        children: <RuleDetail rule={r} />,
      };
    });
  }, [paginated]);

  return (
    <div className={styles.viewContainer}>
      <div className={styles.scrollArea}>
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'flex-start',
            gap: 16,
            marginBottom: 4,
          }}
        >
          <div>
            <Title level={3} style={{ marginBottom: 4 }}>
              Rules Registry
            </Title>
            <Paragraph type="secondary">
              Named rule definitions across the engine — conditions, descriptions, the features each rule references,
              and how many WhenRules blocks include it.
            </Paragraph>
          </div>
          <Link to="/rules/new">
            <Button type="primary" icon={<PlusOutlined />}>
              Add rule
            </Button>
          </Link>
        </div>

        <PendingDraftsBanner pendingResult={pendingResult} />

        <div className={styles.statsRow}>
          <Card size="small">
            <Statistic title="Rules" value={total} />
          </Card>
          <Tooltip title="Total WhenRules blocks across all sources.">
            <Card size="small">
              <Statistic title="WhenRules" value={when_rules_total} />
            </Card>
          </Tooltip>
          <Tooltip
            title={
              unusedOnly
                ? 'Filtering to unused only — click to clear.'
                : 'Rules with no references from WhenRules blocks — cleanup candidates. Click to filter.'
            }
          >
            <Card
              size="small"
              className={`${styles.statCardClickable} ${unusedOnly ? styles.statCardActive : ''}`}
              role="button"
              tabIndex={0}
              aria-pressed={unusedOnly}
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
              <Statistic title="Unused rules" value={unused_total} />
            </Card>
          </Tooltip>
        </div>

        <Space wrap style={{ marginBottom: 12 }}>
          <Input
            size="small"
            prefix={<SearchOutlined />}
            placeholder="Search rules..."
            value={search}
            onChange={(e) => {
              dispatch({ type: 'setSearch', value: e.target.value });
            }}
            allowClear
            style={{ width: 280 }}
          />
          <Select<SortKey>
            size="small"
            value={sortKey}
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
              checked={unusedOnly}
              onChange={(value) => {
                dispatch({ type: 'setUnusedOnly', value });
              }}
            />
            <span style={{ fontSize: 12 }}>Unused only</span>
          </Space>
        </Space>

        <Title level={5} style={{ marginTop: 8 }}>
          Rules ({filtered.length})
        </Title>
        {filtered.length === 0 ? (
          <Empty description="No rules match the current filters" />
        ) : (
          <>
            <Collapse items={collapseItems} bordered={false} />
            <Pagination
              current={page}
              pageSize={pageSize}
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

const RuleHeader: React.FC<{ rule: RuleInfo }> = ({ rule }) => {
  const isUnused = rule.referenced_by_whenrules === 0;
  return (
    <div className={styles.headerRow}>
      <code className={styles.ruleName}>{rule.name}</code>
      {isUnused && (
        <Tooltip title="This rule is defined but no WhenRules block references it. Possible cleanup candidate.">
          <Tag color="orange">unused</Tag>
        </Tooltip>
      )}
      <span className={styles.ruleSource}>{rule.source_file}</span>
      <Space size={6} style={{ flexShrink: 0 }}>
        {rule.referenced_by_whenrules > 0 && (
          <Text type="secondary" style={{ fontSize: 11 }}>
            <strong>{rule.referenced_by_whenrules}</strong> when-rules
          </Text>
        )}
        <Tooltip title="Open this rule's source file in the editor.">
          <Link
            to={{ pathname: '/rules/edit', search: `?path=${encodeURIComponent(rule.source_file)}` }}
            onClick={(e) => {
              // Prevent the surrounding Collapse panel from toggling open.
              e.stopPropagation();
            }}
          >
            <Button type="text" size="small" icon={<EditOutlined />}>
              Edit
            </Button>
          </Link>
        </Tooltip>
      </Space>
    </div>
  );
};

const PendingDraftsBanner: React.FC<{
  pendingResult: ReturnType<typeof usePromiseResult<PendingDraftsResponse>>;
}> = ({ pendingResult }) => {
  if (pendingResult.status !== PromiseResultStatus.Resolved) return null;
  const { pending, error } = pendingResult.value;
  if (error && pending.length === 0) {
    // GitHub backend not configured or unreachable; the rest of the page works without it.
    return null;
  }
  if (pending.length === 0) return null;
  return (
    <Alert
      type="info"
      showIcon
      style={{ marginBottom: 16 }}
      message={`${pending.length} pending rule draft${pending.length === 1 ? '' : 's'} awaiting review`}
      description={
        <Space direction="vertical" size={4} style={{ width: '100%' }}>
          {pending.slice(0, 8).map((p, i) => {
            return <PendingDraftRow key={`${p.url}-${i}`} draft={p} />;
          })}
          {pending.length > 8 && <Text type="secondary">+{pending.length - 8} more in review.</Text>}
        </Space>
      }
    />
  );
};

const PendingDraftRow: React.FC<{ draft: PendingDraft }> = ({ draft }) => {
  return (
    <div>
      <a href={draft.url} target="_blank" rel="noopener noreferrer">
        {draft.title}
      </a>{' '}
      <Text type="secondary" style={{ fontSize: 12 }}>
        by {draft.author}, {draft.touched_files.join(', ')}
      </Text>
    </div>
  );
};

const RuleDetail: React.FC<{ rule: RuleInfo }> = ({ rule }) => {
  return (
    <Descriptions
      size="small"
      column={1}
      colon={false}
      styles={{ label: { width: 180, fontWeight: 600, paddingRight: 16 } }}
    >
      <Descriptions.Item label="Source file">
        <code>{rule.source_file}</code>
      </Descriptions.Item>
      <Descriptions.Item label="Description">
        {rule.description ? <Text>{rule.description}</Text> : <Text type="secondary">—</Text>}
      </Descriptions.Item>
      <Descriptions.Item label={`When all (${rule.when_all.length})`}>
        {rule.when_all.length === 0 ? (
          <Text type="secondary">—</Text>
        ) : (
          <Space direction="vertical" size={4} style={{ width: '100%' }}>
            {rule.when_all.map((cond, i) => {
              return (
                <pre key={i} className={styles.conditionBlock}>
                  {cond}
                </pre>
              );
            })}
          </Space>
        )}
      </Descriptions.Item>
      <Descriptions.Item label={`Referenced features (${rule.referenced_features.length})`}>
        {rule.referenced_features.length === 0 ? (
          <Text type="secondary">—</Text>
        ) : (
          <Space size={6} wrap>
            {rule.referenced_features.map((name) => {
              return <code key={name}>{name}</code>;
            })}
          </Space>
        )}
      </Descriptions.Item>
      <Descriptions.Item label="WhenRules">
        {`${rule.referenced_by_whenrules} block${rule.referenced_by_whenrules === 1 ? '' : 's'}`}
      </Descriptions.Item>
    </Descriptions>
  );
};
