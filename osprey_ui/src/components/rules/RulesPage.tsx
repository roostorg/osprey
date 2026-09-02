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

import { getRuleDrafts, getRulesList } from '../../actions/RulesActions';
import usePromiseResult, { PromiseResultStatus } from '../../hooks/usePromiseResult';
import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import {
  RuleDraftSummary,
  RuleDraftsListResponse,
  RuleInfo,
  RuleRecordStatus,
  RulesListResponse,
  SortKey,
} from '../../types/RulesTypes';
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
  const draftsResult = usePromiseResult<RuleDraftsListResponse>(() => {
    return getRuleDrafts();
  });

  return renderFromPromiseResult(result, (data) => {
    return <RulesPageContent data={data} draftsResult={draftsResult} />;
  });
};

const RulesPageContent: React.FC<{
  data: RulesListResponse;
  draftsResult: ReturnType<typeof usePromiseResult<RuleDraftsListResponse>>;
}> = ({ data, draftsResult }) => {
  const [filters, dispatch] = React.useReducer(filtersReducer, INITIAL_FILTERS);
  const canEditRules = useApplicationConfigStore((state) => state.canEditRules);
  const { rules, total, when_rules_total, unused_total } = data;
  const { search, unusedOnly, sortKey, page, pageSize } = filters;

  // Rows with an edit already underway, keyed by the path they will deploy to. Editing a
  // rule that has one has to open *that*, not the file on disk: drafts are upserted by
  // path, so an editor opened on the deployed copy would overwrite the in-progress work
  // the moment it saved, with nothing on screen to say so.
  //
  // Deployed rows are deliberately excluded. Their content is what is on disk, so opening
  // the file is the same thing, and it stays correct if the file was edited underneath.
  const draftsByPath = React.useMemo(() => {
    if (draftsResult.status !== PromiseResultStatus.Resolved) return new Map<string, RuleDraftSummary>();
    return new Map(
      draftsResult.value.drafts
        .filter((row) => {
          return row.status === 'draft' || row.status === 'deploy_requested';
        })
        .map((row) => {
          return [row.path, row] as const;
        })
    );
  }, [draftsResult]);

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
        label: <RuleHeader rule={r} draft={draftsByPath.get(r.source_file)} />,
        children: <RuleDetail rule={r} />,
      };
    });
  }, [paginated, draftsByPath]);

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
          {canEditRules ? (
            <Link to="/rules/new">
              <Button type="primary" icon={<PlusOutlined />}>
                Add rule
              </Button>
            </Link>
          ) : (
            // Disabled rather than absent: this is the page's one prominent authoring
            // control, so it is the right place to say the ability is missing. The
            // per-rule Edit buttons below are hidden instead — repeating the same
            // explanation on every row would be noise, and this button already gave it.
            <Tooltip title="Authoring rules needs the CAN_EDIT_RULES ability. Ask an Osprey admin to grant it.">
              <Button type="primary" icon={<PlusOutlined />} disabled>
                Add rule
              </Button>
            </Tooltip>
          )}
        </div>

        <DraftsBanner draftsResult={draftsResult} />

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

const RuleHeader: React.FC<{ rule: RuleInfo; draft?: RuleDraftSummary }> = ({ rule, draft }) => {
  // Read from the store rather than threaded down through `collapseItems`, which builds
  // these inside a memo that would otherwise need the flag as a dependency.
  const canEditRules = useApplicationConfigStore((state) => state.canEditRules);
  const isUnused = rule.referenced_by_whenrules === 0;
  // An edit already in progress for this rule is the thing to open. Drafts are upserted
  // by path, so opening the file instead would let a save quietly replace it.
  const editTarget = draft
    ? {
        search: `?draftId=${encodeURIComponent(draft.id)}`,
        tip: 'Continue the edit already in progress for this rule.',
      }
    : {
        search: `?path=${encodeURIComponent(rule.source_file)}`,
        tip: "Open this rule's source file in the editor.",
      };
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
        {draft && (
          <Tooltip title={`This rule has an edit in progress (${STATUS_LABEL[draft.status] ?? draft.status}).`}>
            <Tag color={STATUS_TAG_COLOUR[draft.status] ?? 'default'}>edit in progress</Tag>
          </Tooltip>
        )}
        {canEditRules && (
          <Tooltip title={editTarget.tip}>
            <Link
              to={{ pathname: '/rules/edit', search: editTarget.search }}
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
        )}
      </Space>
    </div>
  );
};

const DraftsBanner: React.FC<{
  draftsResult: ReturnType<typeof usePromiseResult<RuleDraftsListResponse>>;
}> = ({ draftsResult }) => {
  if (draftsResult.status !== PromiseResultStatus.Resolved) return null;
  // The endpoint returns every row in the rules table, deployed ones included — a row is a
  // rule at some point in its lifecycle, not a permanently separate kind of thing. This
  // banner is only about the part of that lifecycle that is *not yet live*, which is what
  // "drafts" means and what makes it worth a panel above the registry.
  //
  // Deployed rows are dropped rather than listed last: they are already below in the
  // registry, so keeping them here shows the same rule twice, the second time under a
  // heading that contradicts its status.
  const rows = draftsResult.value.drafts.filter((row) => {
    return row.status === 'deploy_requested' || row.status === 'draft';
  });
  if (rows.length === 0) return null;
  // Ordered by how much they want someone's attention: a draft whose author has said it is
  // ready is a queue entry waiting on a person, so it sorts and counts ahead of work still
  // in progress.
  const awaiting = rows.filter((row) => {
    return row.status === 'deploy_requested';
  });
  const inProgress = rows.filter((row) => {
    return row.status === 'draft';
  });
  const ordered = [...awaiting, ...inProgress];
  const headline = [
    awaiting.length > 0 ? `${awaiting.length} awaiting deployment` : null,
    inProgress.length > 0 ? `${inProgress.length} in progress` : null,
  ]
    .filter(Boolean)
    .join(' · ');
  return (
    <Alert
      // Always informational. A queue of drafts waiting on someone is not a fault, and
      // colouring it as one means the page is amber whenever anybody has finished a
      // draft — which is most of the time, at which point the colour stops being read.
      // The count carries the emphasis instead, and the per-row status tag says which.
      type="info"
      showIcon
      style={{ marginBottom: 16 }}
      // No empty case: the banner returns null when nothing is pending, so reaching here
      // guarantees at least one of the two counts is non-zero.
      message={`Rule drafts — ${headline}`}
      description={
        <Space direction="vertical" size={4} style={{ width: '100%' }}>
          {ordered.slice(0, 8).map((row) => {
            return <DraftRow key={row.id} draft={row} />;
          })}
          {ordered.length > 8 && <Text type="secondary">+{ordered.length - 8} more.</Text>}
        </Space>
      }
    />
  );
};

// `deploy_requested` reads as a queue entry rather than a state of the file, so it gets
// the label a reviewer is scanning for. Looked up rather than nested ternaries, and with a
// fallback, so a status added server-side renders its raw value instead of nothing.
const STATUS_LABEL: Partial<Record<RuleRecordStatus, string>> = {
  draft: 'draft',
  deploy_requested: 'awaiting deployment',
  deployed: 'deployed',
};

// Colour tracks "does this want someone's attention", not "is this good". Green marks the
// one row a reviewer is looking for; gold marks work still moving; `deployed` drops to the
// neutral tag because a live rule is the resting state and needs nothing from anyone.
//
// `deployed` cannot also be green — two states sharing a colour is the same as neither
// having one, and these two are opposites in the only way that matters here: one is live,
// the other is explicitly not live yet.
const STATUS_TAG_COLOUR: Partial<Record<RuleRecordStatus, string>> = {
  draft: 'gold',
  deploy_requested: 'green',
  deployed: 'default',
};

// Deploying is deliberately not offered here. It happens in the editor, where the draft's
// SML is on screen — a deploy button next to a filename asks someone to publish a rule
// they are not currently looking at.
const DraftRow: React.FC<{ draft: RuleDraftSummary }> = ({ draft }) => {
  return (
    <div className={styles.draftRow}>
      <Link to={{ pathname: '/rules/edit', search: `?draftId=${encodeURIComponent(draft.id)}` }}>{draft.path}</Link>
      <Tag color={STATUS_TAG_COLOUR[draft.status] ?? 'default'}>{STATUS_LABEL[draft.status] ?? draft.status}</Tag>
      <Text type="secondary" style={{ fontSize: 12 }}>
        by {draft.author}
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
