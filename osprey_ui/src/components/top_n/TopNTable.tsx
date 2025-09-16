import { InfoCircleOutlined } from '@ant-design/icons';
import { Table, Tooltip } from 'antd';
import { ColumnProps } from 'antd/lib/table';
import { SortOrder } from 'antd/lib/table/interface';

import { getTopNQueryResults } from '../../actions/EventActions';
import usePromiseResult from '../../hooks/usePromiseResult';
import { wrapEntityValuesWithLabelMenu } from '../../utils/EntityUtils';
import { renderFromPromiseResult } from '../../utils/PromiseResultUtils';
import useQueryStore from '../../stores/QueryStore';

import FeatureNameSelect from './FeatureNameSelect';
import styles from './TopNTable.module.css';
import TopNFooter from './TopNFooter';
import { DimensionDifference, DimensionResult, TopNResult } from '../../types/QueryTypes';

const SortOrders: { [key: string]: SortOrder } = {
  DESCEND: 'descend',
  ASCEND: 'ascend',
};

function getTableColumns(
  featureName: string,
  handleFeatureNameChange: (newDimension: string) => void,
  isPoPEnabled: boolean
): Array<ColumnProps<any>> {
  const columns = [
    {
      dataIndex: 'count',
      key: 'count',
      className: styles.countColumn,
      sortDirections: [SortOrders.DESCEND, SortOrders.ASCEND],
      sorter: (a: any, b: any) => a.count - b.count,
      defaultSortOrder: SortOrders.DESCEND,
      onHeaderCell: () => ({ className: styles.countHeaderCell }),
      title: () => <div className={styles.countColumnTitle}>Count</div>,
      render: (val: number | null) => (val == null ? '---' : val),
      width: isPoPEnabled ? '15%' : '30%',
    },
    {
      title: () => (
        <div className={styles.countColumnTitle} style={{ display: 'flex', justifyContent: 'space-between' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
            <span>Δ PoP</span>
            <Tooltip title="PoP Disabled for Queries >45 days" mouseEnterDelay={0.5}>
              <InfoCircleOutlined style={{ fontSize: '12px' }} />
            </Tooltip>
          </div>
        </div>
      ),
      dataIndex: 'difference',
      key: 'difference',
      width: '20%',
      // Example render that formats the difference and colors it green/red:
      render: (val: number | null) => {
        if (val == null) return 'N/A';
        // Convert fraction to percent
        return <span style={{ color: val >= 0 ? 'green' : 'red' }}>{val}</span>;
      },
      sorter: (a: any, b: any) => a.difference - b.difference,
      defaultSortOrder: SortOrders.DESCEND,
      className: styles.countColumn,
      onHeaderCell: () => ({ className: styles.countHeaderCell }),
      hidden: !isPoPEnabled,
    },
    {
      title: () => <div className={styles.countColumnTitle}>%</div>,
      dataIndex: 'percent_diff',
      key: 'percent_diff',
      width: '10%',
      // Example render that formats the difference and colors it green/red:
      render: (val: number | null) => {
        if (val == null) return 'N/A';
        const style = { color: val >= 0 ? 'green' : 'red' };
        // Convert fraction to percent
        return <span style={style}>{val.toFixed(1) + '%'}</span>;
      },
      sorter: (a: any, b: any) => a.percent_diff - b.percent_diff,
      defaultSortOrder: SortOrders.DESCEND,
      className: styles.countColumn,
      onHeaderCell: () => ({ className: styles.countHeaderCell }),
      hidden: !isPoPEnabled,
    },
    {
      dataIndex: featureName,
      key: featureName,
      title: () => (
        <FeatureNameSelect selectedFeatureName={featureName} onFeatureNameChange={handleFeatureNameChange} />
      ),
      onHeaderCell: () => ({ className: styles.selectHeaderCell }),
      render: (val: string) => (featureName !== '' ? wrapEntityValuesWithLabelMenu(val, featureName) : val),
    },
  ];

  const displayedColumns = columns.filter((column) => !column.hidden);

  return displayedColumns;
}

interface TopNTableProps {
  onRemoveTable: (dimension: string) => void;
  onDimensionChange: (oldDimension: string, newDimension: string) => void;
  onPrecisionChange: (dimension: string, newPrecision: number) => void;
  onTogglePoP: (dimension: string) => void;
  dimension: string; // The column we wish to query https://druid.apache.org/docs/latest/querying/dimensionspecs.html
  showPrecision: boolean; // Whether to show or hide the precision selector on this TopN Table
  precision: number; // The precision to request from the TopN API. Accepts float values between 0 and 1 (i.e. 0.001)
  isPoPEnabled: boolean; // Whether to show or hide the PoP columns on this TopN Table
}

const TopNTable = ({
  onRemoveTable,
  onDimensionChange,
  onPrecisionChange,
  onTogglePoP,
  dimension,
  showPrecision,
  precision,
  isPoPEnabled,
}: TopNTableProps) => {
  const executedQuery = useQueryStore((state) => state.executedQuery);
  const entityFeatureFilters = useQueryStore((state) => state.entityFeatureFilters);

  const topNResult = usePromiseResult(async () => {
    const topN = await getTopNQueryResults({ ...executedQuery, entityFeatureFilters }, dimension, 100, precision);
    const currentPeriod = topN.current_period[0]?.result;
    const comparison = topN.comparison;

    // Disregard any records that don't have a current record on UI
    // Matches bulk label filter behavior
    if (!currentPeriod) {
      return { topN: [], queryDimension: dimension };
    }

    const dataWithDiff = currentPeriod.map((currentRow: DimensionResult) => {
      const key = currentRow[dimension];
      const comparisonRow = comparison?.[0]?.differences.find((row: DimensionDifference) => row.dimension_key === key);
      return {
        [dimension]: key,
        count: currentRow.count,
        difference: comparisonRow ? comparisonRow.difference : null,
        percent_diff: comparisonRow ? comparisonRow.percentage_change : null,
      };
    });
    return { topN: dataWithDiff, queryDimension: dimension };
  }, [executedQuery, entityFeatureFilters, dimension, precision]);

  const handleDimensionChange = (newDimension: string) => {
    onDimensionChange(dimension, newDimension);
  };

  const handlePrecisionChange = (newPrecision: number) => {
    onPrecisionChange(dimension, newPrecision);
  };

  const handleRemoveTable = () => {
    onRemoveTable(dimension);
  };

  const handleTogglePoP = () => {
    onTogglePoP(dimension);
  };

  const tableColumns = getTableColumns(dimension, handleDimensionChange, isPoPEnabled);

  return (
    <div className={styles.tableWrapper}>
      {renderFromPromiseResult(topNResult, ({ topN, queryDimension }) => (
        <>
          <Table
            showSorterTooltip={false}
            columns={tableColumns}
            dataSource={topN}
            rowKey={queryDimension}
            pagination={false}
            size="small"
            scroll={{ y: 400 }}
          />
          <TopNFooter
            onTogglePoP={handleTogglePoP}
            onRemoveTable={handleRemoveTable}
            onPrecisionChange={handlePrecisionChange}
            currentPrecision={precision}
            showPrecision={showPrecision}
            disabled={queryDimension === '' || topN.length === 0}
            dimension={queryDimension}
            isPoPEnabled={isPoPEnabled}
          />
        </>
      ))}
    </div>
  );
};

export default TopNTable;
