import * as React from 'react';
import { Empty } from 'antd';

import useQueryStore from '../../stores/QueryStore';
import { TopNTable } from '../../types/QueryTypes';
import OspreyButton from '../../uikit/OspreyButton';
import TopNIcon from '../../uikit/icons/TopNIcon';
import Panel from '../common/Panel';
import Table from './TopNTable';

import styles from './TopN.module.css';

const TopN: React.FC = () => {
  const [hasEmptyTable, setHasEmptyTable] = React.useState(false);
  const [topNTables, updateTopNTables] = useQueryStore((state) => [state.topNTables, state.updateTopNTables]);

  const handleAddTable = () => {
    setHasEmptyTable(true);
  };

  const handleRemoveTable = (dimension: string) => {
    if (dimension === '') {
      setHasEmptyTable(false);
      return;
    }

    const updatedTables: Map<string, TopNTable> = new Map(topNTables);
    updatedTables.delete(dimension);
    updateTopNTables(updatedTables);
  };

  const handleUpdateDimension = (oldDimension: string, newDimension: string) => {
    const updatedTables: Map<string, TopNTable> = new Map();
    // Maps retain ordering based on insertion; We are basing our insertion ordering off of that fact
    if (oldDimension === '') {
      setHasEmptyTable(false);
      updatedTables.set(newDimension, new TopNTable(newDimension));
    }
    topNTables.forEach((table, dimension) => {
      if (dimension === oldDimension) {
        updatedTables.set(newDimension, new TopNTable(newDimension));
      } else {
        updatedTables.set(dimension, table);
      }
    });

    updateTopNTables(updatedTables);
  };

  const handleUpdatePrecision = (dimension: string, newPrecision: number) => {
    const updatedTables: Map<string, TopNTable> = new Map(topNTables);
    if (updatedTables.has(dimension)) {
      updatedTables.get(dimension)!.precision = newPrecision;
      updateTopNTables(updatedTables);
    }
  };

  const handleTogglePoP = (dimension: string) => {
    const updatedTables: Map<string, TopNTable> = new Map(topNTables);
    if (updatedTables.has(dimension)) {
      const currentTable = updatedTables.get(dimension);
      if (currentTable) {
        currentTable.isPoPEnabled = !currentTable.isPoPEnabled;
        updateTopNTables(updatedTables);
      }
    }
  };

  const renderTopNTables = () => {
    if (topNTables.size === 0 && !hasEmptyTable) {
      return <Empty style={{ paddingTop: 40 }} />;
    }

    const tablesToRender = [...topNTables.values()];

    if (hasEmptyTable) {
      tablesToRender.unshift(new TopNTable(''));
    }

    return (
      <div className={styles.tableWrapper}>
        {tablesToRender.map((table: TopNTable) => {
          return (
            <Table
              key={table.dimension}
              dimension={table.dimension}
              precision={table.precision}
              showPrecision={table.showPrecision}
              isPoPEnabled={table.isPoPEnabled}
              onRemoveTable={handleRemoveTable}
              onDimensionChange={handleUpdateDimension}
              onPrecisionChange={handleUpdatePrecision}
              onTogglePoP={handleTogglePoP}
            />
          );
        })}
      </div>
    );
  };

  return (
    <Panel
      className={styles.topNPanel}
      title="Top N Results"
      icon={<TopNIcon />}
      titleRight={
        <OspreyButton disabled={hasEmptyTable} onClick={handleAddTable}>
          Add Table
        </OspreyButton>
      }
    >
      {renderTopNTables()}
    </Panel>
  );
};

export default TopN;
