import * as React from 'react';
import { Button, List, Checkbox } from 'antd';
import { CheckboxChangeEvent } from 'antd/lib/checkbox';
import { AutoSizer, List as VList, ListRowProps } from 'react-virtualized';
import shallow from 'zustand/shallow';

import useLabelStore from '../../stores/LabelStore';
import { TopNResult } from '../../types/QueryTypes';

import styles from './BulkLabelDrawerList.module.css';

const BulkLabelDrawerList = ({
  entitiesToLabel,
  excludedEntities,
  onRowEntityClick,
  onRowCheckboxChange,
}: {
  entitiesToLabel: TopNResult[];
  excludedEntities: Set<string>;
  onRowEntityClick: (value: string) => void;
  onRowCheckboxChange: (e: CheckboxChangeEvent, value: string) => void;
}) => {
  const [bulkLabelEntityType, bulkLabelFeatureName] = useLabelStore(
    (state) => [state.bulkLabelEntityType, state.bulkLabelFeatureName],
    shallow
  );

  const handleCheckboxChange = (e: CheckboxChangeEvent, value: string) => {
    onRowCheckboxChange(e, value);
  };

  const renderListRows = ({ key, index, style }: ListRowProps) => {
    if (bulkLabelEntityType == null || bulkLabelFeatureName == null) return null;
    const item = entitiesToLabel[index];
    const value = item?.result[0]?.[bulkLabelFeatureName];

    return (
      <List.Item className={styles.listItem} key={key} style={style}>
        <Checkbox
          onChange={(e) => handleCheckboxChange(e, String(value))}
          checked={!excludedEntities.has(String(value))}
        />
        <Button
          type="link"
          onClick={() => onRowEntityClick(String(value))}
        >{`${bulkLabelEntityType} / ${value}`}</Button>
      </List.Item>
    );
  };

  return (
    <AutoSizer>
      {({ width }) => (
        <VList
          className={styles.virtualizedList}
          width={width}
          height={400}
          rowCount={entitiesToLabel.length}
          rowHeight={40}
          rowRenderer={renderListRows}
        />
      )}
    </AutoSizer>
  );
};

export default BulkLabelDrawerList;
