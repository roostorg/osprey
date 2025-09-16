import * as React from 'react';
import classNames from 'classnames';
import { chunk } from 'lodash';

import Text, { TextSizes } from '../../uikit/Text';
import BrandDot from '../../uikit/icons/BrandDot';

import styles from './PropertyTable.module.css';

export type Entries = Record<string, string>;
type Entry = [string, string];

interface PropertyTableProps {
  className?: string;
  entries: Entries[];
  columns?: number;
  renderKey?: (entry: Entry) => React.ReactNode;
  renderValue?: (entry: Entry) => React.ReactNode;
  shouldHighlightCell?: (entry: Entry) => boolean;
  shouldWrapText?: boolean;
}

const PropertyTable = ({
  className,
  entries,
  columns = 3,
  renderKey,
  renderValue,
  shouldHighlightCell,
  shouldWrapText = false,
}: PropertyTableProps) => {
  const entriesAsTuples = entries.map((entry) => Object.entries(entry));
  let currentRowIndex = 0;

  const renderRow = (entryTuple: Entry[], index: number) => {
    return (
      <div key={index} className={(index + 1) % 2 === 0 ? styles.darkRow : styles.row}>
        {entryTuple.map((entry) => {
          const [key, value] = entry;
          const highlightCell = shouldHighlightCell?.(entry);

          return (
            <div className={classNames(styles.keyValuePair, { [styles.highlightedCell]: highlightCell })} key={key}>
              <Text className={styles.key} size={TextSizes.SMALL}>
                {renderKey != null ? renderKey(entry) : key}
                {highlightCell ? <BrandDot className={styles.highlightDot} /> : null}
              </Text>
              <div
                title={shouldWrapText ? undefined : value}
                className={shouldWrapText ? styles.value_wrapped : styles.value}
              >
                {renderValue != null ? renderValue(entry) : value}
              </div>
            </div>
          );
        })}
      </div>
    );
  };

  const renderBlock = (entryBlock: Entry[]) => {
    return chunk(entryBlock, columns).map((block) => {
      const row = renderRow(block, currentRowIndex);
      currentRowIndex++;
      return row;
    });
  };

  return <div className={classNames(styles.propertyTable, className)}>{entriesAsTuples.map(renderBlock)}</div>;
};

export default PropertyTable;
