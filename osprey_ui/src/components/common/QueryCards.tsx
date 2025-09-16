import * as React from 'react';
import moment from 'moment-timezone';

import { QueryRecord } from '../../types/QueryTypes';
import Expand, { ExpandButton } from '../../uikit/Expand';
import OverflowTooltip from '../../uikit/OverflowTooltip';
import Text, { TextSizes, TextColors } from '../../uikit/Text';
import { localizeAndFormatTimestamp } from '../../utils/DateUtils';
import QueryFilter from './QueryFilter';

import { DATE_FORMAT } from '../../Constants';
import styles from './QueryCards.module.css';

interface QueryCardActionDataProps {
  user: string;
  action: string;
  timestamp: number;
}

export const QueryCardActionData = ({ user, action, timestamp }: QueryCardActionDataProps) => {
  return (
    <Text size={TextSizes.SMALL} color={TextColors.LIGHT_SECONDARY}>
      <b>{user}</b> {action} {moment.unix(timestamp).format(DATE_FORMAT)}
    </Text>
  );
};

interface QueryCardHeaderProps {
  children: React.ReactNode;
  isCompact?: boolean;
}

export const QueryCardHeader = ({ children, isCompact = false }: QueryCardHeaderProps) => {
  return <div className={isCompact ? styles.cardHeaderCompact : styles.cardHeader}>{children}</div>;
};

interface QueryCardBodyProps {
  query: QueryRecord;
  queryFilterPlaceholder?: React.ReactNode;
  intervalCopy: string;
  isCompact?: boolean;
}

export const QueryCardBody = ({
  query,
  queryFilterPlaceholder = null,
  intervalCopy,
  isCompact = false,
}: QueryCardBodyProps) => {
  const getOverflowTooltipContent = (content: React.ReactNode) => {
    const topNTables =
      query.top_n.length === 0 ? null : (
        <>
          <div>Top N Tables:</div>
          <ul className={styles.overflowTopNList}>
            {query.top_n.map((table) => table.dimension && <li key={table.dimension}>{table.dimension}</li>)}
          </ul>
        </>
      );

    return (
      <>
        {content}
        {topNTables}
      </>
    );
  };

  const renderQueryFilter = () => {
    if (query.query_filter === '') {
      return queryFilterPlaceholder;
    }

    return (
      <>
        <Expand className={styles.queryFilterExpand} rowHeight={20}>
          <QueryFilter queryFilter={query.query_filter} />
        </Expand>
        <ExpandButton />
        {isCompact ? null : <div className={styles.divider} />}
      </>
    );
  };

  const renderQueryInfo = () => {
    const delimiter = ' • ';
    const dateRange = `${localizeAndFormatTimestamp(query.date_range.start)} - ${localizeAndFormatTimestamp(
      query.date_range.end
    )}`;
    const additionalQueryInfo = [
      `${localizeAndFormatTimestamp(query.date_range.start)} - ${localizeAndFormatTimestamp(query.date_range.end)}`,
    ];

    if (intervalCopy !== '') {
      additionalQueryInfo.unshift(intervalCopy);
    }

    if (query.top_n.length > 0) {
      additionalQueryInfo.push(`Top N: ${query.top_n.join(', ')}`);
    }

    return (
      <OverflowTooltip className={styles.additionalInfo} tooltipContent={getOverflowTooltipContent(dateRange)}>
        {additionalQueryInfo.join(delimiter)}
      </OverflowTooltip>
    );
  };

  return (
    <div className={styles.cardBody}>
      {renderQueryFilter()}
      {renderQueryInfo()}
    </div>
  );
};
