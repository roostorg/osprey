import * as React from 'react';
import classNames from 'classnames';
import { Link } from 'react-router-dom';

import { IntervalOptions, QueryRecord, MomentRangeValues } from '../../types/QueryTypes';
import { MenuOption } from '../../uikit/DropdownMenu';
import { ExpandContextProvider } from '../../uikit/Expand';
import Text from '../../uikit/Text';
import { getIntervalFromDateRange } from '../../utils/DateUtils';
import { getSearchParamsForQueryRecord } from '../../utils/SearchParamUtils';
import IconDropdownMenu from '../common/IconDropdownMenu';
import { QueryCardBody, QueryCardActionData, QueryCardHeader } from '../common/QueryCards';
import QueryRecordCardButtons from './QueryRecordCardButtons';

import { Routes } from '../../Constants';
import styles from './QueryRecordCard.module.css';

export enum QueryRecordCardTypes {
  DETAILED,
  SIDEBAR,
}

interface QueryRecordCardProps {
  query: QueryRecord;
  onSaveQuery: React.Dispatch<React.SetStateAction<string | null>>;
  type: QueryRecordCardTypes;
}

const QueryRecordCard = ({ query, onSaveQuery, type = QueryRecordCardTypes.DETAILED }: QueryRecordCardProps) => {
  const interval = getIntervalFromDateRange(query.date_range);
  const intervalCopy = interval == null ? '' : IntervalOptions[interval as MomentRangeValues].label;
  const isSidebarView = type === QueryRecordCardTypes.SIDEBAR;

  const handleSaveQuery = () => {
    onSaveQuery(query.id);
  };

  const renderQueryCardButtons = () => {
    if (isSidebarView) return null;

    return (
      <QueryRecordCardButtons
        query={query}
        interval={interval}
        intervalCopy={intervalCopy}
        onSaveQuery={handleSaveQuery}
      />
    );
  };

  const getMenuOptions = (): MenuOption[] => {
    const menuOptions = [];

    if (interval != null) {
      const link = (
        <Link
          className={styles.menuItem}
          to={() => ({ pathname: Routes.HOME, search: getSearchParamsForQueryRecord(query, true) })}
        >
          Query {intervalCopy}
        </Link>
      );

      menuOptions.push({ label: link });
    }

    const viewOriginal = (
      <Link className={styles.menuItem} to={{ pathname: Routes.HOME, search: getSearchParamsForQueryRecord(query) }}>
        View Original Query
      </Link>
    );

    menuOptions.push({ label: viewOriginal });
    menuOptions.push({
      label: 'Save Query',
      onClick: handleSaveQuery,
    });

    return menuOptions;
  };

  const queryFilterPlaceholder = isSidebarView ? (
    <Text className={styles.placeholder} italic>
      Empty Query
    </Text>
  ) : null;

  return (
    <ExpandContextProvider>
      <div className={classNames(styles.queryCard, { [styles.sidebar]: isSidebarView })}>
        <QueryCardHeader isCompact={isSidebarView}>
          <QueryCardActionData user={query.executed_by} timestamp={query.executed_at} action="queried at" />
          {isSidebarView ? <IconDropdownMenu options={getMenuOptions()} /> : null}
        </QueryCardHeader>
        <QueryCardBody
          query={query}
          intervalCopy={intervalCopy}
          queryFilterPlaceholder={queryFilterPlaceholder}
          isCompact={isSidebarView}
        />
        {renderQueryCardButtons()}
      </div>
    </ExpandContextProvider>
  );
};

export default QueryRecordCard;
