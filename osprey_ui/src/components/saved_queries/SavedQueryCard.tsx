import * as React from 'react';
import moment from 'moment-timezone';

import { SavedQuery, IntervalOptions, MomentRangeValues, QueryRecord } from '../../types/QueryTypes';
import { MenuOption } from '../../uikit/DropdownMenu';
import { ExpandContextProvider } from '../../uikit/Expand';
import Text, { TextWeights } from '../../uikit/Text';
import { getIntervalFromDateRange, localizeAndFormatTimestamp } from '../../utils/DateUtils';
import IconDropdownMenu from '../common/IconDropdownMenu';
import { QueryCardBody, QueryCardActionData, QueryCardHeader } from '../common/QueryCards';
import SavedQueryCardButtons from './SavedQueryCardButtons';

import styles from './SavedQueryCard.module.css';
import { querySearchContext } from '../query_view/QuerySearchContext';
import { highlightMatchedText, splitStringIncludingMatches } from '../../utils/StringUtils';

export enum SavedQueryActions {
  SHOW_HISTORY,
  DELETE,
  RUN_ORIGINAL,
  RUN_INTERVAL,
}

export enum SavedQueryCardTypes {
  COMPACT,
  DETAILED,
  VERSION_HISTORY,
}

const SavedQueryMenuOptions = [
  { label: 'Run Original Query', action: SavedQueryActions.RUN_ORIGINAL },
  { label: 'Run Query Using Interval', action: SavedQueryActions.RUN_INTERVAL },
  { label: 'Show Saved Query History', action: SavedQueryActions.SHOW_HISTORY },
  { label: 'Delete Saved Query', action: SavedQueryActions.DELETE },
];

interface SavedQueryCardProps {
  savedQuery: SavedQuery;
  query?: QueryRecord;
  isCompact?: boolean;
  onMenuItemClick?: (savedQuery: SavedQuery, action: SavedQueryActions) => void;
  type?: SavedQueryCardTypes;
}

const SavedQueryCard = ({
  savedQuery,
  query,
  onMenuItemClick,
  type = SavedQueryCardTypes.DETAILED,
}: SavedQueryCardProps) => {
  const searchQuery = React.useContext(querySearchContext);

  const queryRecord = query == null ? savedQuery.query : query;
  const interval = getIntervalFromDateRange(queryRecord.date_range);
  const intervalCopy = interval == null ? '' : IntervalOptions[interval as MomentRangeValues].label;

  const handleMenuItemClick = (action: SavedQueryActions) => {
    onMenuItemClick?.(savedQuery, action);
  };

  const getDropdownMenuOptions = (): MenuOption[] =>
    SavedQueryMenuOptions.filter(
      // If the saved query has no interval (i.e. it's a custom range), do not render the 'Run Interval' option.
      (option) => !(option.action === SavedQueryActions.RUN_INTERVAL && interval == null)
    ).map((option) => ({
      label: option.label,
      onClick: () => handleMenuItemClick(option.action),
    }));

  const highlightedHeaderContent = highlightMatchedText(
    splitStringIncludingMatches(savedQuery.name, searchQuery.query, searchQuery.regex)
  );
  const headerContent = (
    <div className={styles.titleRow}>
      <Text weight={TextWeights.SEMIBOLD}>{highlightedHeaderContent}</Text>
      <IconDropdownMenu options={getDropdownMenuOptions()} />
    </div>
  );

  const highlightedQueryContent = highlightMatchedText(
    splitStringIncludingMatches(savedQuery.query.query_filter, searchQuery.query, searchQuery.regex)
  );

  const actionData = (
    <div className={styles.savedQueryActionData}>
      <QueryCardActionData user={savedQuery.saved_by} timestamp={savedQuery.saved_at} action="saved at" />
    </div>
  );

  switch (type) {
    case SavedQueryCardTypes.COMPACT:
      return (
        <div className={styles.savedQueryCardCompact}>
          {headerContent}
          <div className={styles.detailsRow}>
            <QueryCardActionData user={savedQuery.saved_by} action="•" timestamp={savedQuery.saved_at} />
          </div>
        </div>
      );
    case SavedQueryCardTypes.VERSION_HISTORY:
      return (
        <ExpandContextProvider>
          <div className={styles.queryCard}>
            <QueryCardHeader>
              <div className={styles.titleRow}>
                <Text weight={TextWeights.SEMIBOLD}>{`Version saved at: ${localizeAndFormatTimestamp(
                  moment.unix(queryRecord.executed_at)
                )}`}</Text>
              </div>
            </QueryCardHeader>
            <QueryCardBody query={queryRecord} intervalCopy={intervalCopy} />
            {actionData}
          </div>
        </ExpandContextProvider>
      );
    case SavedQueryCardTypes.DETAILED:
      return (
        <ExpandContextProvider>
          <div className={styles.queryCard}>
            <QueryCardHeader>{headerContent}</QueryCardHeader>
            <QueryCardBody query={queryRecord} intervalCopy={intervalCopy} />
            {actionData}
            <SavedQueryCardButtons savedQuery={savedQuery} interval={interval} intervalCopy={intervalCopy} />
          </div>
        </ExpandContextProvider>
      );
  }
};

export default SavedQueryCard;
