import * as React from 'react';
import InfiniteScroll from 'react-infinite-scroller';

import { getQueriesHistory } from '../../actions/QueryActions';
import usePromiseResult from '../../hooks/usePromiseResult';
import useQueryStore from '../../stores/QueryStore';
import { QueryRecord } from '../../types/QueryTypes';
import { renderFromPromiseResult } from '../../utils/PromiseResultUtils';
import SaveQueryModal from '../saved_queries/SaveQueryModal';
import QueryRecordCard, { QueryRecordCardTypes } from './QueryRecordCard';

import styles from './QueryHistoryList.module.css';
import { querySearchContext } from '../query_view/QuerySearchContext';
import { splitStringIncludingMatches } from '../../utils/StringUtils';

const QUERY_BATCH_SIZE = 100;

interface QueryHistoryListProps {
  cardType?: QueryRecordCardTypes;
  userEmail?: string;
}

type QueryHistoryListContentProps = {
  initialQueries: QueryRecord[];
} & QueryHistoryListProps;

const QueryHistoryListContent = ({
  cardType = QueryRecordCardTypes.DETAILED,
  initialQueries,
  userEmail,
}: QueryHistoryListContentProps) => {
  const [offset, setOffset] = React.useState<string | undefined>();
  const [accumulatedQueries, setAccumulatedQueries] = React.useState(initialQueries);
  const [hasMoreQueries, setHasMoreQueries] = React.useState(true);
  const [queryIdToSave, setQueryIdToSave] = React.useState<string | null>(null);
  const searchQuery = React.useContext(querySearchContext);

  React.useEffect(() => {
    let isMounted = true;

    async function fetchMoreQueryHistory() {
      const queries = await getQueriesHistory(userEmail, offset);
      if (!isMounted) return;

      setAccumulatedQueries((prevAccQueries) => [...prevAccQueries, ...queries]);

      if (queries.length === 0 || queries.length % QUERY_BATCH_SIZE !== 0) {
        setHasMoreQueries(false);
      }
    }

    if (offset != null) {
      fetchMoreQueryHistory();
    }

    return () => {
      isMounted = false;
    };
  }, [offset, userEmail]);

  React.useEffect(() => {
    setAccumulatedQueries(accumulatedQueries);
  }, [accumulatedQueries, initialQueries]);

  const handleCancelSave = () => {
    setQueryIdToSave(null);
  };

  const renderQueryRecordCard = (query: QueryRecord) => {
    if (query == null) return null;
    return <QueryRecordCard key={query.id} query={query} onSaveQuery={setQueryIdToSave} type={cardType} />;
  };

  const renderSavedQueryModal = () => {
    if (queryIdToSave == null) return;
    return <SaveQueryModal queryId={queryIdToSave} onCancel={handleCancelSave} />;
  };

  const loadMoreQueries = () => {
    const lastQuery = accumulatedQueries[accumulatedQueries.length - 1];

    if (lastQuery != null && lastQuery.id !== offset) {
      setOffset(lastQuery.id);
    }
  };

  const searchQueryFilter = (value: QueryRecord) => {
    if (!searchQuery || searchQuery.query === '') return true;
    const queryMatches = splitStringIncludingMatches(value.query_filter, searchQuery.query, searchQuery.regex);
    return queryMatches.find((val) => val.matched);
  };

  return (
    <div className={styles.listContainer}>
      {renderSavedQueryModal()}
      <InfiniteScroll pageStart={0} loadMore={loadMoreQueries} hasMore={hasMoreQueries} useWindow={false}>
        {accumulatedQueries.filter(searchQueryFilter).map(renderQueryRecordCard)}
      </InfiniteScroll>
    </div>
  );
};

const QueryHistoryList = ({ cardType = QueryRecordCardTypes.DETAILED, userEmail }: QueryHistoryListProps) => {
  const executedQuery = useQueryStore((state) => state.executedQuery);
  const sortOrder = useQueryStore((state) => state.sortOrder);
  const topNTables = useQueryStore((state) => state.topNTables);

  const queryHistoryResult = usePromiseResult(
    () => getQueriesHistory(userEmail),
    [executedQuery, sortOrder, topNTables, userEmail]
  );

  return renderFromPromiseResult(queryHistoryResult, (queries: QueryRecord[]) => (
    <QueryHistoryListContent cardType={cardType} initialQueries={queries} userEmail={userEmail} />
  ));
};

export default QueryHistoryList;
