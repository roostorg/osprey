import * as React from 'react';
import InfiniteScroll from 'react-infinite-scroller';
import { Redirect } from 'react-router-dom';

import { getSavedQueries } from '../../actions/SavedQueryActions';
import usePromiseResult from '../../hooks/usePromiseResult';
import useSavedQueryStore from '../../stores/SavedQueryStore';
import { SavedQuery } from '../../types/QueryTypes';
import { renderFromPromiseResult } from '../../utils/PromiseResultUtils';
import { makeLatestSavedQueryRoute, makeSavedQueryRoute } from '../../utils/RouteUtils';
import { querySearchContext } from '../query_view/QuerySearchContext';
import DeleteSavedQueryModal from './DeleteSavedQueryModal';
import SavedQueryCard, { SavedQueryActions, SavedQueryCardTypes } from './SavedQueryCard';
import SavedQueryDrawer from './SavedQueryDrawer';

import styles from './SavedQueryList.module.css';
import { splitStringIncludingMatches } from '../../utils/StringUtils';

interface SavedQueryListProps {
  cardType: SavedQueryCardTypes;
  userEmail?: string;
}

const SavedQueryList = ({ cardType, userEmail }: SavedQueryListProps) => {
  const [selectedSavedQuery, setSelectedSavedQuery] = React.useState<SavedQuery | null>(null);
  const [action, setAction] = React.useState<SavedQueryActions | null>(null);
  const [hasMoreSavedQueries, setHasMoreSavedQueries] = React.useState(true);
  const { query: searchQuery, regex } = React.useContext(querySearchContext);

  const { savedQueries, updateSavedQueries } = useSavedQueryStore();

  const savedQueryResults = usePromiseResult(async () => {
    const fetchedSavedQueries = await getSavedQueries(userEmail);
    updateSavedQueries(fetchedSavedQueries);
  }, [userEmail]);

  const handleClearState = () => {
    setSelectedSavedQuery(null);
    setAction(null);
  };

  const setQueryAction = (savedQuery: SavedQuery, action: SavedQueryActions) => {
    setSelectedSavedQuery(savedQuery);
    setAction(action);
  };

  const renderFromAction = () => {
    if (selectedSavedQuery == null) return null;

    switch (action) {
      case SavedQueryActions.RUN_ORIGINAL:
        return <Redirect push to={{ pathname: makeSavedQueryRoute(selectedSavedQuery) }} />;
      case SavedQueryActions.RUN_INTERVAL:
        return <Redirect push to={{ pathname: makeLatestSavedQueryRoute(selectedSavedQuery) }} />;
      case SavedQueryActions.DELETE:
        return <DeleteSavedQueryModal savedQuery={selectedSavedQuery} onCancel={handleClearState} />;
      case SavedQueryActions.SHOW_HISTORY:
        return <SavedQueryDrawer savedQuery={selectedSavedQuery} onClose={handleClearState} />;
      default:
        return null;
    }
  };

  const loadMoreSavedQueries = async () => {
    const lastSavedQuery = savedQueries[savedQueries.length - 1];
    if (lastSavedQuery == null) {
      setHasMoreSavedQueries(false);
      return;
    }

    const newSavedQueries = await getSavedQueries(userEmail, lastSavedQuery.id);

    if (newSavedQueries.length === 0 || newSavedQueries.length % 100 !== 0) {
      setHasMoreSavedQueries(false);
    }

    updateSavedQueries([...savedQueries, ...newSavedQueries]);
  };

  const filterSavedQueries = (query: SavedQuery) => {
    if (!searchQuery || searchQuery === '') return true;
    const queryFilterMatches = splitStringIncludingMatches(query.query.query_filter, searchQuery, regex);
    if (queryFilterMatches.find((val) => val.matched)) return true;
    const queryNameMatches = splitStringIncludingMatches(query.name, searchQuery, regex);
    if (queryNameMatches.find((val) => val.matched)) return true;
    return false;
  };
  return (
    <>
      {renderFromPromiseResult(savedQueryResults, () => (
        <div className={styles.listContainer}>
          <InfiniteScroll pageStart={0} loadMore={loadMoreSavedQueries} hasMore={hasMoreSavedQueries} useWindow={false}>
            {savedQueries.filter(filterSavedQueries).map((savedQuery) => (
              <SavedQueryCard
                key={savedQuery.id}
                savedQuery={savedQuery}
                onMenuItemClick={setQueryAction}
                type={cardType}
              />
            ))}
          </InfiniteScroll>
        </div>
      ))}
      {renderFromAction()}
    </>
  );
};

export default SavedQueryList;
