import * as React from 'react';
import { EditOutlined } from '@ant-design/icons';
import { message } from 'antd';
import { matchPath, useLocation, useParams } from 'react-router-dom';

import { getSavedQuery, updateSavedQuery } from '../../actions/SavedQueryActions';
import usePromiseResult from '../../hooks/usePromiseResult';
import useQueryStore, { history } from '../../stores/QueryStore';
import useSavedQueryStore from '../../stores/SavedQueryStore';
import { SavedQuery, TopNTable } from '../../types/QueryTypes';
import OspreyButton, { ButtonColors } from '../../uikit/OspreyButton';
import Text, { TextColors, TextWeights } from '../../uikit/Text';
import { getIntervalFromDateRange } from '../../utils/DateUtils';
import { renderFromPromiseResult } from '../../utils/PromiseResultUtils';
import { queryStateEquals } from '../../utils/QueryUtils';
import { makeSavedQueryRoute } from '../../utils/RouteUtils';
import { getSearchParamsForQueryRecord } from '../../utils/SearchParamUtils';
import EditSavedQueryNameModal from './EditSavedQueryNameModal';

import { Routes } from '../../Constants';
import styles from './SavedQueryBar.module.css';

const SavedQueryBarContent = ({ initialSavedQuery }: { initialSavedQuery: SavedQuery }) => {
  const executedQuery = useQueryStore((state) => state.executedQuery);
  const topNTables = useQueryStore((state) => state.topNTables);
  const charts = useQueryStore((state) => state.charts);
  const sortOrder = useQueryStore((state) => state.sortOrder);

  const { savedQueries, updateSavedQueries } = useSavedQueryStore();

  const [savedQuery, setSavedQuery] = React.useState(initialSavedQuery);
  const [isSubmitting, setIsSubmitting] = React.useState(false);
  const [showEditModal, setShowEditModal] = React.useState(false);

  const updateSavedQueryState = (updatedSavedQuery: SavedQuery) => {
    setSavedQuery(updatedSavedQuery);

    const storedQuery = savedQueries.find((query) => query.id === savedQuery.id);
    if (storedQuery != null) {
      const updatedSavedQueries = savedQueries.map((query) => (query.id === savedQuery.id ? updatedSavedQuery : query));
      updateSavedQueries(updatedSavedQueries);
    }
  };

  const handleUpdateSavedQuery = async () => {
    setIsSubmitting(true);
    const updatedSavedQuery = await updateSavedQuery(savedQuery.id, savedQuery.name, {
      executedQuery,
      topNTables,
      sortOrder,
      charts,
    });

    setIsSubmitting(false);
    updateSavedQueryState(updatedSavedQuery);

    message.success('Saved query updated', 1);
  };

  const renderUpdateOption = () => {
    const { query } = savedQuery;
    const queryState = { executedQuery, topNTables, sortOrder, charts };

    const savedQueryStateTablesMap: Map<string, TopNTable> = new Map();
    query.top_n.forEach((table) => savedQueryStateTablesMap.set(table.dimension, table));
    const savedQueryState = {
      executedQuery: {
        interval: getIntervalFromDateRange(query.date_range),
        start: query.date_range.start,
        end: query.date_range.end,
        queryFilter: query.query_filter,
      },
      topNTables: savedQueryStateTablesMap,
      sortOrder: query.sort_order,
      charts: [],
    };

    if (!queryStateEquals(queryState, savedQueryState)) {
      return (
        <OspreyButton loading={isSubmitting} color={ButtonColors.LINK_GRAY} onClick={handleUpdateSavedQuery}>
          Update saved query
        </OspreyButton>
      );
    }

    return null;
  };

  const handleToggleEditNameModal = () => {
    setShowEditModal(!showEditModal);
  };

  return (
    <>
      <div>
        <Text tag="span" color={TextColors.LIGHT_SECONDARY}>
          Viewing saved query
        </Text>{' '}
        <Text
          tag="span"
          className={styles.savedQueryName}
          color={TextColors.LIGHT_HEADINGS_PRIMARY}
          weight={TextWeights.SEMIBOLD}
        >
          {savedQuery.name}
          <EditOutlined className={styles.editIcon} onClick={handleToggleEditNameModal} />
        </Text>
      </div>
      {renderUpdateOption()}
      {showEditModal ? (
        <EditSavedQueryNameModal
          savedQuery={savedQuery}
          onClose={handleToggleEditNameModal}
          onUpdate={updateSavedQueryState}
        />
      ) : null}
    </>
  );
};

const SavedQueryBar = () => {
  const { savedQueryId } = useParams<{ savedQueryId: string }>();
  const location = useLocation();

  const savedQueryResults = usePromiseResult(async () => {
    const savedQuery = await getSavedQuery(savedQueryId);
    const useInterval = matchPath(location.pathname, { path: Routes.SAVED_QUERY_LATEST }) != null;

    history.replace({
      pathname: makeSavedQueryRoute(savedQuery),
      search: getSearchParamsForQueryRecord(savedQuery.query, useInterval),
    });

    return savedQuery;
  }, [savedQueryId]);

  return (
    <div className={styles.savedQueryBar}>
      {renderFromPromiseResult(
        savedQueryResults,
        (savedQuery) => (
          <SavedQueryBarContent initialSavedQuery={savedQuery} />
        ),
        { renderResolving: () => <div>Fetching saved query data...</div> }
      )}
    </div>
  );
};

export default SavedQueryBar;
