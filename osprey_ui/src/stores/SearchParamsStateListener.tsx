import { Location } from 'history';
import { isEqual } from 'lodash';

import { extractQueryStateFromSearchParams, setSearchParamsForEntityView } from '../utils/QueryStoreUtils';
import { baseQueryEquals, topNEquals } from '../utils/QueryUtils';
import useQueryStore, { history } from './QueryStore';

import { Routes } from '../Constants';

history.listen((location: Location) => {
  if (location.search === '') {
    if (location.pathname === Routes.HOME) {
      useQueryStore.setState(extractQueryStateFromSearchParams(location));
    } else {
      setSearchParamsForEntityView(history);
    }
    return;
  }

  const { executedQuery, sortOrder, topNTables, customSummaryFeatures, entityFeatureFilters } =
    useQueryStore.getState();
  const newState = extractQueryStateFromSearchParams(location);

  if (!isEqual(newState.customSummaryFeatures, customSummaryFeatures)) {
    useQueryStore.setState({ customSummaryFeatures: newState.customSummaryFeatures });
    return;
  }

  if (!isEqual(newState.entityFeatureFilters, entityFeatureFilters)) {
    useQueryStore.setState({ entityFeatureFilters: newState.entityFeatureFilters });
    return;
  }

  if (!baseQueryEquals(executedQuery, newState.executedQuery)) {
    useQueryStore.setState({ executedQuery: newState.executedQuery });
  }

  if (newState.sortOrder !== sortOrder) {
    useQueryStore.setState({ sortOrder: newState.sortOrder });
  }

  if (!topNEquals(newState.topNTables, topNTables)) {
    useQueryStore.setState({ topNTables: newState.topNTables });
  }
});
