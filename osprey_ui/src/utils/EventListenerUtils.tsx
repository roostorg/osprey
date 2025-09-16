import useApplicationConfigStore from '../stores/ApplicationConfigStore';
import useQueryStore from '../stores/QueryStore';
import { addFeaturesToQueryFilter } from './QueryUtils';
import { openNewQueryWindow } from './SearchParamUtils';

export function startRecordingClicks(e: KeyboardEvent) {
  if (e.key === 'Shift') {
    useApplicationConfigStore.setState({ isRecordingClicks: true });
  }
}

export function stopRecordingClicks(e: KeyboardEvent) {
  if (e.key !== 'Shift') return;
  const { executedQuery, queuedQueryFilters, updateExecutedQuery, updateQueuedQueryFilters } = useQueryStore.getState();

  if (queuedQueryFilters.length > 0) {
    const updatedFilter = addFeaturesToQueryFilter(queuedQueryFilters, executedQuery.queryFilter);
    if (e.ctrlKey || e.metaKey) {
      openNewQueryWindow(updatedFilter);
    } else {
      updateExecutedQuery({ ...executedQuery, queryFilter: updatedFilter });
    }

    updateQueuedQueryFilters([]);
  }

  useApplicationConfigStore.setState({ isRecordingClicks: false });
}
