from ._events import FullSyncRecursive, IncrementalSyncDelete, IncrementalSyncUpsert


class PassthruMux(object):
    def reset_with_initial_event(self, event):
        pass

    def dedupe_event(self, event):
        yield event


class WatchMux(object):
    """Watch mux takes a stream of events and de-duplicates them, allowing downstream handlers to not
    have to worry about handling events that net in no actual change of data.

    That is to say, EtcdWatcher would very much emit redundant duplicate events when full-syncs happen. This
    processes the event stream and drops events which would otherwise be considered duplicate.
    """

    def __init__(self):
        self._prev_event = None

    def reset_with_initial_event(self, event):
        self._prev_event = event

    def dedupe_event(self, event):
        if event != self._prev_event:
            self._prev_event = event
            yield event


class RecursiveWatchMux(object):
    """Recursive watch mux takes a stream of events and deduplicates them in the context of a recursively watched
    directory.

    With recursive watches, would otherwise emit many redundant full-syncs. This processes all those events, and
    drops, or transforms events into more minimal forms. For example, if a full-sync would net in one key being added,
    and another key being removed, it'll just transform them into their respective incremental syncs.
    """

    _DOES_NOT_EXIST = object()

    def __init__(self):
        self._directory = None

    def reset_with_initial_event(self, event):
        assert isinstance(event, FullSyncRecursive)
        self._directory = {e.key: e.value for e in event.items}

    def dedupe_event(self, event):
        if isinstance(event, FullSyncRecursive):
            if self._directory is None:
                self.reset_with_initial_event(event)
                yield event
            else:
                for event in self._synthesize_incremental_events_from_full_sync(event):
                    yield event

        elif isinstance(event, IncrementalSyncUpsert):
            existing = self._directory.get(event.key, self._DOES_NOT_EXIST)
            if event.value != existing:
                self._directory[event.key] = event.value
                yield event

        elif isinstance(event, IncrementalSyncDelete):
            yield IncrementalSyncDelete(key=event.key, prev_value=self._directory.pop(event.key, None))

    def _synthesize_incremental_events_from_full_sync(self, event):
        assert isinstance(event, FullSyncRecursive)

        # If we are full-syncing to an empty set, could means the root key was deleted,
        # and we can just go ahead and yield that full sync event to the empty item set directly,
        if len(event.items) == 0:
            if self._directory:
                self._directory.clear()
                yield event

            return

        unvisited_keys = set(self._directory)
        for sync_one in event.items:
            unvisited_keys.discard(sync_one.key)
            existing = self._directory.get(sync_one.key, self._DOES_NOT_EXIST)
            if sync_one.value != existing:
                self._directory[sync_one.key] = sync_one.key
                yield IncrementalSyncUpsert(key=sync_one.key, value=sync_one.value)

        for key in sorted(unvisited_keys):
            prev_value = self._directory.pop(key)
            yield IncrementalSyncDelete(key=key, prev_value=prev_value)
