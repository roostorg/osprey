from osprey.worker.lib.etcd.watcher import (
    FullSyncOne,
    FullSyncOneNoKey,
    FullSyncRecursive,
    IncrementalSyncDelete,
    IncrementalSyncUpsert,
)
from osprey.worker.lib.etcd.watcher._mux import RecursiveWatchMux, WatchMux


def run_events_thru_mux(events_and_expected_outputs, mux):
    for i, (event, expected_output) in enumerate(events_and_expected_outputs):
        output = list(mux.dedupe_event(event))
        assert output == expected_output


def test_watch_mux():
    events = [
        (FullSyncOne(key='a', value='a'), [FullSyncOne(key='a', value='a')]),
        (FullSyncOne(key='a', value='a'), []),
        (FullSyncOne(key='a', value='b'), [FullSyncOne(key='a', value='b')]),
        (FullSyncOneNoKey(key='a'), [FullSyncOneNoKey(key='a')]),
        (FullSyncOneNoKey(key='a'), []),
        (FullSyncOne(key='a', value='a'), [FullSyncOne(key='a', value='a')]),
    ]

    run_events_thru_mux(events, WatchMux())


def test_watch_mux_no_key_start():
    events = [
        (FullSyncOneNoKey(key='a'), [FullSyncOneNoKey(key='a')]),
        (FullSyncOneNoKey(key='a'), []),
        (FullSyncOneNoKey(key='a'), []),
        (FullSyncOne(key='a', value='a'), [FullSyncOne(key='a', value='a')]),
        (FullSyncOne(key='a', value='a'), []),
        (FullSyncOne(key='a', value='b'), [FullSyncOne(key='a', value='b')]),
        (FullSyncOne(key='a', value='a'), [FullSyncOne(key='a', value='a')]),
    ]

    run_events_thru_mux(events, WatchMux())


def test_recursive_mux_full_syncs_are_muxed():
    events = [
        (FullSyncRecursive(items=[]), [FullSyncRecursive(items=[])]),
        (FullSyncRecursive(items=[]), []),
        (FullSyncRecursive(items=[FullSyncOne(key='a', value='a')]), [IncrementalSyncUpsert(key='a', value='a')]),
        (
            FullSyncRecursive(items=[FullSyncOne(key='a', value='a'), FullSyncOne(key='b', value='b')]),
            [IncrementalSyncUpsert(key='b', value='b')],
        ),
        (FullSyncRecursive(items=[FullSyncOne(key='a', value='a')]), [IncrementalSyncDelete(key='b', prev_value='b')]),
        (FullSyncRecursive(items=[]), [FullSyncRecursive(items=[])]),
        (FullSyncRecursive(items=[]), []),
        (
            FullSyncRecursive(items=[FullSyncOne(key='a', value='a'), FullSyncOne(key='b', value='b')]),
            [IncrementalSyncUpsert(key='a', value='a'), IncrementalSyncUpsert(key='b', value='b')],
        ),
        (
            FullSyncRecursive(items=[FullSyncOne(key='c', value='c')]),
            [
                IncrementalSyncUpsert(key='c', value='c'),
                IncrementalSyncDelete(key='a', prev_value='a'),
                IncrementalSyncDelete(key='b', prev_value='b'),
            ],
        ),
    ]

    run_events_thru_mux(events, RecursiveWatchMux())


def test_recursive_mux_full_sync_does_not_re_emit_when_key_differs_from_value():
    # Regression: the dedup directory must store value (not key). In production keys are etcd
    # paths and values are payloads — when they differ, a bug storing the key would cause every
    # subsequent full-sync to spuriously re-emit every previously-changed entry as an upsert.
    events = [
        # Initial state.
        (
            FullSyncRecursive(items=[FullSyncOne(key='/k1', value='v1'), FullSyncOne(key='/k2', value='v2')]),
            [FullSyncRecursive(items=[FullSyncOne(key='/k1', value='v1'), FullSyncOne(key='/k2', value='v2')])],
        ),
        # Full-sync with /k1 actually changing: exercises _synthesize_incremental_events_from_full_sync.
        (
            FullSyncRecursive(items=[FullSyncOne(key='/k1', value='v1-new'), FullSyncOne(key='/k2', value='v2')]),
            [IncrementalSyncUpsert(key='/k1', value='v1-new')],
        ),
        # Full-sync again with no changes: must emit nothing. Previously this spuriously re-emitted /k1.
        (
            FullSyncRecursive(items=[FullSyncOne(key='/k1', value='v1-new'), FullSyncOne(key='/k2', value='v2')]),
            [],
        ),
    ]

    run_events_thru_mux(events, RecursiveWatchMux())


def test_recursive_mux_incrementals():
    events = [
        (
            FullSyncRecursive(items=[FullSyncOne(key='a', value='a')]),
            [FullSyncRecursive(items=[FullSyncOne(key='a', value='a')])],
        ),
        (IncrementalSyncUpsert(key='a', value='a'), []),
        (IncrementalSyncUpsert(key='a', value='b'), [IncrementalSyncUpsert(key='a', value='b')]),
        (IncrementalSyncUpsert(key='b', value='b'), [IncrementalSyncUpsert(key='b', value='b')]),
        (IncrementalSyncDelete(key='b', prev_value='b'), [IncrementalSyncDelete(key='b', prev_value='b')]),
        (IncrementalSyncDelete(key='b', prev_value='b'), [IncrementalSyncDelete(key='b', prev_value=None)]),
    ]

    run_events_thru_mux(events, RecursiveWatchMux())
