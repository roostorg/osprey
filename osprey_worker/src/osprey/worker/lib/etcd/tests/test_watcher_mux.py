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
