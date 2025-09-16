import pytest
from osprey.worker.lib import etcd


@pytest.mark.parametrize('has_initial_key', [True, False])
def test_watcher_non_recursive(etcd_client, etcd_key, has_initial_key):
    watcher = etcd_client.get_watcher(etcd_key, recursive=False)

    if has_initial_key:
        etcd_client.set(etcd_key, 'genesis')
        watcher_iter = iter(watcher.continue_watching())
        assert next(watcher_iter) == etcd.FullSyncOne(key=etcd_key, value='genesis')
    else:
        watcher_iter = iter(watcher.continue_watching())
        assert next(watcher_iter) == etcd.FullSyncOneNoKey(key=etcd_key)

    # Key is either updated or created here.
    etcd_client.set(etcd_key, 'hello')
    assert next(watcher_iter) == etcd.FullSyncOne(key=etcd_key, value='hello')

    # Key is updated.
    etcd_client.set(etcd_key, 'world')
    assert next(watcher_iter) == etcd.FullSyncOne(key=etcd_key, value='world')

    # Key is CAS updated.
    etcd_client.set(etcd_key, 'cas_update', prev_value='world')
    assert next(watcher_iter) == etcd.FullSyncOne(key=etcd_key, value='cas_update')

    # Key is deleted
    etcd_client.delete(etcd_key)
    assert next(watcher_iter) == etcd.FullSyncOneNoKey(key=etcd_key)

    # Key is created
    etcd_client.set(etcd_key, 'hello')
    assert next(watcher_iter) == etcd.FullSyncOne(key=etcd_key, value='hello')

    # Key is CAS deleted.
    etcd_client.delete(etcd_key, prev_value='hello')
    assert next(watcher_iter) == etcd.FullSyncOneNoKey(key=etcd_key)

    # Key is set with TTL
    etcd_client.set(etcd_key, value='ttl', ttl=1)
    assert next(watcher_iter) == etcd.FullSyncOne(key=etcd_key, value='ttl')

    # expire is observed.
    assert next(watcher_iter) == etcd.FullSyncOneNoKey(key=etcd_key)


@pytest.mark.parametrize('has_initial_directory', [True, False])
def test_watch_directory(etcd_client, etcd_directory, has_initial_directory):
    watcher = etcd_client.get_watcher(etcd_directory, recursive=True)

    if has_initial_directory:
        expected = {}
        for k in 'def':
            sub_key = etcd_directory + k + '.json'
            etcd_client.set(sub_key, k)
            expected[sub_key] = k

        watcher_iter = iter(watcher.continue_watching())
        assert next(watcher_iter).as_dict() == expected

    else:
        watcher_iter = iter(watcher.continue_watching())
        assert next(watcher_iter) == etcd.FullSyncRecursive(items=[])

    sub_key = etcd_directory + 'a.json'
    etcd_client.set(sub_key, 'hello')
    assert next(watcher_iter) == etcd.IncrementalSyncUpsert(key=sub_key, value='hello')

    sub_key = etcd_directory + 'b.json'
    etcd_client.set(sub_key, 'hello b')
    assert next(watcher_iter) == etcd.IncrementalSyncUpsert(key=sub_key, value='hello b')

    etcd_client.set(sub_key, 'hello b:2')
    assert next(watcher_iter) == etcd.IncrementalSyncUpsert(key=sub_key, value='hello b:2')

    etcd_client.set(sub_key, 'hello b:cas', prev_value='hello b:2')
    assert next(watcher_iter) == etcd.IncrementalSyncUpsert(key=sub_key, value='hello b:cas')

    etcd_client.delete(sub_key)
    assert next(watcher_iter) == etcd.IncrementalSyncDelete(key=sub_key, prev_value='hello b:cas')

    sub_key = etcd_directory + 'a.json'
    etcd_client.delete(sub_key, prev_value='hello')
    assert next(watcher_iter) == etcd.IncrementalSyncDelete(key=sub_key, prev_value='hello')

    # Set with TTL:
    etcd_client.set(sub_key, value='ttl', ttl=1)
    assert next(watcher_iter) == etcd.IncrementalSyncUpsert(key=sub_key, value='ttl')
    assert next(watcher_iter) == etcd.IncrementalSyncDelete(key=sub_key, prev_value='ttl')

    etcd_client.set(sub_key, value='hello again')
    assert next(watcher_iter) == etcd.IncrementalSyncUpsert(key=sub_key, value='hello again')

    etcd_client.delete(etcd_directory)
    assert next(watcher_iter) == etcd.FullSyncRecursive(items=[])

    subdir = etcd_directory + 'foo/bar/'
    etcd_client.set(subdir + 'x.json', '{}')
    assert next(watcher_iter) == etcd.IncrementalSyncUpsert(key=subdir + 'x.json', value='{}')
    etcd_client.set(subdir + 'y.json', '{}')
    assert next(watcher_iter) == etcd.IncrementalSyncUpsert(key=subdir + 'y.json', value='{}')
    etcd_client.set(subdir + 'z.json', '{}')
    assert next(watcher_iter) == etcd.IncrementalSyncUpsert(key=subdir + 'z.json', value='{}')
    etcd_client.delete(etcd_directory + 'foo')
    assert next(watcher_iter) == etcd.IncrementalSyncDelete(key=etcd_directory + 'foo', prev_value=None)


def test_watcher_handles_falling_too_far_behind(etcd_client, etcd_directory):
    key = etcd_directory + 'foo.json'
    latest_value = 'initial'
    etcd_client.set(key, latest_value)
    watcher = etcd_client.get_watcher(etcd_directory, recursive=True, _use_mux=False)
    watcher_iter = iter(watcher.continue_watching())

    assert next(watcher_iter) == etcd.FullSyncRecursive(items=[etcd.FullSyncOne(key=key, value=latest_value)])

    # Between the last watch iteration, a bunch of sets happen, causing
    # the watcher to fall behind.
    for i in range(0, etcd.ETCD_HISTORY_BUFFER_SIZE + 10):
        latest_value = str(i)
        etcd_client.set(key, latest_value)

    # Attempting to forward the watcher then causes a full-sync.
    assert next(watcher_iter) == etcd.FullSyncRecursive(items=[etcd.FullSyncOne(key=key, value=latest_value)])

    # And that we can proceed with incremental syncs as planned.
    latest_value = 'incremental-sync'
    etcd_client.set(key, latest_value)
    assert next(watcher_iter) == etcd.IncrementalSyncUpsert(key=key, value=latest_value)


def test_watcher_populates_full_tree_in_initial_sync(etcd_client, etcd_directory):
    keys = [
        etcd_directory + 'foo/bar/config.json',
        etcd_directory + 'foo/config.json',
        etcd_directory + 'quz/config.json',
    ]
    for key in keys:
        etcd_client.set(key, '{}')

    watcher = etcd_client.get_watcher(etcd_directory, recursive=True, _use_mux=False)
    watcher_iter = iter(watcher.continue_watching())

    event = next(watcher_iter)
    assert isinstance(event, etcd.FullSyncRecursive)

    for key in keys:
        key_in_items = key in [item.key for item in event.items]
        assert key_in_items, 'key %s not in full recursive sync event' % key

    etcd_client.delete(etcd_directory + 'foo/bar/config.json')

    # Force falling behind so we next get a full sync event.
    for i in range(0, etcd.ETCD_HISTORY_BUFFER_SIZE + 10):
        etcd_client.set(etcd_directory + 'unrelated/dir', str(i))

    event = next(watcher_iter)
    assert isinstance(event, etcd.FullSyncRecursive)

    # Ensure the parent dirs are not in the fully recursive sync items (they have no values).
    assert etcd_directory + 'foo/bar' not in [item.key for item in event.items]


def test_watcher_handles_falling_too_far_behind_due_to_changes_on_unrelated_key(etcd_client, etcd_directory, etcd_key):
    key = etcd_directory + 'foo.json'
    latest_value = 'initial'
    etcd_client.set(key, latest_value)
    watcher = etcd_client.get_watcher(etcd_directory, recursive=True, _use_mux=False)
    watcher_iter = iter(watcher.continue_watching())

    assert next(watcher_iter) == etcd.FullSyncRecursive(items=[etcd.FullSyncOne(key=key, value=latest_value)])

    # Between the last watch iteration, a bunch of sets happen, causing
    # the watcher to fall behind.
    for i in range(0, etcd.ETCD_HISTORY_BUFFER_SIZE + 10):
        etcd_client.set(etcd_key, str(i))

    latest_value = 'latest'
    etcd_client.set(key, latest_value)
    # Attempting to forward the watcher then causes a full-sync.
    assert next(watcher_iter) == etcd.FullSyncRecursive(items=[etcd.FullSyncOne(key=key, value=latest_value)])

    # And that we can proceed with incremental syncs as planned.
    latest_value = 'incremental-sync'
    etcd_client.set(key, latest_value)
    assert next(watcher_iter) == etcd.IncrementalSyncUpsert(key=key, value=latest_value)


def test_watcher_bad_peer(etcd_client, etcd_key, mocker):
    etcd_client.set(etcd_key, 'genesis')
    peers = ','.join(['http://127.255.255.255:53412'] + etcd_client.peers)
    etcd_client = etcd.EtcdClient(peers=peers)
    watcher = etcd_client.get_watcher(etcd_key, recursive=False)

    # could not think of a better way to reliably test this other than making random non-random
    # so we patch `shuffle()` so that we always call the bad peer first
    with mocker.patch('random.shuffle', return_value=etcd_client.peers):
        initial_state = watcher.begin_watching()
        assert initial_state == etcd.FullSyncOne(key=etcd_key, value='genesis')
