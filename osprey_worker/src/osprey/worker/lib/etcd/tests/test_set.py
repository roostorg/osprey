import pytest
from osprey.worker.lib.etcd import ETCD_HISTORY_BUFFER_SIZE, RequestError, Timeout
from osprey.worker.lib.etcd.set import EtcdSet, ReadOnlyEtcdSet


def test_set_start(etcd_client, etcd_key, dict_value, serializer):
    etcd_client.set(etcd_key, serializer.dumps([dict_value]))

    etcd_set = ReadOnlyEtcdSet(etcd_key, etcd_client, lazy=False, serializer=serializer)

    assert dict_value in etcd_set


def test_set_start_lazy(etcd_client, etcd_key, dict_value, serializer):
    etcd_client.set(etcd_key, serializer.dumps([dict_value]))

    etcd_set = ReadOnlyEtcdSet(etcd_key, etcd_client, lazy=True, serializer=serializer)

    assert dict_value in etcd_set


def test_set_start_broken_connection(etcd_client, etcd_key, dict_key, dict_value, dictionary, serializer, mocker):
    etcd_client.set(etcd_key, serializer.dumps(dictionary))

    class TestError(Exception):
        pass

    # ensure different types of errors can be raised and make it through
    for error in [Timeout(), RequestError('test'), TestError()]:
        with mocker.patch.object(etcd_client, 'send_request', side_effect=error):
            with pytest.raises(type(error)):
                etcd_set = ReadOnlyEtcdSet(etcd_key, etcd_client, lazy=False, serializer=serializer)
                assert dict_key not in etcd_set


def test_set_fall_behind(etcd_client, dict_key, etcd_key, serializer, wait_for_condition):
    etcd_set = ReadOnlyEtcdSet(etcd_key, etcd_client, serializer=serializer)
    etcd_set.watch()

    assert dict_key not in etcd_set

    # stop watching so we fall behind the updates
    etcd_set.stop_watching()

    latest_value = None
    # add more changes to etcd, so that it has enough for this test
    for i in range(0, ETCD_HISTORY_BUFFER_SIZE + 10):
        etcd_client.set(etcd_key, serializer.dumps([i]))
        latest_value = i

    # start watching again and make sure we can catch up
    etcd_set.watch()
    wait_for_condition(lambda: latest_value in etcd_set)


def test_set_start_from_old_key(etcd_client, etcd_key, etcd_key_2, dict_value, serializer, wait_for_condition):
    # add the data to etcd for our key
    etcd_client.set(etcd_key, serializer.dumps([dict_value]))

    # add more changes to etcd to a different key
    for i in range(0, ETCD_HISTORY_BUFFER_SIZE):
        etcd_client.set(etcd_key_2, serializer.dumps([i]))

    # start watching and make sure we can catch up without resynchronizing
    etcd_set = ReadOnlyEtcdSet(etcd_key, etcd_client, serializer=serializer)
    etcd_set.watch()
    wait_for_condition(lambda: dict_value in etcd_set)


def test_set_get(etcd_client, etcd_key, dict_value, serializer, wait_for_condition):
    etcd_set = ReadOnlyEtcdSet(etcd_key, etcd_client, serializer=serializer)
    etcd_set.watch()

    assert dict_value not in etcd_set

    etcd_client.set(etcd_key, serializer.dumps([dict_value]))

    wait_for_condition(lambda: dict_value in etcd_set)


def test_set_updates_when_key_is_deleted(
    etcd_client, etcd_key, dict_key, dict_value, dictionary, serializer, wait_for_condition
):
    etcd_set = ReadOnlyEtcdSet(etcd_key, etcd_client, serializer=serializer)
    etcd_set.watch()

    assert dict_value not in etcd_set

    etcd_client.set(etcd_key, serializer.dumps([dict_value]))

    wait_for_condition(lambda: dict_value in etcd_set)

    etcd_client.delete(etcd_key)

    wait_for_condition(lambda: dict_value not in etcd_set)


def test_set_watch_newly_set_value(etcd_client, etcd_key, dict_value, serializer, wait_for_condition):
    etcd_set = ReadOnlyEtcdSet(etcd_key, etcd_client, serializer=serializer, lazy=False)

    # check race condition where a value is set just after we load, but before watching
    etcd_client.set(etcd_key, serializer.dumps([dict_value]))

    etcd_set.watch()

    wait_for_condition(lambda: dict_value in etcd_set)


def test_set_persist(etcd_client, etcd_key, dict_value, serializer, wait_for_condition):
    etcd_set = ReadOnlyEtcdSet(etcd_key, etcd_client, serializer=serializer)
    etcd_set.watch()

    assert dict_value not in etcd_set

    etcd_set_source = EtcdSet(etcd_key, etcd_client, serializer=serializer)
    etcd_set_source.add(dict_value)

    wait_for_condition(lambda: dict_value in etcd_set)

    etcd_set_source.discard(dict_value)

    wait_for_condition(lambda: dict_value not in etcd_set)


def test_set_update(etcd_client, etcd_key, serializer, wait_for_condition):
    etcd_set = ReadOnlyEtcdSet(etcd_key, etcd_client, serializer=serializer)
    etcd_set.watch()

    new_set = {1, 2, 3}
    etcd_set_source = EtcdSet(etcd_key, etcd_client, serializer=serializer)
    etcd_set_source.update(lambda _: new_set)

    wait_for_condition(lambda: etcd_set == new_set)


def test_set_update_must_return_a_set(etcd_client, etcd_key, serializer):
    etcd_set_source = EtcdSet(etcd_key, etcd_client, serializer=serializer)
    with pytest.raises(TypeError) as e:
        etcd_set_source.update(lambda _: None)

    assert e.match('must return a new set')


def test_set_len(etcd_client, etcd_key, serializer, wait_for_condition):
    etcd_set = EtcdSet(etcd_key, etcd_client, serializer=serializer, lazy=False)
    etcd_set.watch()

    assert len(etcd_set) == 0

    new_set = {1, 2, 3}
    etcd_set.update(lambda _: new_set)

    wait_for_condition(lambda: len(etcd_set) == 3)
    wait_for_condition(lambda: new_set == etcd_set)
