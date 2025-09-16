from __future__ import absolute_import

import pytest
from osprey.worker.lib.etcd import ETCD_HISTORY_BUFFER_SIZE, RequestError, Timeout
from osprey.worker.lib.etcd.dict import EtcdDict, ReadOnlyEtcdDict

pytestmark = pytest.mark.timeout(2)


def test_dict_start(etcd_client, etcd_key, dict_key, dict_value, dictionary, serializer):
    etcd_client.set(etcd_key, serializer.dumps(dictionary))

    etcd_dict = ReadOnlyEtcdDict(etcd_key, etcd_client, lazy=False, serializer=serializer)

    value = etcd_dict.get(dict_key)
    assert value == dict_value


def test_dict_start_lazy(etcd_client, etcd_key, dict_key, dict_value, dictionary, serializer):
    etcd_client.set(etcd_key, serializer.dumps(dictionary))

    etcd_dict = ReadOnlyEtcdDict(etcd_key, etcd_client, lazy=True, serializer=serializer)

    value = etcd_dict.get(dict_key)
    assert value == dict_value


def test_dict_start_lazy_with_watcher(etcd_client, etcd_key, dict_key, dict_value, dictionary, serializer):
    etcd_client.set(etcd_key, serializer.dumps(dictionary))

    etcd_dict = ReadOnlyEtcdDict(etcd_key, etcd_client, lazy=True, serializer=serializer)

    watched_updates = []

    def watcher(v):
        watched_updates.append(v)

    etcd_dict.add_watcher(watcher)
    assert len(watched_updates) == 0

    # the dict is lazy, so this first "get" will load the values
    value = etcd_dict.get(dict_key)

    assert len(watched_updates) == 1  # first load should call the watcher
    assert value == dict_value


def test_dict_start_broken_connection(etcd_client, etcd_key, dict_key, dict_value, dictionary, serializer, mocker):
    etcd_client.set(etcd_key, serializer.dumps(dictionary))

    class TestError(Exception):
        pass

    # ensure different types of errors can be raised and make it through
    for error in [Timeout(), RequestError('test'), TestError()]:
        with mocker.patch.object(etcd_client, 'send_request', side_effect=error):
            with pytest.raises(type(error)):
                etcd_dict = ReadOnlyEtcdDict(etcd_key, etcd_client, serializer=serializer)
                etcd_dict.get(dict_key)


def test_dict_fall_behind(etcd_client, etcd_key, dict_key, serializer, wait_for_condition):
    etcd_dict = ReadOnlyEtcdDict(etcd_key, etcd_client, serializer=serializer)
    etcd_dict.watch()

    value = etcd_dict.get(dict_key)
    assert value is None

    # stop watching so we fall behind the updates
    etcd_dict.stop_watching()

    latest_value = None
    # add more changes to etcd, so that it has enough for this test
    for i in range(0, ETCD_HISTORY_BUFFER_SIZE + 10):
        dictionary = {dict_key: i}
        etcd_client.set(etcd_key, serializer.dumps(dictionary))
        latest_value = i

    # start watching again and make sure we can catch up
    etcd_dict.watch()
    wait_for_condition(lambda: etcd_dict.get(dict_key) == latest_value)


def test_dict_start_from_old_key(
    etcd_client, etcd_key, etcd_key_2, dict_key, dict_value, serializer, wait_for_condition
):
    # add the data to etcd for our key
    etcd_client.set(etcd_key, serializer.dumps({dict_key: dict_value}))

    # add more changes to etcd to a different key
    for i in range(0, ETCD_HISTORY_BUFFER_SIZE):
        dictionary = {dict_key: i}
        etcd_client.set(etcd_key_2, serializer.dumps(dictionary))

    # start watching and make sure we can catch up without resynchronizing
    etcd_dict = ReadOnlyEtcdDict(etcd_key, etcd_client, serializer=serializer)
    etcd_dict.watch()

    wait_for_condition(lambda: etcd_dict.get(dict_key) == dict_value)


def test_dict_get(etcd_client, etcd_key, dict_key, dict_value, dictionary, serializer, wait_for_condition):
    etcd_dict = ReadOnlyEtcdDict(etcd_key, etcd_client, serializer=serializer)
    etcd_dict.watch()

    serialized_dict = serializer.dumps(dictionary)
    serialized_empty_dict = serializer.dumps({})

    wait_for_condition(lambda: etcd_dict.get(dict_key) is None)

    etcd_client.set(etcd_key, serialized_dict)
    wait_for_condition(lambda: etcd_dict.get(dict_key) == dict_value)

    etcd_client.set(etcd_key, serialized_empty_dict)
    wait_for_condition(lambda: etcd_dict.get(dict_key) is None)

    etcd_client.set(etcd_key, serialized_dict)
    wait_for_condition(lambda: etcd_dict.get(dict_key) == dict_value)


def test_dict_updates_when_key_is_deleted(
    etcd_client, etcd_key, dict_key, dict_value, dictionary, serializer, wait_for_condition
):
    etcd_dict = ReadOnlyEtcdDict(etcd_key, etcd_client, serializer=serializer)
    etcd_dict.watch()

    wait_for_condition(lambda: etcd_dict.get(dict_key) is None)

    etcd_client.set(etcd_key, serializer.dumps(dictionary))
    wait_for_condition(lambda: etcd_dict.get(dict_key) == dict_value)

    etcd_client.delete(etcd_key)

    wait_for_condition(lambda: etcd_dict.get(dict_key) is None)


def test_dict_watch_newly_set_value(
    etcd_client, etcd_key, dict_key, dict_value, dictionary, serializer, wait_for_condition
):
    etcd_dict = ReadOnlyEtcdDict(etcd_key, etcd_client, serializer=serializer, lazy=False)

    # check race condition where a value is set just after we load, but before watching
    etcd_client.set(etcd_key, serializer.dumps(dictionary))

    etcd_dict.watch()

    wait_for_condition(lambda: etcd_dict.get(dict_key) == dict_value)


def test_dict_watcher(etcd_client, etcd_key, dict_key, dict_value, dictionary, serializer, wait_for_condition):
    etcd_dict = ReadOnlyEtcdDict(etcd_key, etcd_client, serializer=serializer)
    etcd_dict.watch()

    watch_updates = []

    def watcher(d):
        watch_updates.append(d)

    etcd_dict.add_watcher(watcher)

    assert len(watch_updates) == 0

    etcd_client.set(etcd_key, serializer.dumps(dictionary))

    wait_for_condition(lambda: len(watch_updates) > 0 and watch_updates[0].get(dict_key) == dict_value)


def test_dict_persist(etcd_client, etcd_key, dict_key, dict_value, dictionary, serializer, wait_for_condition):
    etcd_dict = ReadOnlyEtcdDict(etcd_key, etcd_client, serializer=serializer)
    etcd_dict.watch()

    value = etcd_dict.get(dict_key)
    assert value is None

    etcd_dict_source = EtcdDict(etcd_key, etcd_client, serializer=serializer)
    etcd_dict_source[dict_key] = dict_value

    wait_for_condition(lambda: etcd_dict.get(dict_key) == dict_value)


def test_double_set_race(etcd_client, etcd_key, serializer):
    etcd_dict = EtcdDict(etcd_key, etcd_client, serializer=serializer)
    etcd_dict.watch()

    etcd_dict['a_key'] = '1'
    assert etcd_dict['a_key'] == '1'
    etcd_dict['b_key'] = '2'
    assert etcd_dict['b_key'] == '2'
    assert etcd_dict['a_key'] == '1'


def test_double_del_race(etcd_client, etcd_key, serializer):
    etcd_dict = EtcdDict(etcd_key, etcd_client, serializer=serializer)
    etcd_dict.watch()

    etcd_dict.replace_with({'a_key': '1', 'b_key': '2'})
    assert etcd_dict['a_key'] == '1'
    assert etcd_dict['b_key'] == '2'
    del etcd_dict['a_key']
    assert 'a_key' not in etcd_dict
    del etcd_dict['b_key']
    assert 'b_key' not in etcd_dict
    assert 'a_key' not in etcd_dict


def test_replace_race(etcd_client, etcd_key, serializer):
    etcd_dict = EtcdDict(etcd_key, etcd_client, serializer=serializer)
    etcd_dict.watch()

    etcd_dict['a_key'] = '3'
    assert etcd_dict['a_key'] == '3'
    etcd_dict.replace_with({'a_key': '4', 'b_key': '5'})
    assert etcd_dict._dict == {'a_key': '4', 'b_key': '5'}


def test_clear_race(etcd_client, etcd_key, serializer):
    etcd_dict = EtcdDict(etcd_key, etcd_client, serializer=serializer)
    etcd_dict.watch()

    etcd_dict['a_key'] = '1'
    assert etcd_dict['a_key'] == '1'
    etcd_dict.clear()
    assert etcd_dict._dict == {}
