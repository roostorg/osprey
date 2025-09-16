# flake8: noqa E402
# ruff: noqa: E402
from gevent.monkey import patch_all

patch_all()

import json

import pytest
from osprey.worker.lib import etcd
from osprey.worker.lib.testing.util import wait_for_condition as wait_for_condition_impl


@pytest.fixture(scope='module')
def etcd_client():
    return etcd.EtcdClient()


@pytest.fixture(scope='function')
def etcd_key(request, etcd_client):
    key = '/etcd_key_test/%s.json' % request.node.name
    cleanup_etcd_key(request, etcd_client, key)
    return key


@pytest.fixture(scope='function')
def etcd_directory(request, etcd_client):
    key = '/etcd_key_test/%s/' % request.node.name
    cleanup_etcd_key(request, etcd_client, key)
    return key


@pytest.fixture(scope='function')
def etcd_key_2(request, etcd_client):
    key = '/etcd_key_test/%s_2.json' % request.node.name
    cleanup_etcd_key(request, etcd_client, key)
    return key


def cleanup_etcd_key(request, etcd_client, key):
    def do_delete():
        try:
            etcd_client.delete(key)
        except etcd.KeyNotFoundError:
            pass

    do_delete()
    request.addfinalizer(do_delete)


@pytest.fixture(scope='function')
def dict_key(request):
    return 'test_key_%s' % request.node.name


@pytest.fixture(scope='function')
def dict_value(request):
    return 'test_value_%s' % request.node.name


@pytest.fixture(scope='function')
def dictionary(dict_key, dict_value):
    return {dict_key: dict_value}


@pytest.fixture(scope='module')
def serializer():
    return json


@pytest.fixture(scope='module')
def wait_for_condition():
    return wait_for_condition_impl
