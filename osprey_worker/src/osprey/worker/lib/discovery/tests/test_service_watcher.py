import pytest
from osprey.worker.lib import etcd
from osprey.worker.lib.discovery.directory import Directory
from osprey.worker.lib.discovery.exceptions import ServiceUnavailable
from osprey.worker.lib.discovery.service import Service
from osprey.worker.lib.etcd.ring import EtcdHashRing, EtcdHashRingMember
from pytest import fixture

SERVICE_NAME = 'test_service'


@fixture(scope='module')
def etcd_client():
    return etcd.EtcdClient()


@fixture(scope='module')
def etcd_key():
    return '/unittests'


@fixture(scope='function')
def directory(request, etcd_client, etcd_key):
    def teardown():
        etcd_client.delete(etcd_key, directory=True, recursive=True)

    request.addfinalizer(teardown)

    return Directory(etcd_key, etcd_client=etcd_client)


@fixture(scope='function')
def ring(etcd_client, etcd_key):
    return EtcdHashRing(etcd_client=etcd_client, key=f'{etcd_key}/{SERVICE_NAME}/ring')


def test_select_skip_instances(directory: Directory, ring: EtcdHashRing):
    ring.add(EtcdHashRingMember(name='localhost:5001', num_replicas=128))
    ring.add(EtcdHashRingMember(name='localhost:5002', num_replicas=128))

    directory.register(Service(SERVICE_NAME, 5001), 5)
    directory.register(Service(SERVICE_NAME, 5002), 5)

    watcher = directory.get_watcher(SERVICE_NAME)

    primary = watcher.select(selector='a', secondaries=1, instances_to_skip=0)
    secondary = watcher.select(selector='a', secondaries=1, instances_to_skip=1)

    assert primary != secondary

    with pytest.raises(ServiceUnavailable):
        watcher.select(selector='a', secondaries=1, instances_to_skip=2)
