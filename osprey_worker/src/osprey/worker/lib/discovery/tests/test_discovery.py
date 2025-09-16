from gevent import monkey

monkey.patch_all(aggressive=True)  # noqa: E402

import gevent  # noqa: E402
import pytest  # noqa: E402
from osprey.worker.lib import etcd  # noqa: E402
from osprey.worker.lib.discovery.directory import Directory  # noqa: E402
from osprey.worker.lib.discovery.exceptions import ServiceUnavailable  # noqa: E402
from osprey.worker.lib.discovery.service import Service  # noqa: E402
from pytest import fixture, raises  # noqa: E402


@fixture(scope='function')
def directory(request):
    etcd_client = etcd.EtcdClient()

    def teardown():
        etcd_client.delete('/unittests', directory=True, recursive=True)

    request.addfinalizer(teardown)

    return Directory('/unittests', etcd_client=etcd_client)


def sync(seconds=0.02):
    """Sleeps test greenlet to let things update."""
    gevent.sleep(seconds)


def test_register(directory):
    s1 = Service('a', 5001)
    s2 = Service('b', 5002)

    directory.register(s1, 5)
    directory.register(s2, 5)

    assert directory.names == {'a', 'b'}


def test_unregister(directory):
    s1 = Service('a', 5001)
    directory.register(s1, 5)
    assert directory.names == {'a'}
    directory.unregister(s1)
    sync()
    assert len(list(directory.select_all('a'))) == 0


def test_directory(directory):
    """
    :type directory: discovery.Directory
    """

    service5001 = Service('a', 5001)
    directory.register(service5001, 5)
    service5002 = Service('a', 5002)
    directory.register(service5002, 5)
    directory.register(Service('a', 5003), 5)
    directory.register(Service('a', 5004), 5)
    directory.register(Service('b', 5004), 5)

    # Service `a` should not collide with service `b`
    assert isinstance(directory.select('a'), Service)
    assert len(directory.get_watcher('a')) == 4

    # Unregister an instance.
    directory.unregister(service5002)
    sync()
    assert len(directory.get_watcher('a')) == 3

    # Update an instance.
    service5001.metadata = {'leader': True}
    directory.register(service5001, 5)
    sync()
    for service in directory.select_all('a'):
        if service.id == service5001.id:
            assert service.metadata == service5001.metadata

    def selector(key):
        return lambda s: s.metadata.get(key)

    assert directory.select('a', selector('leader')).id == service5001.id
    assert len(directory.select_all('a', selector('leader'))) == 1

    # Update an instance.
    service5001.metadata = {'follower': True}
    directory.register(service5001, 5)
    sync()
    assert len(directory.select_all('a', selector('leader'))) == 0
    with raises(ServiceUnavailable):
        directory.select('a', selector('leader'))

    # Ensure the previous leader is now a follower.
    assert directory.select('a', selector('follower')).metadata == {'follower': True}
    assert len(directory.select_all('a', selector('follower'))) == 1


def test_watcher_handles_service_instance_removed_between_watcher_restarts(directory):
    """
    This test-case tests that the service watcher properly handles the case
    where a service is de-announced in-between when the watcher is restarted.
    """

    # The service should start out as unavailable.
    with pytest.raises(ServiceUnavailable):
        directory.select('a')

    svcs = [Service('a', 5000 + i) for i in range(5)]

    # Now let's register some services.
    for svc in svcs:
        directory.register(svc, ttl=30)

    sync()

    # Let's ensure that the service now exists in the watcher.
    assert directory.select('a')

    # Let's kill the watcher to emulate the case where the watcher may have died.
    directory.get_watcher('a').stop()
    sync()

    # Now we remove some of the services while the watcher is down.
    directory.unregister(svcs[0])

    sync()

    # Re-selecting will attempt to re-start the watcher. This should work now.
    assert directory.select('a')
    assert len(directory.get_watcher('a')) == 4
    assert len(directory.get_watcher('a')._rotation) == 4


@pytest.mark.xfail(reason='secure etcd not supported in harbormaster and ci')
def test_singleton():
    """
    This test case tests that the singleton method forwards kwargs
    and only creates a single singleton per unique set of kwargs
    """
    basic_instance = Directory.instance()
    secure_instance = Directory.instance(secure=True)
    secure_instance2 = Directory.instance(secure=True)

    assert basic_instance.etcd_client != secure_instance.etcd_client
    assert secure_instance.etcd_client == secure_instance2.etcd_client
    assert secure_instance.etcd_client.auth_token is not None
