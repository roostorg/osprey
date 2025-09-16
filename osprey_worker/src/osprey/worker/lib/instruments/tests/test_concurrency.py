# Patch so we can test concurrency
import gevent
import gevent.monkey

gevent.monkey.patch_all()

from unittest.mock import call  # noqa: E402

import pytest  # noqa: E402
from osprey.worker.lib.instruments import concurrency, metrics  # noqa: E402


@pytest.fixture(scope='function', autouse=True)
def datadog(mocker):
    mocker.patch('osprey.worker.lib.instruments.metrics.gauge')


@concurrency('test')
def func(foo, throw=False, sleep=0):
    if throw:
        raise Exception('a')
    gevent.sleep(sleep)
    return foo


def test_single_call():
    func(1)
    metrics.gauge.assert_has_calls([call('test', 1, tags=None), call('test', 0, tags=None)], any_order=False)


def test_exception():
    with pytest.raises(Exception):
        func(1, throw=True)

    metrics.gauge.assert_has_calls([call('test', 1, tags=None), call('test', 0, tags=None)], any_order=False)


def test_concurrent():
    greenlets = []
    for _ in range(0, 3):
        greenlets.append(gevent.spawn(func, 1, sleep=1))

    gevent.joinall(greenlets, timeout=5)

    metrics.gauge.assert_has_calls(
        [call('test', n, tags=None) for n in [1, 2, 3, 2, 1, 0]],
        any_order=False,
    )
