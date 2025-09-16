from __future__ import absolute_import

import os
import uuid

import gevent
from gevent.timeout import with_timeout
from osprey.worker.lib import etcd

LOCK_DIR_KEY = '/locks'


class DistributedLock(object):
    """
    Distributed lock powered by Etcd.
    """

    def __init__(self, key, ttl=5, etcd_client=None):
        """
        Create a new lock

        :type key: str
        :type ttl: int
        :type etcd_client: etcd.EtcdClient
        """
        self.key = key
        self.ttl = ttl
        self.value = None
        self.etcd_client = etcd_client or etcd.EtcdClient()

    def __enter__(self):
        return self.acquire()

    # noinspection PyUnusedLocal
    def __exit__(self, type, value, traceback):
        self.release()

    @property
    def _lock_name(self):
        return os.path.join(LOCK_DIR_KEY, self.key)

    def locked(self):
        if self.value:
            return True

        try:
            self.etcd_client.get(self._lock_name)
            return True

        except etcd.KeyNotFoundError:
            return False

    def acquire(self, blocking=True, timeout=-1):
        """
        Acquire a lock, blocking or non-blocking.

        :type blocking: bool
        :type timeout: int
        :rtype: bool
        """
        if self.value:
            return True

        def acquire():
            while True:
                try:
                    value = uuid.uuid4().hex
                    self.etcd_client.create(self._lock_name, value, ttl=self.ttl)
                    self.value = value
                    return True
                except etcd.NodeExistsError:
                    if not blocking:
                        return False
                    gevent.sleep(1)

        if timeout > 0:
            return with_timeout(timeout, acquire, timeout_value=False)
        else:
            return acquire()

    def release(self):
        """
        Release the lock
        """
        if self.value:
            try:
                self.etcd_client.delete(self._lock_name, prev_value=self.value)
            except etcd.KeyNotFoundError:
                pass
            self.value = None
