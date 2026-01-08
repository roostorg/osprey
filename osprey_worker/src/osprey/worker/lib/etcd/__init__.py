from __future__ import absolute_import, print_function

import base64
import errno
import json
import logging
import os
import random
import socket
from email.message import Message
from typing import Any, Callable, Optional

import gevent
import six
import six.moves.http_client
import six.moves.urllib.error
import six.moves.urllib.parse
import six.moves.urllib.request
from six.moves import map

from .watcher import (  # noqa F401
    BaseEvent,
    BaseWatcher,
    EtcdWatcher,
    FullSyncOne,
    FullSyncOneNoKey,
    FullSyncRecursive,
    IncrementalSyncDelete,
    IncrementalSyncUpsert,
)

try:
    import grpc
    from osprey.rpc.etcd_watcherd.v1.etcd_watcherd_pb2_grpc import EtcdWatcherdServiceStub

    from .watcher.watcherd_impl import EtcdWatcherdWatcher
except ImportError:
    has_grpc = False
else:
    has_grpc = True


ACTION_GET = 'get'
ACTION_SET = 'set'
ACTION_CREATE = 'create'
ACTION_UPDATE = 'update'
ACTION_DELETE = 'delete'
ACTION_COMPARE_AND_SWAP = 'compareAndSwap'
ACTION_COMPARE_AND_DELETE = 'compareAndDelete'
ACTION_EXPIRE = 'expire'

SET_ACTIONS = {ACTION_SET, ACTION_CREATE, ACTION_COMPARE_AND_SWAP, ACTION_UPDATE}
DELETE_ACTIONS = {ACTION_DELETE, ACTION_EXPIRE, ACTION_COMPARE_AND_DELETE}

ETCD_HISTORY_BUFFER_SIZE = 1000

log = logging.getLogger('etcd')


def _build_url(peer, key):
    """Construct a full URL to an Etcd resource.

    :type peer: str
    :type key: str
    :rtype: str
    """
    return '%s/v2/keys%s' % (peer, key)


def parse_event(headers: Message, body: bytes):
    """Parse an Etcd event or throw exception.

    :type headers: dict
    :type body: str
    :rtype: EtcdEvent
    """
    if isinstance(body, six.string_types + (bytes,)):
        raw_event = json.loads(body)
    action = raw_event.get('action')
    if action:
        node = EtcdNode.from_raw(raw_event['node'])
        raw_prev_node = raw_event.get('prevNode')
        prev_node = raw_prev_node and EtcdNode.from_raw(raw_prev_node)
        etcd_index = int(headers['x-etcd-index'])
        raft_index = int(headers['x-raft-index'])
        raft_term = int(headers['x-raft-term'])
        return EtcdEvent(action, node, prev_node, etcd_index, raft_index, raft_term)
    else:
        raise EtcdError.from_raw(raw_event)


class EtcdError(Exception):
    def __init__(self, code, cause, index, message):
        """Generic Etcd exception.

        :type code: int
        :type cause: str
        :type index: int
        :type message: str
        """
        self.code = code
        self.cause = cause
        self.index = index
        super(EtcdError, self).__init__(cause or message)

    @classmethod
    def from_raw(cls, raw_error):
        """Convert raw dictionary error to EtcdError.

        :type raw_error: dict
        :rtype: EtcdError
        """
        # TODO: make exception for every type
        code = raw_error.get('errorCode', -1)
        actual_cls = {100: KeyNotFoundError, 101: CompareFailedError, 105: NodeExistsError}.get(code, cls)
        return actual_cls(
            code=code, cause=raw_error.get('cause'), index=raw_error.get('index'), message=raw_error.get('message')
        )


class KeyNotFoundError(EtcdError):
    pass


class CompareFailedError(EtcdError):
    pass


class NodeExistsError(EtcdError):
    pass


class RequestError(EtcdError):
    def __init__(self, message):
        super(RequestError, self).__init__(None, None, None, message)


class Timeout(RequestError):
    def __init__(self):
        super(Timeout, self).__init__(None)


class EtcdEvent(object):
    def __init__(self, action, node, prev_node=None, etcd_index=None, raft_index=None, raft_term=None):
        """Event from the Etcd storage log.

        :type action: str
        :type node: EtcdNode
        :type prev_node: EtcdNode|None
        :type etcd_index: int|None
        :type raft_index: int|None
        :type raft_term: int|None
        """
        self.action = action
        self.node = node
        self.prev_node = prev_node
        self.etcd_index = etcd_index
        self.raft_index = raft_index
        self.raft_term = raft_term

    def __repr__(self):
        return '<%s: %s@%s>' % (self.__class__.__name__, self.action, self.node.key)


class EtcdNode(object):
    def __init__(self, key, value, directory, created_index, modified_index, ttl, expiration, nodes):
        """Encapsulated instance of a node.

        :type key: str
        :type value: str|None
        :type directory: bool
        :type created_index: int
        :type modified_index: int
        :type ttl: int|None
        :type expiration: datetime.datetime|None
        :type nodes: list[EtcdNode]
        """
        self.key = key
        self.value = value
        self.created_index = created_index
        self.modified_index = modified_index
        self.ttl = ttl
        self.nodes = nodes

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.key)

    @classmethod
    def from_raw(cls, raw_node):
        """Convert raw dictionary node to EtcdNode.

        :type raw_node: dict
        :rtype: EtcdNode
        """
        if raw_node:
            return cls(
                key=raw_node['key'],
                value=raw_node.get('value'),
                directory=raw_node.get('dir', False),
                created_index=raw_node.get('createdIndex', 0),
                modified_index=raw_node.get('modifiedIndex', 0),
                ttl=raw_node.get('ttl'),
                expiration=raw_node.get('expiration'),
                nodes=list(map(cls.from_raw, raw_node.get('nodes', []))),
            )
        else:
            return None


class EtcdClient(object):
    def __init__(
        self,
        peers=None,
        watcherd_addr=None,
        http_timeout=60,
        secure=False,
        username=None,
        password=None,
        as_guest=False,
    ):
        self.auth_token = None

        if secure:
            env_variable = 'ETCD_SECURE_PEERS'
        else:
            env_variable = 'ETCD_PEERS'

        peers = peers or os.environ.get(env_variable, 'http://127.0.0.1:2379')
        if peers.count(';') == 2:
            env_username, env_password, peers = peers.split(';', 2)
            username = username if username is not None else env_username
            password = password if password is not None else env_password

        if secure and not as_guest:
            if not username or not password:
                raise ValueError('Both username and password must be specified to connect securely to etcd')
            concatted = ':'.join([username, password])
            self.auth_token = base64.urlsafe_b64encode(concatted.encode('utf-8')).decode('utf-8')

        self.peers = peers.split(',')
        self.http_timeout = http_timeout

        if watcherd_addr is None:
            watcherd_addr = os.environ.get('{}_WATCHERD_ADDR'.format(env_variable))

        if not has_grpc and watcherd_addr:
            raise RuntimeError('watcherd_addr (%s) specified, but grpc is not installed.' % watcherd_addr)

        self.watcherd_addr = watcherd_addr
        self._watcherd_client = None

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, ', '.join(self.peers))

    def create(
        self, key: str, value: Optional[str] = None, ttl: Optional[int] = None, sequential=False, directory=False
    ):
        """Create a node with all possible options.

        >>> client = EtcdClient()
        >>> event = client.create('/doctest', sequential=True)
        >>> event.node.key == '/doctest/%s' % event.node.modified_index
        True
        >>> client.create('/doctest')
        Traceback (most recent call last):
            ...
        NodeExistsError: /doctest

        :type key: str
        :type value: str|None
        :type ttl: int|None
        :type sequential: bool
        :type directory: bool
        :rtype: EtcdEvent
        """
        params: dict[str, Any] = {}
        if ttl is not None:
            params['ttl'] = ttl
            if value is not None:
                params['value'] = value
        else:
            params['value'] = value

        if sequential:
            method = 'POST'
        else:
            method = 'PUT'
            params['prevExist'] = False

            if directory:
                params['dir'] = True

        return self.send_request(method, key, params)

    def get(self, key: str, consistent=False, quorum=False, recursive=False, sorted=False, prev_index=0):
        """Get the value of a node.

        >>> client = EtcdClient()
        >>> event = client.get('/')
        >>> event
        <EtcdEvent: get@/>
        >>> client.set('/doctest/watch')
        <EtcdEvent: create@/doctest/watch>

        :type key: str
        :type consistent: bool
        :type quorum: bool
        :type recursive: bool
        :type sorted: bool
        :type prev_index: int
        :rtype: EtcdEvent
        """
        params = {}
        if consistent:
            params['consistent'] = True
        if quorum:
            params['quorum'] = True
        if recursive:
            params['recursive'] = True
        if sorted:
            params['sorted'] = True
        return self.send_request('GET', key, params)

    def set(
        self,
        key: str,
        value='',
        ttl: Optional[int] = None,
        prev_exist: Optional[str] = None,
        prev_value: Optional[str] = None,
        prev_index: Optional[int] = None,
        directory=False,
    ):
        """Set the value of a node.

        >>> client = EtcdClient()
        >>> client.set('/doctest/set', 'doctest')
        <EtcdEvent: create@/doctest/set>
        >>> event = client.get('/doctest/set')
        >>> event.node.value
        'doctest'

        :type key: str
        :type value: str
        :type ttl: int
        :type prev_exist: bool
        :type prev_value: str
        :type prev_index: int
        :type directory: bool
        :rtype: EtcdEvent
        """
        params: dict[str, Any] = {}
        if prev_exist is not None:
            params['prevExist'] = prev_exist
        if ttl:
            params['ttl'] = ttl
            if value is not None:
                params['value'] = value
        else:
            params['value'] = value
        if prev_value is not None:
            params['prevValue'] = prev_value
        if prev_exist is not None:
            params['prevExist'] = prev_exist
        if prev_index is not None:
            params['prevIndex'] = prev_index
        if directory:
            params['directory'] = True
        return self.send_request('PUT', key, params)

    def refresh(self, key: str, ttl: int):
        """
        Refreshes a value in etcd, without notifying any watchers.

        >>> client = EtcdClient()
        >>> client.set('/doctest/refresh', 'doctest')
        <EtcdEvent: set@/doctest/refresh>
        >>> client.refresh('/doctest/refresh', ttl=5)
        <EtcdEvent: update@/doctest/refresh>

        :type key: str
        :type ttl: int
        """
        params = {'prevExist': True, 'refresh': True, 'ttl': ttl}
        return self.send_request('PUT', key, params)

    def delete(
        self,
        key: str,
        prev_value: Optional[str] = None,
        prev_index: Optional[int] = None,
        directory=True,
        recursive=True,
    ):
        """Delete a node.

        >>> client = EtcdClient()
        >>> client.set('/doctest/delete', 'doctest')
        <EtcdEvent: create@/doctest/delete>
        >>> event = client.delete('/doctest/delete')
        >>> event.action == ACTION_DELETE
        True

        :type key: str
        :type prev_value: str
        :type prev_index: int
        :type directory: bool
        :type recursive: bool
        :rtype: EtcdEvent
        """
        params: dict[str, Any] = {}
        if prev_value is not None:
            params['prevValue'] = prev_value
        if prev_index is not None:
            params['prevIndex'] = prev_index
        if directory:
            params['directory'] = True
        if recursive:
            params['recursive'] = True
        return self.send_request('DELETE', key, params)

    def get_watcher(self, key: str, recursive=False, _use_mux=True):
        """
        Returns a new EtcdWatcher for a given key.

        :type key: str
        :type recursive: bool
        :type _use_mux: bool
        :rtype: BaseWatcher
        """
        if self.watcherd_addr:
            if self._watcherd_client is None:
                channel = grpc.insecure_channel(
                    self.watcherd_addr,
                    options=[
                        ('grpc.keepalive_time_ms', 10000),
                        ('grpc.keepalive_timeout_ms', 5000),
                        ('grpc.keepalive_permit_without_calls', True),
                        ('grpc.http2.max_pings_without_data', 0),
                        ('grpc.http2.min_time_between_pings_ms', 10000),
                        ('grpc.http2.min_ping_interval_without_data_ms', 5000),
                    ],
                )
                self._watcherd_client = EtcdWatcherdServiceStub(channel)

            return EtcdWatcherdWatcher(key=key, recursive=recursive, watcherd_client=self._watcherd_client)

        return EtcdWatcher(key=key, recursive=recursive, etcd_client=self, _use_mux=_use_mux)

    def _send_request(self, method: str, key: str, params: dict[str, Any], jitter_timeout: bool, url: str):
        """Send request to Etcd."""
        try:
            if method in ['GET', 'DELETE']:
                if params:
                    url = '%s?%s' % (url, six.moves.urllib.parse.urlencode(params))
                data = None
            else:
                data = six.moves.urllib.parse.urlencode(params).encode()
            opener = six.moves.urllib.request.build_opener(HTTPHandler)
            request = six.moves.urllib.request.Request(url, data)
            if data:
                request.add_header('Content-Type', 'application/x-www-form-urlencoded')
            if self.auth_token:
                request.add_header('Authorization', 'Basic ' + self.auth_token)
            request.get_method = lambda: method  # type: ignore
            timeout = self.http_timeout

            # If we are jittering the timeout (because we are in watch mode), allow
            # the timeout to be in the range of (self.http_timeout, self.http_timeout * 2)
            if jitter_timeout:
                timeout = random.uniform(self.http_timeout, self.http_timeout * 2)

            # We add 1 second rather arbitrarily to the gevent timeout, to make sure that it's not
            # racing with the request timeout. The outer timeout is more of a "guard" incase the
            # inner timeout isn't working.
            with gevent.Timeout(seconds=timeout + 1, exception=Timeout):
                r = opener.open(request, timeout=timeout)
                assert r is not None
                return parse_event(r.info(), r.read())
        except socket.timeout:
            raise Timeout()
        except six.moves.urllib.error.HTTPError as e:
            return parse_event(e.info(), e.read())
        except six.moves.urllib.error.URLError as e:
            if e.errno == errno.ETIMEDOUT:
                raise Timeout()
            raise RequestError(str(e))
        except six.moves.http_client.HTTPException as e:
            raise RequestError(str(e))

    def send_request(self, method: str, key: str, params: dict[str, Any], jitter_timeout=False, try_all_peers=False):
        """
        Send a request to etcd

        If `try_all_peers` is set, failing requests will be attempted against all peers before an exception is raised.
        """
        peers = self.peers if try_all_peers else [random.choice(self.peers)]
        urls = [_build_url(p, key) for p in peers]
        random.shuffle(urls)
        last_exception = ValueError('No peers specified in client')  # type: Exception
        for url in urls:
            try:
                return self._send_request(method, key, params, jitter_timeout, url)
            except EtcdError as exception:
                last_exception = exception
        raise last_exception


class HTTPConnection(six.moves.http_client.HTTPConnection):
    def _send_output(self, message_body=None, encode_chunked=False):
        """Sends the headers + body in one chunk, only supports bytes message bodies."""
        self._buffer.extend((b'', b''))
        assert not encode_chunked, 'Does not support chunked encoding'

        if message_body is not None:
            assert isinstance(message_body, bytes), 'Message body must be bytes.'
        else:
            message_body = b''

        msg = b'\r\n'.join(self._buffer) + message_body
        del self._buffer[:]
        self.send(msg)


class HTTPHandler(six.moves.urllib.request.HTTPHandler):
    def http_open(self, req):
        return self.do_open(HTTPConnection, req)


def walk_etcd_tree(
    start_path: str,
    matcher_fn: Optional[Callable[[EtcdNode], bool]] = None,
    action_fn: Optional[Callable[[EtcdNode], Any]] = None,
    max_depth: Optional[int] = None,
    _current_depth=0,
):
    """
    Walk an etcd tree recursively, optionally performing actions on matching nodes.

    Args:
        start_path: The path to start walking from
        matcher_fn: Optional function that takes a node and returns True if it matches criteria
        action_fn: Optional function to call on matching nodes
        max_depth: Optional maximum depth to traverse
        _current_depth: Internal parameter for tracking recursion depth

    Returns:
        List of results from action_fn for all matching nodes
    """
    etcd_client = EtcdClient()
    results: list[Any] = []

    if max_depth is not None and _current_depth > max_depth:
        return results

    try:
        current_node = etcd_client.get(start_path).node
    except Exception as e:
        print('Error accessing path {0}: {1}'.format(start_path, e))
        return results

    # Check if this node matches our criteria
    if matcher_fn is None or matcher_fn(current_node):
        if action_fn:
            try:
                result = action_fn(current_node)
                results.append(result)
            except Exception as e:
                print('Error performing action on {0}: {1}'.format(current_node.key, e))

    # Recurse into children
    try:
        for child in getattr(current_node, 'nodes', []):
            child_results = walk_etcd_tree(child.key, matcher_fn, action_fn, max_depth, _current_depth + 1)
            results.extend(child_results)
    except Exception as e:
        print('Error accessing children of {0}: {1}'.format(start_path, e))

    return results
