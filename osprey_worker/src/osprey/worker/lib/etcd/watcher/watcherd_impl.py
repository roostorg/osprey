import logging
import time

from osprey.rpc.etcd_watcherd.v1.etcd_watcherd_pb2 import WatchKeyRecursiveRequest, WatchKeyRequest
from osprey.worker.lib.backoff import Backoff
from osprey.worker.lib.ddtrace_utils import with_tracing_context

from ._base import BaseWatcher
from ._events import FullSyncOne, FullSyncOneNoKey, FullSyncRecursive, IncrementalSyncDelete, IncrementalSyncUpsert
from ._mux import PassthruMux, RecursiveWatchMux, WatchMux

log = logging.getLogger('etcd.watcher')


class EtcdWatcherState(object):
    """
    Represents the state of the etcd watcher.
    """

    # The watcher is stopped.
    STOPPED = 'stopped'

    # The watcher is consuming from a grpc unary stream.
    STREAMING = 'streaming'

    # The watcher needs to reset the stream since it failed.
    RESET = 'reset'


class EtcdWatcherdWatcher(BaseWatcher):
    """
    Implementation of the base watcher interface that talks to etcd_watcherd.
    """

    # HACK![willhug] Manually clears the trace context after creating the GRPC stream request.
    # After the stream is created here, in most cases it will be put into a gevent greenlet.  This
    # leaves the stream tracing span on the current greenlet's context.  This means all new spans
    # will have the stream's span as a parent. And the stream tracing span won't get closed until
    # the stream itself closes.  Clearing the trace context here allows all other spans to be created
    # properly.
    @with_tracing_context(ctx=None)
    def _get_initial_event_and_stream(self):
        if self._recursive:
            request = WatchKeyRecursiveRequest()
            request.key = self._key
            response_stream = self._watcherd_client.WatchKeyRecursive(request)
            raw_initial_event = next(response_stream)
        else:
            request = WatchKeyRequest()
            request.key = self._key
            response_stream = self._watcherd_client.WatchKey(request)
            raw_initial_event = next(response_stream)

        initial_event = self._translate_event(raw_initial_event)
        if self._recursive and not isinstance(initial_event, FullSyncRecursive):
            raise Exception(
                'invariant: begin watching returned initial event of wrong type for recursive watch of {}: {!r}'.format(
                    self._key, initial_event
                )
            )

        return initial_event, response_stream

    def __init__(self, key, recursive=False, watcherd_client=None, _use_mux=True):
        # watcherd requires the key to end with `/` for recursive watcher.
        if recursive and not key.endswith('/'):
            key += '/'

        self._key = key
        self._stream = None
        self._recursive = recursive
        self._watcherd_client = watcherd_client
        self._state = EtcdWatcherState.STOPPED
        self._backoff = Backoff(min_delay=1, max_delay=5)
        if _use_mux:
            self._mux = RecursiveWatchMux() if recursive else WatchMux()
        else:
            self._mux = PassthruMux()

    def begin_watching(self):
        if self._state != EtcdWatcherState.STOPPED:
            raise Exception('Watcher is already running.')

        initial_event, self._stream = self._get_initial_event_and_stream()
        self._mux.reset_with_initial_event(initial_event)
        return initial_event

    def continue_watching(self):
        if self._state != EtcdWatcherState.STOPPED:
            raise Exception('Watcher is already running.')

        if self._stream:
            self._set_state(EtcdWatcherState.STREAMING)
        else:
            self._set_state(EtcdWatcherState.RESET)

        try:
            # We are now beginning the watch cycle - and will be in 1 of 3 possible states:
            # - STOPPED: Stop watching.
            # - WAIT: Wait for updates from etcd going past `self._index`.
            # - SYNC: We need to re-sync fully with etcd.
            while self._state != EtcdWatcherState.STOPPED:
                if self._state == EtcdWatcherState.STREAMING:
                    for event in self._consume_stream():
                        yield event

                elif self._state == EtcdWatcherState.RESET:
                    for event in self._reset_stream():
                        yield event
        finally:
            # Something has interrupted this iterator, we can move to the stopped state now.
            self._set_state(EtcdWatcherState.STOPPED)
            if self._stream:
                self._stream.cancel()
                self._stream = None
            if not isinstance(self._mux, PassthruMux):
                self._mux = RecursiveWatchMux() if self._recursive else WatchMux()

    def __repr__(self):
        return 'EtcdWatcherdWatcher(key=%r, recursive=%r, state=%r, stream=%r)' % (
            self._key,
            self._recursive,
            self._state,
            self._stream,
        )

    def _set_state(self, state):
        log.debug('%r transitioning to state %r', self, state)
        self._state = state

    def _consume_stream(self):
        try:
            for raw_event in self._stream:
                event = self._translate_event(raw_event)
                for event in self._mux.dedupe_event(event):
                    yield event

        except MemoryError as e:
            raise e

        except Exception:
            if self._stream:
                self._stream.cancel()
                self._stream = None
            self._set_state(EtcdWatcherState.RESET)
            delay = self._backoff.fail()
            log.exception(
                '%r: etcd-watcherd stream raised an error. sleeping for %.2f sec before retrying', self, delay
            )
            time.sleep(delay)

    def _reset_stream(self):
        # noinspection PyBroadException
        try:
            initial_event, self._stream = self._get_initial_event_and_stream()
            self._backoff.succeed()
            self._set_state(EtcdWatcherState.STREAMING)
            for event in self._mux.dedupe_event(initial_event):
                yield event

        except MemoryError as e:
            raise e

        except Exception:
            delay = self._backoff.fail()
            log.exception('%r: etcd raised an error. sleeping for %.2f sec before retrying', self, delay)
            time.sleep(delay)

    def _translate_event(self, event):
        if self._recursive:
            if event.HasField('full_sync'):
                return FullSyncRecursive(
                    items=[FullSyncOne(key=key, value=value) for (key, value) in event.full_sync.items.items()]
                )
            elif event.HasField('sync_one'):
                sync_one = event.sync_one
                return IncrementalSyncUpsert(key=sync_one.key, value=sync_one.value)
            elif event.HasField('delete_one'):
                delete_one = event.delete_one
                return IncrementalSyncDelete(key=delete_one.key, prev_value=delete_one.prev_value)
        else:
            if event.HasField('value'):
                return FullSyncOne(key=self._key, value=event.value.value)
            else:
                return FullSyncOneNoKey(key=self._key)
