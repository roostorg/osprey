class BaseWatcher(object):
    """The base watcher interface that all other watchers should implement."""

    def begin_watching(self):
        """
        Call this to begin watching a key. Returning the initial sync event. This is useful if you want to perform
        the initial sync of the key with the data structure in a blocking fashion, before passing the watcher
        off into a greenlet to continue keeping the data structure up to date. It is not mandatory to call this
        function before calling `continue_watching`. If not called, `continue_watching` will just get the first
        initial sync event.

        Example:
        ```
        def handle_event(event):
            if isinstance(event, FullSyncOne):
                print('value set to: %r' % event.value)
            elif isinstance(event, FullSyncOneNoKey):
                print('key was deleted :(')

        def continue_watching(watcher):
            for event in watcher.continue_watching():
                handle_event(event)

        # Sets up the watcher for the key `foo`.
        watcher = client.get_watcher('foo')
        # Handles the initial create event in a blocking manner.
        handle_event(watcher.begin_watching())
        # Spawns the greenlet to continue watching the key and handling updates.
        gevent.spawn(continue_watching, watcher)
        ```


        If this is a recursive watcher, the return type is always FullSyncRecursive(), with an empty
        items array if the key does not exist.

        If it is a non-recursive watcher, the return type is either FullSyncOne() or FullSyncOneNoKey() depending
        on whether or not the key exists.

        :rtype: FullSyncRecursive|FullSyncOne|FullSyncOneNoKey
        """
        raise NotImplementedError

    def continue_watching(self):
        """
        Returns an iterator, that when iterated over, continues the watch of a given key. This function does not spawn
        a greenlet, and if the iterator is interrupted, the watch ends. This means that it is safe to run this in a
        greenlet, and simply kill the greenlet in order to kill the watcher. This watcher will retry its watches
        infinitely, until you stop the iterator, attempting to recover from all etcd errors it can, including
        etcd_index_cleared, by restarting and performing a full sync.

        If this is a recursive watcher, the following events will be yielded:
            - FullSyncRecursive(): When the key is either deleted, or a full sync happens due to etcd_index_cleared
              error. If you are watching the key recursively, you should throw away all existing data you have, and
              fully synchronize to this state. If the key is deleted, a FullSyncRecursive() with an empty items
              array will be yielded.
            - IncrementalSyncUpsert(): A sub-key is created or updated.
            - IncrementalSyncDelete(): A sub-key is deleted.

        If this is a non-recursive watcher, the following events will be yielded:
            - FullSyncOne(): The key was updated with a value.
            - FullSyncOneNoKey(): The key was deleted, or didn't exist when the watch tried to start.

        Example:
        ```
        watcher = client.get_watcher('hello', recursive=False)
        for event in watcher.continue_watching():
            if isinstance(event, FullSyncOne):
                print('value set to: %r' % event.value)
            elif isinstance(event, FullSyncOneNoKey):
                print('key was deleted :(')
        ```
        """
        raise NotImplementedError
