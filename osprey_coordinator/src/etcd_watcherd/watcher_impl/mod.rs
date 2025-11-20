pub(crate) mod key_watcher;
pub(crate) mod recursive_key_watcher;
pub(self) mod util;

use std::fmt::Debug;

pub(crate) mod sealed {
    // sealed trait pattern, no one can impl `WatchEvents` outside of this crate.
    pub trait Sealed {}
}

pub trait WatchEvents: sealed::Sealed + Clone + Send + Sync + 'static {
    type Value: Sync + Send + Clone + 'static;
    const WATCHER_MAX_BACKLOG_SIZE: usize;

    fn full_sync_event(value: Self::Value) -> Self;
}

/// This trait basically powers the finite state machine of watching an etcd key. It is executed by [`crate::WatcherLoop`].
///
/// Implementators are the key watcher and recursive key watcher.
#[async_trait::async_trait]
pub(crate) trait WatcherImpl: Debug + Send + Sync + 'static {
    type Events: WatchEvents;

    fn key(&self) -> &str;

    /// Starts the watch phase, gets the current watched value, and the etcd index, see [`WatchState`].
    async fn begin_watching(
        &self,
    ) -> Result<WatchState<<Self::Events as WatchEvents>::Value>, crate::etcd::EtcdError>;

    /// When etcd returns an error that the watcher has fallen too far behind, we need to reset the watcher. This method
    /// is invoked when that is called. See [`WatchResult`] for more info.
    async fn reset_watcher(
        &self,
        current_value: &<Self::Events as WatchEvents>::Value,
    ) -> Result<WatchResult<Self::Events>, crate::etcd::EtcdError>;

    /// This is invoked with the [`WatchState`] and is called when we should continue to long-poll etcd for changes to the key.
    async fn continue_watching(
        &self,
        watch_state: &WatchState<<Self::Events as WatchEvents>::Value>,
    ) -> Result<WatchResult<Self::Events>, crate::etcd::EtcdError>;
}

/// Holds the current value of the watcher, and the etcd index that we should continue to call the watch call on.
pub(crate) struct WatchState<V> {
    pub current_value: V,
    pub wait_index: u64,
}

/// Returned by the `continue_watching` and `reset_watcher` methods on [`WatcherImpl`].
pub(crate) enum WatchResult<E>
where
    E: WatchEvents,
{
    /// The watch was successful, and we should call [`WatcherImpl::continue_watching`] next.
    Success {
        events: Vec<E>,
        state: WatchState<E::Value>,
    },
    /// The watch had timed out, and we should call [`WatcherImpl::continue_watching`] next, not advancing the state.
    Timeout,
    /// The watch needs to be reset, and we should call [`WatcherImpl::reset_watcher`] next.
    ResetWatcher,
}
