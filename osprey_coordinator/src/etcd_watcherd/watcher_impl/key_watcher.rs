use std::{fmt::Debug, sync::Arc};

use super::{
    util::{err_is_watch_timeout, err_should_reset_watcher, jittered_watch_timeout},
    WatchEvents, WatchResult, WatchState, WatcherImpl,
};

/// These events are emitted by [`crate::WatcherEvents`] when using a single key watcher.
#[derive(Clone, Debug)]
pub enum KeyWatchEvents {
    /// The key has been updated.
    /// - Some(value): Means that the key has been set to to the given value.
    /// - None: The key has been deleted.
    FullSync { value: Option<Arc<str>> },
}

impl super::sealed::Sealed for KeyWatchEvents {}

impl WatchEvents for KeyWatchEvents {
    type Value = Option<Arc<str>>;
    const WATCHER_MAX_BACKLOG_SIZE: usize = 16;

    fn full_sync_event(value: Self::Value) -> Self {
        Self::FullSync { value }
    }
}

pub(crate) struct KeyWatcher {
    client: crate::etcd::Client,
    key: String,
}

impl Debug for KeyWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyWatcher")
            .field("key", &self.key)
            .finish()
    }
}

impl KeyWatcher {
    pub(crate) fn new(client: crate::etcd::Client, key: String) -> Self {
        Self { client, key }
    }

    fn compute_delta_event(
        current_value: &Option<Arc<str>>,
        new_value: Option<Arc<str>>,
    ) -> (Vec<KeyWatchEvents>, Option<Arc<str>>) {
        // Only emit an event if the value actually changed. Otherwise, re-use the old value's arc.
        if new_value != *current_value {
            (
                vec![KeyWatchEvents::full_sync_event(new_value.clone())],
                new_value,
            )
        } else {
            (vec![], current_value.clone())
        }
    }
}

#[async_trait::async_trait]
impl WatcherImpl for KeyWatcher {
    type Events = KeyWatchEvents;

    fn key(&self) -> &str {
        &self.key
    }

    async fn begin_watching(
        &self,
    ) -> Result<WatchState<<Self::Events as WatchEvents>::Value>, crate::etcd::EtcdError> {
        let result = self.client.get(self.key()).await;
        let response = match result {
            // Handle key-not-found by setting the initial value to None.
            Err(crate::etcd::EtcdError::KeyNotFound { index, .. }) => {
                return Ok(WatchState {
                    current_value: None,
                    wait_index: index,
                })
            }
            Err(etcd_error) => return Err(etcd_error),
            Ok(ok) => ok,
        };

        Ok(WatchState {
            current_value: response.data.node.value.map(|s| s.into()),
            wait_index: response.cluster_info.etcd_index.unwrap_or_default(),
        })
    }

    async fn continue_watching(
        &self,
        watch_state: &WatchState<<Self::Events as WatchEvents>::Value>,
    ) -> Result<WatchResult<Self::Events>, crate::etcd::EtcdError> {
        let result = self
            .client
            .watch_opt(
                self.key(),
                watch_state.wait_index,
                false,
                jittered_watch_timeout(),
            )
            .await;

        let (new_value, wait_index) = match result {
            // If the error is a event index cleared code, we need to reset the watcher.
            Err(ref err) if err_should_reset_watcher(err) => return Ok(WatchResult::ResetWatcher),
            // The error is a timeout, so we should propegate that upstream.
            Err(ref err) if err_is_watch_timeout(err) => return Ok(WatchResult::Timeout),
            // A generic etcd error can be propagated to the caller.
            Err(etcd_error) => return Err(etcd_error),
            // Otherwise, we have got a value for the key.
            Ok(ok) => (
                ok.data.node.value,
                ok.data.node.modified_index.unwrap_or_default(),
            ),
        };

        let (events, current_value) =
            Self::compute_delta_event(&watch_state.current_value, new_value.map(|s| s.into()));

        Ok(WatchResult::Success {
            events,
            state: WatchState {
                current_value,
                wait_index,
            },
        })
    }

    async fn reset_watcher(
        &self,
        current_value: &<Self::Events as WatchEvents>::Value,
    ) -> Result<WatchResult<Self::Events>, crate::etcd::EtcdError> {
        let WatchState {
            wait_index,
            current_value: next_value,
        } = self.begin_watching().await?;
        let (events, current_value) = Self::compute_delta_event(current_value, next_value);

        Ok(WatchResult::Success {
            events,
            state: WatchState {
                current_value,
                wait_index,
            },
        })
    }
}
