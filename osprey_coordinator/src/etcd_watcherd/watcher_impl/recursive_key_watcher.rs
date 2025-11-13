use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use crate::etcd::Action;
use tracing::warn;

use super::{
    util::{err_is_watch_timeout, err_should_reset_watcher, jittered_watch_timeout},
    WatchEvents, WatchResult, WatchState, WatcherImpl,
};

/// These events are emitted by [`crate::WatcherEvents`] when using a recursive watcher.
#[derive(Clone, Debug)]
pub enum RecursiveKeyWatchEvents {
    /// A FullSync event is emitted as the initial watch event, but also, can be emitted after the initial watch event.
    ///
    /// This event represents a "snapshot" of the data within the etcd key watcher. Sometimes, the key watcher
    /// might reset internally and decide to send a full sync again, so you should be prepared to handle this event multiple times.
    ///
    /// Additionally,
    ///  - this event can be emitted with an empty `items` hashmap, if the directory that is being watched is deleted.
    ///  - this event can be emitted if your watcher falls too far behind in handling
    ///    [`RecursiveKeyWatchEvents::SyncOne`] or [`RecursiveKeyWatchEvents::DeleteOne`] events,
    ///    to catch up the watcher.
    FullSync {
        items: Arc<HashMap<Arc<str>, Arc<str>>>,
    },
    /// Emitted when a single key is either created or updated.
    SyncOne { key: Arc<str>, value: Arc<str> },
    /// Emitted when a single key is deleted or expires.
    DeleteOne { key: Arc<str>, prev_value: Arc<str> },
}

impl super::sealed::Sealed for RecursiveKeyWatchEvents {}

impl WatchEvents for RecursiveKeyWatchEvents {
    type Value = Arc<HashMap<Arc<str>, Arc<str>>>;
    const WATCHER_MAX_BACKLOG_SIZE: usize = 64;

    fn full_sync_event(value: Self::Value) -> Self {
        Self::FullSync { items: value }
    }
}

pub(crate) struct RecursiveKeyWatcher {
    client: crate::etcd::Client,
    key: String,
}

impl Debug for RecursiveKeyWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecursiveKeyWatcher")
            .field("key", &self.key)
            .finish()
    }
}

impl RecursiveKeyWatcher {
    pub(crate) fn new(client: crate::etcd::Client, key: String) -> Self {
        Self { client, key }
    }
}

type Value = <RecursiveKeyWatchEvents as WatchEvents>::Value;

#[async_trait::async_trait]
impl WatcherImpl for RecursiveKeyWatcher {
    type Events = RecursiveKeyWatchEvents;

    fn key(&self) -> &str {
        &self.key
    }

    async fn begin_watching(&self) -> Result<WatchState<Value>, crate::etcd::EtcdError> {
        let result = self.client.get_recursive(self.key()).await;
        let response = match result {
            // Key does not exist, so we'll start out with an empty directory.
            Err(crate::etcd::EtcdError::KeyNotFound { index, .. }) => {
                return Ok(WatchState {
                    current_value: Default::default(),
                    wait_index: index,
                })
            }
            Err(etcd_error) => return Err(etcd_error),
            Ok(ok) => ok,
        };

        let wait_index = response.cluster_info.etcd_index.unwrap_or_default();
        let current_value = Self::collect_node_children(response.data.node);

        Ok(WatchState {
            wait_index,
            current_value,
        })
    }

    async fn reset_watcher(
        &self,
        current_value: &Value,
    ) -> Result<WatchResult<Self::Events>, crate::etcd::EtcdError> {
        let watch_state = self.begin_watching().await?;
        let wait_index = watch_state.wait_index;

        // The directory is empty. Figure out whether or not we should dispatch events.
        if watch_state.current_value.is_empty() {
            // If the current value is also empty, we can emit no events, as nothing has changed (empty is empty),
            // additionally, we can continue to use the old `current_value`.
            let (current_value, events) = if current_value.is_empty() {
                (current_value.clone(), vec![])
            } else {
                // Otherwise, current value was not empty. We'll emit a `FullSync` to notify all consumers to clear their copy of the data.
                (
                    watch_state.current_value.clone(),
                    vec![RecursiveKeyWatchEvents::FullSync {
                        items: watch_state.current_value,
                    }],
                )
            };

            return Ok(WatchResult::Success {
                events,
                state: WatchState {
                    current_value,
                    wait_index,
                },
            });
        }

        let initial_value = Arc::try_unwrap(watch_state.current_value)
            .expect("invariant: arc has more than 1 reference");

        // We have data, and will do a diff.
        let (events, current_value) = Self::compute_reset_events(current_value, initial_value);

        Ok(WatchResult::Success {
            events,
            state: WatchState {
                wait_index,
                current_value,
            },
        })
    }

    async fn continue_watching(
        &self,
        watch_state: &WatchState<Value>,
    ) -> Result<WatchResult<Self::Events>, crate::etcd::EtcdError> {
        let result = self
            .client
            .watch_opt(
                self.key(),
                watch_state.wait_index,
                true,
                jittered_watch_timeout(),
            )
            .await;

        let data = match result {
            // If the error is a event index cleared code, we need to reset the watcher.
            Err(ref err) if err_should_reset_watcher(err) => return Ok(WatchResult::ResetWatcher),
            // The error is a timeout, so we should propegate that upstream.
            Err(ref err) if err_is_watch_timeout(err) => return Ok(WatchResult::Timeout),
            // A generic etcd error can be propagated to the caller.
            Err(etcd_error) => return Err(etcd_error),
            // Otherwise, we have got a value for the key.
            Ok(ok) => ok.data,
        };
        let wait_index = data.node.modified_index.unwrap_or_default();

        match data.action {
            // Key has been deleted.
            Action::CompareAndDelete | Action::Delete | Action::Expire => {
                let dir = data.node.dir.unwrap_or_default();
                let key = data.node.key.unwrap_or_default();
                let (events, current_value) = if dir {
                    self.compute_dir_delete_events(key, watch_state)
                } else {
                    self.compute_single_key_delete_events(key, watch_state)
                };

                Ok(WatchResult::Success {
                    events,
                    state: WatchState {
                        current_value,
                        wait_index,
                    },
                })
            }
            // Key has been set.
            Action::Create | Action::Set | Action::Update | Action::CompareAndSwap => {
                let key = data.node.key.unwrap_or_default();
                let value = data.node.value.unwrap_or_default();
                let (events, current_value) =
                    self.compute_key_update_events(watch_state, key, value);

                Ok(WatchResult::Success {
                    events,
                    state: WatchState {
                        current_value,
                        wait_index,
                    },
                })
            }
            // We really shouldn't get GET actions. If we do, let's just reset the watcher, and start over.
            Action::Get => {
                warn!(
                    "Watch for key {} returned an unexpected action {:?}, will reset watcher.",
                    self.key(),
                    data.action,
                );
                Ok(WatchResult::ResetWatcher)
            }
        }
    }
}

impl RecursiveKeyWatcher {
    /// Compute the events that need to be emitted if a directory is deleted.
    fn compute_dir_delete_events(
        &self,
        key: String,
        watch_state: &WatchState<Value>,
    ) -> (Vec<RecursiveKeyWatchEvents>, Value) {
        // The root key we were watching has been deleted, so we'll emit a full sync.
        if &key == self.key() {
            // Current value was also empty, so we can just re-use it, and emit no events.
            if watch_state.current_value.is_empty() {
                (vec![], watch_state.current_value.clone())
            } else {
                // Otherwise, we'll create an empty map to store as our current value, emit a full sync,
                // to signify the directory is empty.
                let empty = Arc::new(HashMap::new());
                (
                    vec![RecursiveKeyWatchEvents::FullSync {
                        items: empty.clone(),
                    }],
                    empty,
                )
            }
        } else {
            // Otherwise, we've got a sub key we're deleting. We'll go ahead and loop over all keys and remove the ones we care about.
            // NOTE: This could probably be made much more efficient if we backed this with a BTreeMap internally, but for our use-cases,
            // the watched directories are not too big, and sub-directory deletes are quite rare.
            let mut cloned = HashMap::clone(&watch_state.current_value);
            let prefix = format!("{}/", key);
            let mut events = vec![];
            cloned.retain(|k, v| {
                if k.starts_with(&prefix) {
                    events.push(RecursiveKeyWatchEvents::DeleteOne {
                        key: k.clone(),
                        prev_value: v.clone(),
                    });
                    false
                } else {
                    true
                }
            });

            (events, cloned.into())
        }
    }

    /// Compute the events that need to be emitted if a single key is deleted.
    fn compute_single_key_delete_events(
        &self,
        key: String,
        watch_state: &WatchState<Value>,
    ) -> (Vec<RecursiveKeyWatchEvents>, Value) {
        // We're removing an individual key.
        let mut cloned = HashMap::clone(&watch_state.current_value);
        if let Some((key, prev_value)) = cloned.remove_entry(key.as_str()) {
            // Recycle the Arc for that individual key in the `DeleteOne` event.
            (
                vec![RecursiveKeyWatchEvents::DeleteOne { key, prev_value }],
                cloned.into(),
            )
        } else {
            // This is an edge-case, and shold really never happen, but if it does, we'll throw
            // away the clone we just made, and re-use the current value's arc.
            (vec![], watch_state.current_value.clone())
        }
    }

    /// Given the current [`WatchState`], and a key/value pair, compute the events that need to be emitted,
    /// and also, return a new [`Value`] with the mutation applied.
    fn compute_key_update_events(
        &self,
        watch_state: &WatchState<Value>,
        key: String,
        value: String,
    ) -> (Vec<RecursiveKeyWatchEvents>, Value) {
        match watch_state.current_value.get_key_value(key.as_str()) {
            // Value has not changed, even though it was "set" in etcd, we drop events that don't effectively change the value.
            // Additionally, we'll re-use the underlying watch_state's current value, by cloning the Arc, instead of doing a shallow copy.
            Some((_, current_value)) if **current_value == value => {
                (vec![], watch_state.current_value.clone())
            }
            // Value has changed, or doesn't exist yet.
            other => {
                let mut cloned = HashMap::clone(&watch_state.current_value);
                // Try and recycle the current key's arc.
                let key = match other {
                    Some((key, _)) => key.clone(),
                    None => key.into(),
                };
                let value: Arc<str> = value.into();
                cloned.insert(key.clone(), value.clone());
                let events = vec![RecursiveKeyWatchEvents::SyncOne { key, value }];

                (events, cloned.into())
            }
        }
    }

    /// When a reset is received, we'll perform a simple diff of the current value, and the latest value as returned by etcd.
    ///
    /// This function computes the events that need to be emitted.
    fn compute_reset_events(
        current_value: &Value,
        next_value: HashMap<Arc<str>, Arc<str>>,
    ) -> (Vec<RecursiveKeyWatchEvents>, Value) {
        let mut events = vec![];
        let mut maybe_next_value = None;
        let mut unvisited_keys: HashSet<_> = current_value.keys().collect();

        // Compute events for all new/updated keys.
        for (key, value) in next_value.into_iter() {
            unvisited_keys.remove(&key);

            match current_value.get(&key) {
                // The value is the same.
                Some(current_key_value) if current_key_value == &value => continue,
                _ => {
                    // We're about to mutate, so clone the current map, so we can mutate it.
                    let cloned =
                        maybe_next_value.get_or_insert_with(|| HashMap::clone(current_value));

                    events.push(RecursiveKeyWatchEvents::SyncOne {
                        key: key.clone(),
                        value: value.clone(),
                    });
                    cloned.insert(key, value);
                }
            }
        }

        // Compute events for all deleted keys.
        for key in unvisited_keys {
            let cloned = maybe_next_value.get_or_insert_with(|| HashMap::clone(current_value));
            if let Some((key, prev_value)) = cloned.remove_entry(key) {
                events.push(RecursiveKeyWatchEvents::DeleteOne { key, prev_value });
            }
        }

        let next_value = match maybe_next_value {
            Some(cloned) => cloned.into(),
            None => current_value.clone(),
        };
        (events, next_value)
    }

    /// Given an etcd node that was returned by a recursive get operation, descend down the node and its children, and collect
    /// all values into a hash map ([`Value`]).
    fn collect_node_children(node: crate::etcd::Node) -> Value {
        /// Counts how values exist in a given node, recursively traversing down its children and summing those up too.
        fn count_children(num_total_children: usize, node: &crate::etcd::Node) -> usize {
            // If we are a directory, we need to count our children.
            let num_node_children = if node.dir.unwrap_or_default() {
                // Children can be `None` if there are no children, so we handle that case with the `unwrap_or_default()`.
                node.nodes
                    .as_ref()
                    .map(|children| children.iter().fold(0, count_children))
                    .unwrap_or_default()
            } else {
                1
            };

            num_node_children + num_total_children
        }

        /// Recursively traverses down a `node`, populating `target` with the keys & values found.
        fn collect_children(target: &mut HashMap<Arc<str>, Arc<str>>, node: crate::etcd::Node) {
            if let Some(children) = node.nodes {
                for child in children {
                    collect_children(target, child);
                }
            } else if let (Some(key), Some(value)) = (node.key, node.value) {
                target.insert(key.into(), value.into());
            }
        }

        let capacity = count_children(0, &node);
        let mut value = HashMap::with_capacity(capacity);
        collect_children(&mut value, node);
        debug_assert_eq!(value.len(), capacity);

        value.into()
    }
}
