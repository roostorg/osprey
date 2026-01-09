use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::backoff_utils::{AsyncBackoff, Config as AsyncBackoffConfig};
use crate::metrics::counters::StaticCounter;
use parking_lot::RwLock;
use tokio::{sync::mpsc, time::sleep};
use tracing::{error, info};

use crate::etcd_watcherd::{
    WatchEvents, WatchHandle, WatchResult, WatchState, WatcherImpl, WatcherStats,
};

pub(crate) struct WatcherInner<E>
where
    E: WatchEvents,
{
    #[allow(dead_code)]
    key: String,
    current_value: E::Value,
    next_subscription_id: usize,
    watchers: HashMap<WatcherSubscriptionId, mpsc::Sender<E>>,
    stats: Arc<WatcherStats>,
}

pub(crate) struct WatcherSubscription<E>
where
    E: WatchEvents,
{
    pub subscription_id: WatcherSubscriptionId,
    pub receiver: mpsc::Receiver<E>,
    pub full_sync_event: E,
}

/// A subscription id is created by [`WatcherInner::create_subscription`] and is used to identify
/// a given subscription within the system.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub(crate) struct WatcherSubscriptionId(usize);

impl<E> WatcherInner<E>
where
    E: WatchEvents,
{
    /// Get a reference to the watcher inner's current value.
    pub(crate) fn current_value(&self) -> &E::Value {
        &self.current_value
    }

    /// Creates a watcher subscription, which holds a snapshot of the current value, and a receiver which
    /// will receive a stream of events as the watched key is updated.
    pub(crate) fn create_subscription(&mut self) -> WatcherSubscription<E> {
        self.stats.subscriptions_created.incr();
        self.create_subscription_impl()
    }

    fn create_subscription_impl(&mut self) -> WatcherSubscription<E> {
        let subscription_id = WatcherSubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;
        let (sender, receiver) = mpsc::channel(E::WATCHER_MAX_BACKLOG_SIZE);
        self.watchers.insert(subscription_id, sender);
        WatcherSubscription {
            subscription_id,
            receiver,
            full_sync_event: E::full_sync_event(self.current_value.clone()),
        }
    }

    fn update_subscribers(&mut self, events: Vec<E>, current_value: &<E as WatchEvents>::Value) {
        self.current_value = current_value.clone();

        // No watchers or events, so no further dispatching to do.
        if self.watchers.is_empty() || events.is_empty() {
            return;
        }

        // We have events to dispatch to their respective watchers.
        for event in events {
            let prev_watchers_len = self.watchers.len();

            // Try to write to all senders, retaining only the ones that have not overflowed. The ones that have overflowed will need to be
            // restarted, as they missed events. This will trigger a full-sync. This is to make sure if a receiver goes pathologically
            // behind, we'll terminate it, and it must restart watching.
            self.watchers
                .retain(|_, sender| sender.try_send(event.clone()).is_ok());

            let watchers_len = self.watchers.len();
            let watchers_removed = prev_watchers_len - watchers_len;
            self.stats
                .subscriptions_reset
                .incr_by(watchers_removed as _);
            self.stats.subscriptions_notified.incr_by(watchers_len as _);
        }
    }

    pub(crate) fn remove_subscription(&mut self, watcher_id: WatcherSubscriptionId) {
        if self.watchers.remove(&watcher_id).is_some() {
            self.stats.subscriptions_removed.incr();
        }
    }

    pub(crate) fn recreate_subscription(
        &mut self,
        watcher_id: WatcherSubscriptionId,
    ) -> WatcherSubscription<E> {
        self.stats.subscriptions_recreated.incr();
        self.watchers.remove(&watcher_id);
        self.create_subscription_impl()
    }

    pub(crate) fn num_subscribers(&self) -> usize {
        self.watchers.len()
    }
}

pub(crate) struct WatcherLoop<I>
where
    I: WatcherImpl,
{
    inner: Arc<RwLock<WatcherInner<I::Events>>>,
    watcher_impl: I,
    backoff: AsyncBackoff,
    stats: Arc<WatcherStats>,
}

impl<I> WatcherLoop<I>
where
    I: WatcherImpl,
{
    /// Starts the watcher loop, resolving once the loop has started, and an initial value has been read from etcd.
    pub(crate) async fn start(
        watcher_impl: I,
        stats: Arc<WatcherStats>,
    ) -> Result<WatchHandle<I::Events>, crate::etcd::EtcdError> {
        // Begin watching by synchronizing with etcd.
        let initial_watch_state = match watcher_impl.begin_watching().await {
            Ok(initial_watch_state) => initial_watch_state,
            Err(err) => {
                error!(
                    "watcher {:?} failed to start (error: {:?})",
                    watcher_impl, err
                );
                stats.watcher_start_fails.incr();
                return Err(err);
            }
        };

        let key = watcher_impl.key().to_owned();
        let backoff = AsyncBackoff::new(AsyncBackoffConfig {
            min_delay: Duration::from_secs(5),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
        });

        let watcher = Self {
            watcher_impl,
            backoff,
            stats: stats.clone(),
            inner: Arc::new(RwLock::new(WatcherInner {
                current_value: initial_watch_state.current_value.clone(),
                next_subscription_id: 0,
                watchers: HashMap::new(),
                stats,
                key,
            })),
        };

        let inner = watcher.inner.clone();
        let join_handle = tokio::task::spawn(watcher.run_loop(initial_watch_state));
        let watch_handle = WatchHandle::new(inner, join_handle);

        Ok(watch_handle)
    }

    /// The background task that runs the watcher loop fsm.
    async fn run_loop(
        mut self,
        initial_watch_state: WatchState<<I::Events as WatchEvents>::Value>,
    ) {
        let mut watch_loop_state = WatchLoopState::ContinueWatching(initial_watch_state);
        self.stats.watcher_start_success.incr();
        info!("Watcher (impl={:?} has started)", &self.watcher_impl);

        loop {
            let (next_watch_loop_state, events) = self.run_loop_once(watch_loop_state).await;
            watch_loop_state = next_watch_loop_state;
            self.inner
                .write()
                .update_subscribers(events, watch_loop_state.current_value());
        }
    }

    /// Runs one iteration of the watcher loop, returning the events to be emitted, and the next watch loop state that this function will be called with.
    async fn run_loop_once(
        &mut self,
        watch_loop_state: WatchLoopState<<I::Events as WatchEvents>::Value>,
    ) -> (
        WatchLoopState<<I::Events as WatchEvents>::Value>,
        Vec<I::Events>,
    ) {
        let (watch_result, prev_watch_state) = match watch_loop_state {
            WatchLoopState::ContinueWatching(prev_watch_state) => {
                let watch_result = self.watcher_impl.continue_watching(&prev_watch_state).await;

                match watch_result {
                    // The watch was a success!
                    Ok(result) => (result, prev_watch_state),
                    // The watch failed, we'll backoff and try again.
                    Err(err) => {
                        self.log_and_sleep_after_error(err).await;
                        return (WatchLoopState::ContinueWatching(prev_watch_state), vec![]);
                    }
                }
            }
            WatchLoopState::ResetWatcher(prev_watch_state) => {
                match self
                    .watcher_impl
                    .reset_watcher(&prev_watch_state.current_value)
                    .await
                {
                    // The watch reset was successful, which means we can continue the watcher now.
                    Ok(watch_result) => (watch_result, prev_watch_state),
                    // Watch reset was unsuccessful, we'll need to retry resetting.
                    Err(err) => {
                        self.log_and_sleep_after_error(err).await;
                        return (WatchLoopState::ResetWatcher(prev_watch_state), vec![]);
                    }
                }
            }
        };

        self.backoff.succeed();
        match watch_result {
            // The watch was successful, and state will be updated to the next state.
            WatchResult::Success { events, state } => {
                self.stats.watch_success.incr();
                let next_state = WatchLoopState::ContinueWatching(state);
                (next_state, events)
            }
            // The watch has timed out, so we'll retry the watch with the same state as before.
            WatchResult::Timeout => {
                self.stats.watch_timeout.incr();
                (WatchLoopState::ContinueWatching(prev_watch_state), vec![])
            }
            // The watch needs to be reset. Inner watcher state has not changed.
            WatchResult::ResetWatcher => {
                self.stats.watch_reset.incr();
                (WatchLoopState::ResetWatcher(prev_watch_state), vec![])
            }
        }
    }

    async fn log_and_sleep_after_error(&mut self, error: crate::etcd::EtcdError) {
        let delay = self.backoff.fail();
        error!(
            "Watcher for key {} encountered an etcd error ({:?}), will retry after {:?}",
            self.watcher_impl.key(),
            error,
            delay
        );
        self.stats.watch_etcd_errors.incr();
        sleep(delay).await;
    }
}

pub(crate) enum WatchLoopState<V> {
    ContinueWatching(WatchState<V>),
    ResetWatcher(WatchState<V>),
}

impl<V> WatchLoopState<V> {
    fn current_value(&self) -> &V {
        match self {
            WatchLoopState::ResetWatcher(watch_state) => &watch_state.current_value,
            WatchLoopState::ContinueWatching(watch_state) => &watch_state.current_value,
        }
    }
}
