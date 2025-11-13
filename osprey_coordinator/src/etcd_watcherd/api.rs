use std::{fmt::Debug, sync::Arc, time::Duration};

use crate::cached_futures::CachedFutures;
use crate::metrics::counters::StaticCounter;
use crate::tokio_utils::AbortOnDrop;
use anyhow::Result;
use parking_lot::RwLock;
use tokio::{sync::mpsc, task::JoinHandle, time::interval};

use crate::etcd_watcherd::{
    KeyWatchEvents, RecursiveKeyWatchEvents, WatchEvents, WatcherInner, WatcherLoop, WatcherStats,
    WatcherSubscription, WatcherSubscriptionId,
};

const WATCHER_SWEEP_INTERVAL: Duration = Duration::from_secs(120);

#[derive(Clone, Debug, Default)]
pub(crate) struct ActiveWatchers {
    pub(crate) key: CachedFutures<String, WatchHandle<KeyWatchEvents>, Arc<crate::etcd::EtcdError>>,
    pub(crate) recursive_key:
        CachedFutures<String, WatchHandle<RecursiveKeyWatchEvents>, Arc<crate::etcd::EtcdError>>,
}

#[derive(Clone, Debug)]
pub struct Watcher {
    client: crate::etcd::Client,
    _sweeper_abort_on_drop: Arc<AbortOnDrop<()>>,
    pub(crate) active_watchers: ActiveWatchers,
    pub(crate) stats: Arc<WatcherStats>,
}

impl Watcher {
    /// Creates a new [`Watcher`], using the given etcd client.
    ///
    /// Ideally for the lifespan of your application, you should use a single watcher instance.
    ///
    /// This watcher instance will multiplex all watches to etcd, so it makes senes to use it as a
    /// singleton within your application.
    pub fn new(client: crate::etcd::Client) -> Self {
        let active_watchers = ActiveWatchers::default();
        let stats = Arc::new(WatcherStats::default());
        let sweeper_abort_on_drop = Arc::new(AbortOnDrop::new(tokio::task::spawn(
            Self::sweep_watchers(active_watchers.clone(), stats.clone()),
        )));

        Self {
            client,
            active_watchers,
            _sweeper_abort_on_drop: sweeper_abort_on_drop,
            stats,
        }
    }

    /// This background task is responsible for cleaning up watchers that no longer are being actively used.
    async fn sweep_watchers(active_watchers: ActiveWatchers, stats: Arc<WatcherStats>) {
        let mut interval = interval(WATCHER_SWEEP_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            let mut watchers_swept = 0;
            watchers_swept += active_watchers
                .key
                .retain_ready(|handle| handle.num_handles() > 1);
            watchers_swept += active_watchers
                .recursive_key
                .retain_ready(|handle| handle.num_handles() > 1);

            stats.watchers_swept.incr_by(watchers_swept as _);
        }
    }

    /// Watches a single key for changes.
    ///
    /// Returns a [`WatchHandle`] that can be used to acquire the current value of the key at any given time,
    /// or subscribe to real time updates to that key.
    pub async fn watch_key(&self, key: impl Into<String>) -> Result<WatchHandle<KeyWatchEvents>> {
        let key = key.into();
        let client = self.client.clone();
        let stats = self.stats.clone();
        let watcher_future =
            self.active_watchers
                .key
                .get_or_cache_default(key, move |key| async move {
                    use crate::etcd_watcherd::watcher_impl::key_watcher::KeyWatcher;
                    let watcher_impl = KeyWatcher::new(client, key);
                    let handle = WatcherLoop::start(watcher_impl, stats)
                        .await
                        .map_err(Arc::new)?;
                    Ok(handle)
                });

        watcher_future
            .await
            .map_err(|e| anyhow::anyhow!("etcd error: {:?}", e))
    }

    /// Watches the keys within a directory for changes.
    ///
    /// Returns a [`WatchHandle`] that can be used to acquire the current value of the keys within the directory
    /// at any given time, or subscribe to real time updates to that key.
    pub async fn watch_key_recursive(
        &self,
        key: impl Into<String>,
    ) -> Result<WatchHandle<RecursiveKeyWatchEvents>> {
        let key = key.into();
        let client = self.client.clone();
        let stats = self.stats.clone();
        let watcher_future = self.active_watchers.recursive_key.get_or_cache_default(
            key,
            move |key| async move {
                use crate::etcd_watcherd::watcher_impl::recursive_key_watcher::RecursiveKeyWatcher;
                let watcher_impl = RecursiveKeyWatcher::new(client, key);
                let handle = WatcherLoop::start(watcher_impl, stats)
                    .await
                    .map_err(Arc::new)?;
                Ok(handle)
            },
        );

        watcher_future
            .await
            .map_err(|e| anyhow::anyhow!("etcd error: {:?}", e))
    }
}

/// Returned by the [`Watcher::watch_key`] or [`Watcher::watch_key_recursive`] methods.
///
/// Allows you to read the key's current value with [`WatchHandle::current_value`], and also subscribe to updates
/// to the key using [`WatchHandle::events`].
#[derive(Clone)]
pub struct WatchHandle<E>
where
    E: WatchEvents,
{
    /// Holds an arc to the WatcherInner that contains the state of the watched key, and the
    /// means to establish additional subscriptions.
    inner: Arc<RwLock<WatcherInner<E>>>,
    /// A join handle to the background task that is keeping the watcher synced with etcd.
    watcher_loop_abort_on_drop: Arc<AbortOnDrop<()>>,
}

impl<E> WatchHandle<E>
where
    E: WatchEvents,
{
    pub(crate) fn new(
        inner: Arc<RwLock<WatcherInner<E>>>,
        watcher_loop_join_handle: JoinHandle<()>,
    ) -> Self {
        Self {
            inner,
            watcher_loop_abort_on_drop: Arc::new(AbortOnDrop::new(watcher_loop_join_handle)),
        }
    }

    /// Returns the current value of the key. This is a snapshotted value, meaning that it is immutable.
    pub fn current_value(&self) -> E::Value {
        self.inner.read().current_value().clone()
    }

    /// Consumes the [`WatchHandle`] and returns a [`WatcherEvents`], which allows your application to be notified of changes to watched keys in real time.
    ///
    /// We will try and emit the minimal set of events and eliminate duplicate events that would net in no effective state change. What this means is
    /// that if a key is set to the same value multiple times in etcd, where as etcd might update the watcher internally, we will not emit these events
    /// as they result in no effective change of state.
    ///
    /// # Example:
    /// ```no_run
    /// # use anyhow::Result;
    /// # use etcd_watcherd::{Watcher, KeyWatchEvents};
    /// # async fn example() -> Result<()> {
    /// let watcher = Watcher::new(crate::etcd::Client::from_etcd_peers()?);
    /// let mut events = watcher.watch_key("/hello").await?.events();
    /// loop {
    ///     let KeyWatchEvents::FullSync { value } = events.next().await;
    ///     println!("key updated: {:?}", value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn events(self) -> WatcherEvents<E> {
        let WatcherSubscription {
            subscription_id,
            receiver,
            full_sync_event,
        } = self.inner.write().create_subscription();

        WatcherEvents {
            handle: self,
            subscription_id,
            initial_event: Some(Box::new(full_sync_event)),
            receiver,
        }
    }

    /// Returns the number of active subscribers for this watch handle.
    pub(crate) fn num_subscribers(&self) -> usize {
        self.inner.read().num_subscribers()
    }

    /// Returns the number of copies of this watch handle that are currently active in the
    /// program.
    fn num_handles(&self) -> usize {
        // We are using the strong count of the loop's abort on drop, since this will correspond 1:1
        // with the number of copies this handle has active.
        Arc::strong_count(&self.watcher_loop_abort_on_drop)
    }
}

/// Returned by [`WatchHandle::events`].
///
/// Acts as a stream of events that can be consumed to be kept up-to-date on the changes to an etcd key
/// in real time.
pub struct WatcherEvents<E>
where
    E: WatchEvents,
{
    handle: WatchHandle<E>,
    subscription_id: WatcherSubscriptionId,
    receiver: mpsc::Receiver<E>,
    // a box is chosen here to reduce the size of the struct, as this will be empty most of the time,
    // as this event is consumed by the first call to [`WatcherEvents::next`], or [`WatcherEvents::initial_event`].
    initial_event: Option<Box<E>>,
}

impl<E> Drop for WatcherEvents<E>
where
    E: WatchEvents,
{
    fn drop(&mut self) {
        let mut inner = self.handle.inner.write();
        inner.remove_subscription(self.subscription_id);
    }
}

impl<E> WatcherEvents<E>
where
    E: WatchEvents,
{
    /// Returns the "initial watch event", which is either [`crate::RecursiveKeyWatchEvents::FullSync`] or [`crate::KeyWatchEvents::FullSync`]
    /// depending on what kind of watcher you have initialized (recursive or not).
    ///
    /// # Panics
    /// This method must be called before the first call to `next`, and must only be called once. If these invariants are not upheld, this method
    /// will cause a panic.
    pub fn initial_event(&mut self) -> E {
        *self.initial_event.take().expect("invariant: `initial_event` can only be called once, and before the first call to `next`.")
    }

    /// Returns a new copy of the [`WatchHandle`] that this [`WatcherEvents`] is using.
    pub fn handle(&self) -> WatchHandle<E> {
        self.handle.clone()
    }

    /// Awaits a the next key change event.
    ///
    /// If `initial_event` was not called, this method will return immediately the first time called with the initial event, otherwise
    /// will wait until an event is received.
    ///
    /// It is *safe* to drop this future and re-call it, (for example, wrapping this with a `timeout`, or using the `select!` macro.)
    /// No events will be dropped if this future is cancelled, as long as the return value is not dropped.
    pub async fn next(&mut self) -> E {
        if let Some(event) = self.initial_event.take() {
            return *event;
        }

        match self.receiver.recv().await {
            Some(event) => event,
            // The subscription has ended, which means we've fallen too far behind,so we'll automatically re-create the subscription,
            // and emit a full sync event.
            None => self.recreate_subscription(),
        }
    }

    fn recreate_subscription(&mut self) -> E {
        let WatcherSubscription {
            subscription_id,
            receiver,
            full_sync_event,
        } = self
            .handle
            .inner
            .write()
            .recreate_subscription(self.subscription_id);

        self.receiver = receiver;
        self.subscription_id = subscription_id;
        full_sync_event
    }
}
