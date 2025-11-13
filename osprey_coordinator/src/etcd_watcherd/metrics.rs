use std::time::Duration;

use crate::cached_futures::{CachedFutureRef, CachedFutures};
use crate::metrics::{EmitMetrics, SharedMetrics};
use tokio::{
    task::JoinHandle,
    time::{interval, MissedTickBehavior},
};

use crate::etcd_watcherd::{watcher_impl::WatchEvents, WatchHandle, Watcher};

pub struct WatcherMetrics {
    watcher: Watcher,
    metrics: SharedMetrics,
}

impl Watcher {
    /// Spawns a worker that will emit metrics to the provided [`SharedMetrics`].
    ///
    /// This worker will last for as long as the returned [`WatcherMetricsGuard`] lasts,
    /// and will automatically shut down when it is dropped.
    pub fn emit_metrics(&self, metrics: SharedMetrics) -> WatcherMetricsGuard {
        let watcher = self.clone();
        let metrics = WatcherMetrics { watcher, metrics };
        let emit_loop_join_handle = tokio::task::spawn(metrics.emit_loop());

        WatcherMetricsGuard {
            emit_loop_join_handle,
        }
    }
}

impl WatcherMetrics {
    async fn emit_loop(mut self) {
        let mut interval = interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            self.emit_metrics().await;
        }
    }

    async fn emit_metrics(&mut self) {
        emit_watcher_counts("key", &self.metrics, &self.watcher.active_watchers.key);
        emit_watcher_counts(
            "recursive_key",
            &self.metrics,
            &self.watcher.active_watchers.recursive_key,
        );

        emit_subscriber_counts("key", &self.metrics, &self.watcher.active_watchers.key);
        emit_subscriber_counts(
            "recursive_key",
            &self.metrics,
            &self.watcher.active_watchers.recursive_key,
        );

        self.watcher.stats.emit_metrics(&self.metrics).await;
    }
}

pub struct WatcherMetricsGuard {
    emit_loop_join_handle: JoinHandle<()>,
}

impl Drop for WatcherMetricsGuard {
    fn drop(&mut self) {
        self.emit_loop_join_handle.abort();
    }
}

fn emit_watcher_counts<K, T, E>(
    watcher_type: &str,
    metrics: &SharedMetrics,
    cached_futures: &CachedFutures<K, T, E>,
) where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + 'static,
{
    let (active_watchers, pending_watchers) =
        cached_futures.fold((0, 0), |(active, pending), (_, fut_ref)| match fut_ref {
            CachedFutureRef::Ready(_) => (active + 1, pending),
            CachedFutureRef::Pending => (active, pending + 1),
        });

    metrics
        .gauge_with_tags("watchers", active_watchers)
        .with_tag("status", "active")
        .with_tag("type", watcher_type)
        .send();

    metrics
        .gauge_with_tags("watchers", pending_watchers)
        .with_tag("status", "pending")
        .with_tag("type", watcher_type)
        .send();
}

fn emit_subscriber_counts<K, T, E>(
    watcher_type: &str,
    metrics: &SharedMetrics,
    cached_futures: &CachedFutures<K, WatchHandle<T>, E>,
) where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + WatchEvents + 'static,
    E: Clone + Send + Sync + 'static,
{
    let subscribers = cached_futures.fold_ready(0, |subscribers, (_, handle)| {
        subscribers + handle.num_subscribers()
    });

    metrics
        .gauge_with_tags("subscribers", subscribers as u64)
        .with_tag("type", watcher_type)
        .send();
}
