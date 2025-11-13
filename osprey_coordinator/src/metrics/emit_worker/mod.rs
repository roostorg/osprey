use crate::tokio_utils::AbortOnDrop;
use std::{sync::Arc, time::Duration};

use tokio::time::{interval, MissedTickBehavior};

use crate::metrics::{EmitMetrics, SharedMetrics};

const DEFAULT_METRIC_REPORTING_FREQUENCY: Duration = Duration::from_millis(300);

pub trait SpawnEmitWorker {
    #[must_use = "spawn_emit_worker return value must be used."]
    fn spawn_emit_worker(self, cadence_metrics: SharedMetrics) -> AbortOnDrop<()>;

    #[must_use = "spawn_emit_worker_with_frequency return value must be used."]
    fn spawn_emit_worker_with_frequency(
        self,
        cadence_metrics: SharedMetrics,
        frequency: Duration,
    ) -> AbortOnDrop<()>;
}

impl<T> SpawnEmitWorker for Arc<T>
where
    T: EmitMetrics,
{
    fn spawn_emit_worker(self, cadence_metrics: SharedMetrics) -> AbortOnDrop<()> {
        self.spawn_emit_worker_with_frequency(cadence_metrics, DEFAULT_METRIC_REPORTING_FREQUENCY)
    }

    fn spawn_emit_worker_with_frequency(
        self,
        cadence_metrics: SharedMetrics,
        frequency: Duration,
    ) -> AbortOnDrop<()> {
        let mut interval = interval(frequency);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let join_handle = tokio::task::spawn(async move {
            loop {
                interval.tick().await;
                self.emit_metrics(&cadence_metrics).await;
            }
        });

        AbortOnDrop::new(join_handle)
    }
}

impl<T> SpawnEmitWorker for T
where
    T: EmitMetrics,
{
    fn spawn_emit_worker(self, cadence_metrics: SharedMetrics) -> AbortOnDrop<()> {
        self.spawn_emit_worker_with_frequency(cadence_metrics, DEFAULT_METRIC_REPORTING_FREQUENCY)
    }

    fn spawn_emit_worker_with_frequency(
        self,
        cadence_metrics: SharedMetrics,
        frequency: Duration,
    ) -> AbortOnDrop<()> {
        let mut interval = interval(frequency);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let join_handle = tokio::task::spawn(async move {
            loop {
                interval.tick().await;
                self.emit_metrics(&cadence_metrics).await;
            }
        });
        AbortOnDrop::new(join_handle)
    }
}
