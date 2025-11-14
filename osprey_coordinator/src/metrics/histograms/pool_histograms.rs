use std::{sync::Arc, thread, time::Duration};

use arc_swap::ArcSwap;
use dynamic_pool::{DynamicPool, DynamicReset};
use hdrhistogram::Histogram;
use lazy_static::lazy_static;
use tracing::{error, warn};

lazy_static! {
    static ref CPU_COUNT: usize = num_cpus::get();
}

/// A hdrHistogram wrapper with the DynamicReset trait implementation.
/// So this struct can be used as a DynamicPoolItem in a DynamicPool.
#[derive(Debug)]
pub struct PoolItemHistogram(Histogram<u64>);

impl DynamicReset for PoolItemHistogram {
    fn reset(&mut self) {}
}

impl std::ops::Deref for PoolItemHistogram {
    type Target = Histogram<u64>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for PoolItemHistogram {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// A arc swappable hdrHistograms stored in a dynamic pool.
#[derive(Debug)]
pub struct PoolHistograms(ArcSwap<DynamicPool<PoolItemHistogram>>);

impl std::ops::Deref for PoolHistograms {
    type Target = ArcSwap<DynamicPool<PoolItemHistogram>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for PoolHistograms {
    fn default() -> Self {
        Self::new()
    }
}

impl PoolHistograms {
    pub(crate) fn new() -> Self {
        PoolHistograms(ArcSwap::from(Self::create()))
    }

    #[inline(always)]
    fn create() -> Arc<DynamicPool<PoolItemHistogram>> {
        Arc::new(DynamicPool::new(*CPU_COUNT, *CPU_COUNT, || {
            // Create a Histogram that can count values in the [1..u64::max_value()] range with 1% precision.
            // If the provided parameters to create this histogram are invalid, returns an error, which should not happen.
            let h = hdrhistogram::Histogram::<u64>::new_with_bounds(1, u64::max_value(), 2)
                .map_err(|e| {
                    error!["Couldn't create histogram: {:?}", e];
                    e
                })
                .unwrap();
            PoolItemHistogram(h)
        }))
    }

    #[inline(always)]
    pub(crate) fn record(&self, duration: Duration) {
        self.record_value(duration.as_millis() as _)
    }

    #[inline(always)]
    pub(crate) fn record_value(&self, value: u64) {
        for _i in 0..2 {
            if let Some(mut item) = self.load().try_take() {
                if let Err(e) = item.record(value) {
                    warn!["Couldn't record value: {:?}", e];
                }
                return;
            }

            thread::yield_now();
        }
    }

    #[inline(always)]
    fn renew_and_swap(&self) -> Arc<DynamicPool<PoolItemHistogram>> {
        self.swap(Self::create())
    }

    // Merge all the histograms in the dynamic pool and calcualte the histogram
    pub fn get_histogram_metrics(&self) -> Option<HistogramMetrics> {
        let mut remaining = *CPU_COUNT;
        let old_histograms = self.renew_and_swap();
        let first = old_histograms.try_take()?;
        let mut merged = first.detach().0;
        remaining -= 1;

        for i in 0..2 {
            while let Some(next) = old_histograms.try_take() {
                let histogram = next.detach().0;
                remaining -= 1;
                merged.add(histogram).ok();
            }

            if remaining == 0 {
                break;
            }
            thread::yield_now();

            if i == 1 {
                warn!(
                    "Couldn't take all items. Still remaining {} items in the dynamic pool.",
                    remaining
                );
            }
        }

        let sample_count = merged.len();
        if sample_count == 0 {
            return None;
        }

        let mean = merged.mean();
        let p95 = merged.value_at_quantile(0.95);
        let p99 = merged.value_at_quantile(0.99);
        let min = merged.min();
        let max = merged.max();
        let median = merged.value_at_quantile(0.5);

        Some(HistogramMetrics {
            sample_count,
            mean,
            min,
            max,
            p95,
            p99,
            median,
        })
    }
}

#[derive(Debug)]
pub struct HistogramMetrics {
    pub sample_count: u64,
    pub mean: f64,
    pub min: u64,
    pub max: u64,
    pub p95: u64,
    pub p99: u64,
    pub median: u64,
}
