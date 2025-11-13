use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use cadence::{Gauge, MetricBuilder};

use crate::metrics::base_metric::BaseMetric;
use crate::metrics::SharedMetrics;

#[derive(Debug)]
pub struct GaugeInner {
    pub(crate) current: AtomicU64,
    pub(crate) max: AtomicU64,
    pub(crate) min: AtomicU64,
}

impl Default for GaugeInner {
    fn default() -> Self {
        Self {
            current: AtomicU64::new(0),
            // min -> MAX, and max -> MIN are correct here. we are using the smallest/largest value in the domain
            // to track: "the gauge has not changed within this interval."
            max: AtomicU64::new(u64::MIN),
            min: AtomicU64::new(u64::MAX),
        }
    }
}

impl GaugeInner {
    // Get the current value of a gauge
    pub fn current(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }

    pub(crate) fn min(&self, current: u64) -> u64 {
        let min = self.min.load(Ordering::Relaxed);
        if min == u64::MAX {
            current
        } else {
            min
        }
    }

    pub(crate) fn max(&self, current: u64) -> u64 {
        let max = self.max.load(Ordering::Relaxed);
        if max == u64::MIN {
            current
        } else {
            max
        }
    }

    pub(crate) fn swap_min(&self, current: u64) -> u64 {
        let min = self.min.swap(u64::MAX, Ordering::Relaxed);
        if min == u64::MAX {
            current
        } else {
            min
        }
    }

    pub(crate) fn swap_max(&self, current: u64) -> u64 {
        let max = self.max.swap(u64::MIN, Ordering::Relaxed);
        if max == u64::MIN {
            current
        } else {
            max
        }
    }

    pub(crate) fn decr_by(&self, value: u64) {
        self.current.fetch_sub(value, Ordering::Relaxed);

        // Track min/max values in the reporting window.
        let current = self.current();
        self.min.fetch_min(current, Ordering::Relaxed);
        self.max.fetch_max(current, Ordering::Relaxed);
    }

    pub(crate) fn incr_by(&self, value: u64) {
        self.current.fetch_add(value, Ordering::Relaxed);

        // Track min/max values in the reporting window.
        let current = self.current();
        self.min.fetch_min(current, Ordering::Relaxed);
        self.max.fetch_max(current, Ordering::Relaxed);
    }

    pub(crate) fn set(&self, value: u64) {
        self.current.store(value, Ordering::Relaxed);

        // Track min/max values in the reporting window.
        self.min.fetch_min(value, Ordering::Relaxed);
        self.max.fetch_max(value, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> GaugeSnapshot {
        let current = self.current();
        let mut min = self.swap_min(current);
        let mut max = self.swap_max(current);

        // In the rare case that these can end up swapped due to de-sync between resetting the min and max, and an active
        // metrics recorder, just swap the min/max so they are sensible for this metrics window.
        if min > max {
            std::mem::swap(&mut min, &mut max);
        }

        GaugeSnapshot { current, min, max }
    }
}

pub(crate) struct GaugeSnapshot {
    pub(crate) current: u64,
    pub(crate) min: u64,
    pub(crate) max: u64,
}

impl GaugeSnapshot {
    pub(crate) fn emit<M, TApplyDynamicTags>(
        self,
        metrics: &SharedMetrics,
        apply_dynamic_tags: TApplyDynamicTags,
    ) where
        M: BaseMetric,
        TApplyDynamicTags: for<'a> Fn(MetricBuilder<'a, 'a, Gauge>) -> MetricBuilder<'a, 'a, Gauge>,
    {
        {
            let mut builder = metrics.gauge_with_tags(<M as BaseMetric>::NAME, self.current);
            builder = <M as BaseMetric>::fold_static_tag_pairs(builder, |b, k, v| b.with_tag(k, v));
            builder = apply_dynamic_tags(builder);
            builder.try_send().ok();
        }
        {
            let metric_name = format!("{}.min", <M as BaseMetric>::NAME);
            let mut builder = metrics.gauge_with_tags(&metric_name, self.min);
            builder = <M as BaseMetric>::fold_static_tag_pairs(builder, |b, k, v| b.with_tag(k, v));
            builder = apply_dynamic_tags(builder);
            builder.try_send().ok();
        }
        {
            let metric_name = format!("{}.max", <M as BaseMetric>::NAME);
            let mut builder = metrics.gauge_with_tags(&metric_name, self.max);
            builder = <M as BaseMetric>::fold_static_tag_pairs(builder, |b, k, v| b.with_tag(k, v));
            builder = apply_dynamic_tags(builder);
            builder.try_send().ok();
        }
    }
}
pub struct One;
pub struct Many(pub u64);

pub trait StoredValue: Send {
    fn get(&self) -> u64;
}

impl StoredValue for One {
    fn get(&self) -> u64 {
        1
    }
}

impl StoredValue for Many {
    fn get(&self) -> u64 {
        self.0
    }
}

pub struct GaugeHandleOwned<Value: StoredValue = One> {
    pub(crate) value: Value,
    pub(crate) inner: Arc<GaugeInner>,
}

impl<Value: StoredValue> Drop for GaugeHandleOwned<Value> {
    fn drop(&mut self) {
        self.inner
            .current
            .fetch_sub(self.value.get(), Ordering::Relaxed);
    }
}
