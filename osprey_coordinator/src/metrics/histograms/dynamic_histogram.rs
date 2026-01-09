use async_trait::async_trait;
use fxhash::FxHashMap;
use parking_lot::RwLock;
use std::{collections::hash_map::Entry, fmt::Debug, time::Duration};

use crate::metrics::base_metric::{BaseMetric, DynamicTagKey, EmitMetrics};
use crate::metrics::SharedMetrics;

use super::pool_histograms::{HistogramMetrics, PoolHistograms};

pub type DynamicHistogramStorage<Key> = RwLock<FxHashMap<Key, PoolHistograms>>;

pub trait DynamicHistogram: BaseMetric {
    type Key: DynamicTagKey;

    #[doc(hidden)]
    fn storage(&self) -> &DynamicHistogramStorage<Self::Key>;

    #[doc(hidden)]
    #[inline(always)]
    fn record_with_key(&self, key: Self::Key, duration: Duration) {
        self.record_value_with_key(key, duration.as_millis() as _)
    }

    #[doc(hidden)]
    #[inline(always)]
    fn record_value_with_key(&self, key: Self::Key, value: u64) {
        let storage = self.storage();

        // First, try the *fast* path, for a dynamic key pair we've seen before.
        let locked = storage.read();
        if let Some(pool_histograms) = locked.get(&key) {
            pool_histograms.record_value(value);
            return;
        }
        drop(locked);

        // Second, try the *slow* path, which is that we have not discovered this dynamic key pair before,
        // so we must insert it into the storage.
        let mut locked = storage.write();
        match locked.entry(key) {
            Entry::Occupied(ent) => {
                ent.get().record_value(value);
            }
            Entry::Vacant(ent) => {
                let pool_histograms = PoolHistograms::new();
                pool_histograms.record_value(value);
                ent.insert(pool_histograms);
            }
        }
    }
}

/// A wrapper that disambiguates between static and dynamic histograms for the purpose
/// of the EmitMetrics auto trait impl.
#[derive(Default)]
pub struct DynamicHistogramWrapper<Histogram: DynamicHistogram>(Histogram);

impl<Histogram: DynamicHistogram> std::ops::Deref for DynamicHistogramWrapper<Histogram> {
    type Target = Histogram;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Histogram: DynamicHistogram> Debug for DynamicHistogramWrapper<Histogram> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicHistogram")
            .field("name", &Histogram::NAME)
            .field("static_tags", &Histogram::STATIC_TAGS)
            .field("dynamic_tag_keys", &Histogram::Key::get_tag_keys())
            .field("num_distinct_keys", &self.storage().read().len())
            .finish()
    }
}

#[async_trait]
impl<Histogram> EmitMetrics for DynamicHistogramWrapper<Histogram>
where
    Histogram: DynamicHistogram,
{
    async fn emit_metrics(&self, metrics: &SharedMetrics) {
        let storage = self.storage();
        let locked = storage.read();
        for (key, value) in locked.iter() {
            if let Some(histogram_metrics) = value.get_histogram_metrics() {
                Self::emit_dynamic_histogram_metrics(key, histogram_metrics, metrics);
            }
        }
    }
}

impl<Histogram> DynamicHistogramWrapper<Histogram>
where
    Histogram: DynamicHistogram,
{
    fn emit_dynamic_histogram_metrics(
        dynamic_key: &Histogram::Key,
        histogram_metrics: HistogramMetrics,
        metrics: &SharedMetrics,
    ) {
        Self::emit_counter_metrics(
            dynamic_key,
            metrics,
            format!("{}.count", Histogram::NAME).as_str(),
            histogram_metrics.sample_count as i64,
        );
        Self::emit_float_gauge_metrics(
            dynamic_key,
            metrics,
            format!("{}.avg", Histogram::NAME).as_str(),
            histogram_metrics.mean,
        );
        Self::emit_gauge_metrics(
            dynamic_key,
            metrics,
            format!("{}.median", Histogram::NAME).as_str(),
            histogram_metrics.median,
        );
        Self::emit_gauge_metrics(
            dynamic_key,
            metrics,
            format!("{}.95percentile", Histogram::NAME).as_str(),
            histogram_metrics.p95,
        );
        Self::emit_gauge_metrics(
            dynamic_key,
            metrics,
            format!("{}.99percentile", Histogram::NAME).as_str(),
            histogram_metrics.p99,
        );
        Self::emit_gauge_metrics(
            dynamic_key,
            metrics,
            format!("{}.min", Histogram::NAME).as_str(),
            histogram_metrics.min,
        );
        Self::emit_gauge_metrics(
            dynamic_key,
            metrics,
            format!("{}.max", Histogram::NAME).as_str(),
            histogram_metrics.max,
        );
    }

    fn emit_counter_metrics(
        dynamic_key: &Histogram::Key,
        metrics: &SharedMetrics,
        key: &str,
        value: i64,
    ) {
        let mut builder = metrics.count_with_tags(key, value);
        builder = Histogram::fold_static_tag_pairs(builder, |b, k, v| b.with_tag(k, v));
        builder = dynamic_key.fold_tag_pairs(builder, |b, k, v| b.with_tag(k, v));
        builder.try_send().ok();
    }

    fn emit_gauge_metrics(
        dynamic_key: &Histogram::Key,
        metrics: &SharedMetrics,
        key: &str,
        value: u64,
    ) {
        let mut builder = metrics.gauge_with_tags(key, value);
        builder = Histogram::fold_static_tag_pairs(builder, |b, k, v| b.with_tag(k, v));
        builder = dynamic_key.fold_tag_pairs(builder, |b, k, v| b.with_tag(k, v));
        builder.try_send().ok();
    }

    fn emit_float_gauge_metrics(
        dynamic_key: &Histogram::Key,
        metrics: &SharedMetrics,
        key: &str,
        value: f64,
    ) {
        let mut builder = metrics.gauge_with_tags(key, value);
        builder = Histogram::fold_static_tag_pairs(builder, |b, k, v| b.with_tag(k, v));
        builder = dynamic_key.fold_tag_pairs(builder, |b, k, v| b.with_tag(k, v));
        builder.try_send().ok();
    }
}
