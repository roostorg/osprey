use std::{fmt::Debug, time::Duration};

use crate::metrics::{BaseMetric, EmitMetrics, SharedMetrics};
use async_trait::async_trait;

use super::pool_histograms::{HistogramMetrics, PoolHistograms};

#[async_trait]
pub trait StaticHistogram: BaseMetric {
    fn value(&self) -> &PoolHistograms;

    fn record(&self, duration: Duration) {
        self.value().record(duration);
    }

    fn record_value(&self, value: u64) {
        self.value().record_value(value);
    }
}

/// A wrapper that disambiguates between static and dynamic histograms for the purpose
/// of the EmitMetrics auto trait impl.
#[derive(Default)]
pub struct StaticHistogramWrapper<Histogram: StaticHistogram>(Histogram);

impl<Histogram: StaticHistogram> std::ops::Deref for StaticHistogramWrapper<Histogram> {
    type Target = Histogram;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Histogram: StaticHistogram> Debug for StaticHistogramWrapper<Histogram> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticHistogram")
            .field("name", &Histogram::NAME)
            .field("static_tags", &Histogram::STATIC_TAGS)
            .field("used_histograms", &self.value().load().used())
            .finish()
    }
}

#[async_trait]
impl<Histogram> EmitMetrics for StaticHistogramWrapper<Histogram>
where
    Histogram: StaticHistogram,
{
    async fn emit_metrics(&self, metrics: &SharedMetrics) {
        if let Some(histogram_metrics) = self.value().get_histogram_metrics() {
            Self::emit_static_histogram_metrics(histogram_metrics, metrics);
        }
    }
}

impl<Histogram> StaticHistogramWrapper<Histogram>
where
    Histogram: StaticHistogram,
{
    fn emit_static_histogram_metrics(histogram_metrics: HistogramMetrics, metrics: &SharedMetrics) {
        Self::emit_counter_metrics(
            metrics,
            format!("{}.count", Histogram::NAME).as_str(),
            histogram_metrics.sample_count as i64,
        );
        Self::emit_float_gauge_metrics(
            metrics,
            format!("{}.avg", Histogram::NAME).as_str(),
            histogram_metrics.mean,
        );
        Self::emit_gauge_metrics(
            metrics,
            format!("{}.median", Histogram::NAME).as_str(),
            histogram_metrics.median,
        );
        Self::emit_gauge_metrics(
            metrics,
            format!("{}.95percentile", Histogram::NAME).as_str(),
            histogram_metrics.p95,
        );
        Self::emit_gauge_metrics(
            metrics,
            format!("{}.99percentile", Histogram::NAME).as_str(),
            histogram_metrics.p99,
        );
        Self::emit_gauge_metrics(
            metrics,
            format!("{}.min", Histogram::NAME).as_str(),
            histogram_metrics.min,
        );
        Self::emit_gauge_metrics(
            metrics,
            format!("{}.max", Histogram::NAME).as_str(),
            histogram_metrics.max,
        );
    }

    fn emit_counter_metrics(metrics: &SharedMetrics, key: &str, value: i64) {
        let mut builder = metrics.count_with_tags(key, value);
        builder = Histogram::fold_static_tag_pairs(builder, |b, k, v| b.with_tag(k, v));
        builder.try_send().ok();
    }

    fn emit_gauge_metrics(metrics: &SharedMetrics, key: &str, value: u64) {
        let mut builder = metrics.gauge_with_tags(key, value);
        builder = Histogram::fold_static_tag_pairs(builder, |b, k, v| b.with_tag(k, v));
        builder.try_send().ok();
    }

    fn emit_float_gauge_metrics(metrics: &SharedMetrics, key: &str, value: f64) {
        let mut builder = metrics.gauge_with_tags(key, value);
        builder = Histogram::fold_static_tag_pairs(builder, |b, k, v| b.with_tag(k, v));
        builder.try_send().ok();
    }
}
