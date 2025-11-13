pub mod base_metric;
pub mod counters;
pub mod emit_worker;
pub mod gauges;
pub mod histograms;
pub mod macros;
mod recycling_socket_metrics_sink;
pub mod string_interner;
pub mod v2;

pub use crate::define_metrics;
pub use metrics_derive::{MetricsEmitter, TagKey};

use crate::metrics::recycling_socket_metrics_sink::BufferedUdpMetricSink;
pub use base_metric::{BaseMetric, DynamicTagKey, DynamicTagValue, EmitMetrics, StaticTag};
use cadence::{
    ext::{ToCounterValue, ToGaugeValue, ToHistogramValue, ToTimerValue},
    Counted, Counter, Gauge, Gauged, Histogram, Histogrammed, MetricBuilder, MetricResult,
    QueuingMetricSink, StatsdClient, Timed, Timer,
};
use std::sync::Arc;
use std::{env, fmt::Debug};
use thiserror::Error;

pub type SharedMetrics = Arc<WrappedStatsdClient>;

#[derive(Debug, Clone, Error)]
#[error("Failed to build metrics client: {0}")]
pub struct MetricsClientBuildError(String);

pub fn new_client(prefix: impl Into<String>) -> Result<SharedMetrics, MetricsClientBuildError> {
    MetricsClientBuilder::new(prefix).build()
}

#[derive(Debug, Clone)]
pub struct MetricsClientBuilder {
    prefix: String,
    tags: Vec<(String, String)>,
}

impl MetricsClientBuilder {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            tags: Vec::new(),
        }
    }

    pub fn build(mut self) -> Result<SharedMetrics, MetricsClientBuildError> {
        let host = std::env::var("DD_AGENT_HOST")
            .or_else(|_| std::env::var("DATADOG_TRACE_AGENT_HOSTNAME"))
            .unwrap_or_else(|_| "127.0.0.1".into());
        let host = (host.as_ref(), 8125);
        let buffered_sink = BufferedUdpMetricSink::from(host)
            .map_err(|e| MetricsClientBuildError(format!("metric error: {e}")))?;
        let queuing_sink = QueuingMetricSink::from(buffered_sink);

        let mut tags = base_tags();
        tags.append(&mut self.tags);

        let wsc = WrappedStatsdClient::with_base_tags(
            StatsdClient::from_sink(self.prefix.as_ref(), queuing_sink),
            tags,
        );

        Ok(Arc::new(wsc))
    }

    pub fn with_tags(mut self, tags: Vec<(String, String)>) -> Self {
        self.tags.extend(tags);
        self
    }

    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.push((key.into(), value.into()));
        self
    }
}

fn base_tags() -> Vec<(String, String)> {
    let mut tags = Vec::new();

    if let Ok(tag) = env::var("DD_ENTITY_ID") {
        tags.push(("dd.internal.entity_id".to_owned(), tag))
    }

    tags
}

#[derive(Debug)]
pub struct WrappedStatsdClient {
    base_tags: Vec<(String, String)>,
    wrapped: StatsdClient,
}

impl WrappedStatsdClient {
    pub fn new(wrapped: StatsdClient) -> Self {
        Self {
            base_tags: vec![],
            wrapped,
        }
    }

    pub fn with_base_tags<I, K, V>(wrapped: StatsdClient, base_tags: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            base_tags: base_tags
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            wrapped,
        }
    }

    // gauge
    pub fn gauge_with_tags<'a, T: ToGaugeValue>(
        &'a self,
        key: &'a str,
        value: T,
    ) -> MetricBuilder<'a, 'a, Gauge> {
        self.base_tags
            .iter()
            .fold(self.wrapped.gauge_with_tags(key, value), |acc, (k, v)| {
                acc.with_tag(k, v)
            })
    }

    pub fn gauge<'a, T: ToGaugeValue>(&'a self, key: &'a str, value: T) -> MetricResult<Gauge> {
        self.gauge_with_tags(key, value).try_send()
    }

    // time:

    pub fn time_with_tags<'a, T: ToTimerValue>(
        &'a self,
        key: &'a str,
        value: T,
    ) -> MetricBuilder<'a, 'a, Timer> {
        self.base_tags
            .iter()
            .fold(self.wrapped.time_with_tags(key, value), |acc, (k, v)| {
                acc.with_tag(k, v)
            })
    }

    pub fn time<'a, T: ToTimerValue>(&'a self, key: &'a str, value: T) -> MetricResult<Timer> {
        self.time_with_tags(key, value).try_send()
    }

    // count:

    pub fn count_with_tags<'a, T: ToCounterValue>(
        &'a self,
        key: &'a str,
        value: T,
    ) -> MetricBuilder<'a, 'a, Counter> {
        self.base_tags
            .iter()
            .fold(self.wrapped.count_with_tags(key, value), |acc, (k, v)| {
                acc.with_tag(k, v)
            })
    }

    pub fn count<'a, T: ToCounterValue>(&'a self, key: &'a str, value: T) -> MetricResult<Counter> {
        self.count_with_tags(key, value).try_send()
    }

    pub fn incr(&self, key: &str) -> MetricResult<Counter> {
        self.incr_with_tags(key).try_send()
    }

    pub fn incr_with_tags<'a>(&'a self, key: &'a str) -> MetricBuilder<'a, 'a, Counter> {
        self.count_with_tags(key, 1)
    }

    pub fn decr(&self, key: &str) -> MetricResult<Counter> {
        self.decr_with_tags(key).try_send()
    }

    pub fn decr_with_tags<'a>(&'a self, key: &'a str) -> MetricBuilder<'a, 'a, Counter> {
        self.count_with_tags(key, -1)
    }

    // histogram:
    pub fn histogram<T: ToHistogramValue>(&self, key: &str, value: T) -> MetricResult<Histogram> {
        self.histogram_with_tags(key, value).try_send()
    }

    pub fn histogram_with_tags<'a, T: ToHistogramValue>(
        &'a self,
        key: &'a str,
        value: T,
    ) -> MetricBuilder<'a, 'a, Histogram> {
        self.base_tags.iter().fold(
            self.wrapped.histogram_with_tags(key, value),
            |acc, (k, v)| acc.with_tag(k, v),
        )
    }
}
