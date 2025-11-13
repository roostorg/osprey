mod counter;
mod gauge;
mod histogram;
mod send;
#[cfg(test)]
pub(crate) mod statsd_output;

use std::hash::Hash;
use std::iter::{self, Empty};
use std::ops::Deref;
use std::sync::Arc;

use bytes_utils::Str;
use cadence::{Metric, MetricBuilder, MetricError, StatsdClient};

pub use self::counter::*;
pub use self::gauge::*;
pub use self::histogram::*;
pub use self::send::*;

/// A type that emits its own metrics.
/// Usually a collection of other named metrics,
/// like a struct.
pub trait MetricsEmitter {
    fn emit_metrics(
        &self,
        client: &StatsdClient,
        base_tags: &mut dyn Iterator<Item = (&str, &str)>,
    ) -> Result<(), MetricError>;
}

/// Boxes of types that implement `MetricsEmitter` implement `MetricsEmitter`
/// by calling `emit_metrics` on the inner value.
impl<T: MetricsEmitter> MetricsEmitter for Box<T> {
    fn emit_metrics(
        &self,
        client: &StatsdClient,
        base_tags: &mut dyn Iterator<Item = (&str, &str)>,
    ) -> Result<(), MetricError> {
        self.as_ref().emit_metrics(client, base_tags)
    }
}

/// `Arc`s of types that implement `MetricsEmitter` implement `MetricsEmitter`
/// by calling `emit_metrics` on the inner value.
impl<T: MetricsEmitter> MetricsEmitter for Arc<T> {
    fn emit_metrics(
        &self,
        client: &StatsdClient,
        base_tags: &mut dyn Iterator<Item = (&str, &str)>,
    ) -> Result<(), MetricError> {
        self.as_ref().emit_metrics(client, base_tags)
    }
}

#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct NopMetricsEmitter;

impl MetricsEmitter for NopMetricsEmitter {
    fn emit_metrics(
        &self,
        _client: &StatsdClient,
        _base_tags: &mut dyn Iterator<Item = (&str, &str)>,
    ) -> Result<(), MetricError> {
        Ok(())
    }
}

/// Wraps a [`MetricsEmitter`] or [`BaseMetric`] with base tags.
#[derive(Clone, Debug)]
pub struct TagWrapper<T> {
    tags: Vec<(Str, Str)>,
    inner: T,
}

impl<T> TagWrapper<T> {
    /// Wraps the given [`MetricsEmitter`] or [`BaseMetric`] with base tags.
    ///
    /// # Example
    ///
    /// ```
    /// # use crate::metrics::v2::TagWrapper;
    /// # let emitter = crate::metrics::v2::NopMetricsEmitter;
    /// TagWrapper::new([("foo", "bar")], emitter)
    /// # ;
    /// ```
    pub fn new<K, V>(tags: impl IntoIterator<Item = (K, V)>, inner: T) -> Arc<Self>
    where
        K: Into<Str>,
        V: Into<Str>,
    {
        Arc::new(TagWrapper {
            tags: tags
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            inner,
        })
    }

    /// Returns the inner emitter or metric.
    #[inline]
    pub fn inner(&self) -> &T {
        &self.inner
    }
}

impl<T: MetricsEmitter> MetricsEmitter for TagWrapper<T> {
    fn emit_metrics(
        &self,
        client: &StatsdClient,
        base_tags: &mut dyn Iterator<Item = (&str, &str)>,
    ) -> Result<(), MetricError> {
        let mut base_tags = base_tags
            .map(|t| t) // to unify the lifetimes
            .chain(self.tags.iter().map(|(k, v)| (k.deref(), v.deref())));
        self.inner.emit_metrics(client, &mut base_tags)
    }
}

impl<T: BaseMetric> BaseMetric for TagWrapper<T> {
    fn send_metric(
        &self,
        client: &StatsdClient,
        name: &str,
        base_tags: &mut dyn Iterator<Item = (&str, &str)>,
    ) -> Result<(), MetricError> {
        let mut base_tags = base_tags
            .map(|t| t) // to unify the lifetimes
            .chain(self.tags.iter().map(|(k, v)| (k.deref(), v.deref())));
        self.inner.send_metric(client, name, &mut base_tags)
    }
}

/// A single metric (e.g. a counter or a histogram).
pub trait BaseMetric {
    /// Send the data of the metric under the given name.
    fn send_metric(
        &self,
        client: &StatsdClient,
        name: &str,
        base_tags: &mut dyn Iterator<Item = (&str, &str)>,
    ) -> Result<(), MetricError>;
}

/// Boxes of types that implement `BaseMetric` implement `BaseMetric`
/// by calling `send_metric` on the inner value.
impl<T: BaseMetric> BaseMetric for Box<T> {
    fn send_metric(
        &self,
        client: &StatsdClient,
        name: &str,
        base_tags: &mut dyn Iterator<Item = (&str, &str)>,
    ) -> Result<(), MetricError> {
        self.as_ref().send_metric(client, name, base_tags)
    }
}

/// `Arc`s of types that implement `BaseMetric` implement `BaseMetric`
/// by calling `send_metric` on the inner value.
impl<T: BaseMetric> BaseMetric for Arc<T> {
    fn send_metric(
        &self,
        client: &StatsdClient,
        name: &str,
        base_tags: &mut dyn Iterator<Item = (&str, &str)>,
    ) -> Result<(), MetricError> {
        self.as_ref().send_metric(client, name, base_tags)
    }
}

/// A collection of key/value metric tags to apply to a raw number.
pub trait TagKey: Hash + Eq {
    type TagIterator<'a>: Iterator<Item = (&'a str, &'a str)>
    where
        Self: 'a;

    fn tags<'a>(&'a self) -> Self::TagIterator<'a>;
}

/// Helper trait to convert tag values to static strings.
/// This is implemented for types that can be used as tag values.
pub trait TagValue {
    fn as_tag_str(&self) -> &'static str;
}

impl TagValue for &'static str {
    fn as_tag_str(&self) -> &'static str {
        self
    }
}

impl TagValue for crate::metrics::string_interner::InternResult {
    fn as_tag_str(&self) -> &'static str {
        crate::metrics::DynamicTagValue::as_static_str(self)
    }
}

impl TagValue for tonic::Code {
    fn as_tag_str(&self) -> &'static str {
        crate::metrics::DynamicTagValue::as_static_str(self)
    }
}

impl TagValue for bool {
    fn as_tag_str(&self) -> &'static str {
        crate::metrics::DynamicTagValue::as_static_str(self)
    }
}

/// The unit type can be used as an empty set of metric tags.
impl TagKey for () {
    type TagIterator<'a> = Empty<(&'a str, &'a str)>;

    fn tags<'a>(&'a self) -> Self::TagIterator<'a> {
        iter::empty()
    }
}

/// Helper function to collect a set of base tags and a key's tags together
/// and send the metric.
pub(crate) fn send_submetric<'m, T: Metric + From<String>>(
    mut builder: MetricBuilder<'m, '_, T>,
    base_tags: impl IntoIterator<Item = (&'m str, &'m str)>,
    key: &'m impl TagKey,
) {
    for (base_key, base_value) in base_tags {
        builder = builder.with_tag(base_key, base_value);
    }
    for (k, v) in key.tags() {
        builder = builder.with_tag(k, v);
    }
    builder.try_send().ok();
}

#[doc(hidden)]
pub mod macro_support {
    pub use cadence::{MetricError, StatsdClient};
}
