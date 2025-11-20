use std::{
    fmt::Debug,
    sync::atomic::{AtomicI64, Ordering},
};

use async_trait::async_trait;

use crate::metrics::{BaseMetric, EmitMetrics, SharedMetrics};

pub trait StaticCounter: BaseMetric {
    #[doc(hidden)]
    fn value(&self) -> &AtomicI64;

    #[inline(always)]
    fn incr_by(&self, value: i64) {
        self.value().fetch_add(value, Ordering::Relaxed);
    }

    #[inline(always)]
    fn decr_by(&self, value: i64) {
        self.incr_by(-value);
    }

    #[inline(always)]
    fn incr(&self) {
        self.incr_by(1);
    }

    #[inline(always)]
    fn decr(&self) {
        self.decr_by(1);
    }
}

/// A wrapper that disambiguates between static and dynamic counters for the purpose
/// of the EmitMetrics auto trait impl.
#[derive(Default)]
pub struct StaticCounterWrapper<Counter: StaticCounter>(Counter);

impl<Counter: StaticCounter> std::ops::Deref for StaticCounterWrapper<Counter> {
    type Target = Counter;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Counter: StaticCounter> Debug for StaticCounterWrapper<Counter> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticCounter")
            .field("name", &Counter::NAME)
            .field("static_tags", &Counter::STATIC_TAGS)
            .field("value", &self.value().load(Ordering::Relaxed))
            .finish()
    }
}

#[async_trait]
impl<Counter> EmitMetrics for StaticCounterWrapper<Counter>
where
    Counter: StaticCounter,
{
    async fn emit_metrics(&self, metrics: &SharedMetrics) {
        let count = self.value().swap(0, Ordering::Relaxed);
        if count == 0 {
            return;
        }

        let mut builder = metrics.count_with_tags(Counter::NAME, count);
        builder = Counter::fold_static_tag_pairs(builder, |b, k, v| b.with_tag(k, v));
        builder.try_send().ok();
    }
}
