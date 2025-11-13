use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;

use super::{GaugeHandleOwned, GaugeInner, Many, One};
use crate::metrics::base_metric::{BaseMetric, EmitMetrics};
use crate::metrics::SharedMetrics;

pub trait StaticGauge: BaseMetric {
    #[doc(hidden)]
    fn inner(&self) -> &GaugeInner;

    #[doc(hidden)]
    fn inner_arc(&self) -> Arc<GaugeInner>;

    /// Increments the gauge by a given value.
    ///
    /// # Safety
    /// This method must be called with an accompanied [`StaticGauge::decr_by`], otherwise the state of the gauge
    /// will be incorrect.
    ///
    /// More conveniently and safely though, the use of [`StaticGauge::acquire_owned`]
    /// or [`StaticGauge::acquire_many_owned`] is recommended,
    /// which automatically decrements the gauge for you when the returned [`GaugeHandle`] falls out of scope.
    #[inline(always)]
    unsafe fn incr_by(&self, value: u64) {
        self.inner().incr_by(value);
    }

    /// Decrements the gauge by a given value.
    ///
    /// # Safety
    /// The gauge value after decrement could underflow.
    ///
    /// See [`StaticGauge::incr_by`] for more information.
    #[inline(always)]
    unsafe fn decr_by(&self, value: u64) {
        self.inner().decr_by(value);
    }

    /// Increments the gauge by 1.
    ///
    /// # Safety
    /// This method must be called with an accompanied [`StaticGauge::decr`], otherwise the state of the gauge
    /// will be incorrect.
    ///
    /// See [`StaticGauge::incr_by`] for more information.
    #[inline(always)]
    unsafe fn incr(&self) {
        self.incr_by(1);
    }

    /// Decrements the gauge by 1.
    ///
    /// # Safety
    /// The gauge value after decrement could underflow.
    ///
    /// See [`StaticGauge::incr_by`] for more information.
    #[inline(always)]
    unsafe fn decr(&self) {
        self.decr_by(1);
    }

    /// Increments the gauge by a given value and returns a [`GaugeHandleOwned`] which will automatically decrement
    /// the gauge by that value when dropped.
    #[inline(always)]
    fn acquire_many_owned(&self, value: u64) -> GaugeHandleOwned<Many> {
        unsafe { self.incr_by(value) };

        GaugeHandleOwned {
            value: Many(value),
            inner: self.inner_arc(),
        }
    }

    /// Increments the gauge by 1 and returns a [`GaugeHandleOwned`] which will automatically decrement
    /// the gauge when dropped.
    #[inline(always)]
    fn acquire_owned(&self) -> GaugeHandleOwned<One> {
        unsafe { self.incr() };

        GaugeHandleOwned {
            value: One,
            inner: self.inner_arc(),
        }
    }

    /// Sets the gauge to a given value. This method is useful when you have a background task that periodically
    /// reports a given value.
    #[inline(always)]
    fn set(&self, value: u64) {
        self.inner().set(value);
    }
}

#[derive(Default)]
pub struct StaticGaugeWrapper<Gauge: StaticGauge>(Gauge);

impl<Gauge: StaticGauge> std::ops::Deref for StaticGaugeWrapper<Gauge> {
    type Target = Gauge;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Gauge: StaticGauge> Debug for StaticGaugeWrapper<Gauge> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = self.inner();
        let current = value.current();
        f.debug_struct("StaticGauge")
            .field("name", &Gauge::NAME)
            .field("static_tags", &Gauge::STATIC_TAGS)
            .field("current", &current)
            .field("min", &value.min(current))
            .field("max", &value.max(current))
            .finish()
    }
}

#[async_trait]
impl<Gauge: StaticGauge> EmitMetrics for StaticGaugeWrapper<Gauge> {
    async fn emit_metrics(&self, metrics: &SharedMetrics) {
        self.inner()
            .snapshot()
            .emit::<Gauge, _>(metrics, |builder| builder);
    }
}
