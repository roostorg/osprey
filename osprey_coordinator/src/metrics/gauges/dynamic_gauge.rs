use std::{collections::hash_map::Entry, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use fxhash::FxHashMap;
use parking_lot::RwLock;

use super::gauge_struct::{GaugeHandleOwned, GaugeInner, Many, One};
use crate::metrics::base_metric::{BaseMetric, DynamicTagKey, EmitMetrics};
use crate::metrics::SharedMetrics;

pub type DynamicGaugeStorage<Key> = RwLock<FxHashMap<Key, Arc<GaugeInner>>>;

/// A dynamic gauge holds gauges for a key/value pair for a singular metric.
pub trait DynamicGauge: BaseMetric {
    type Key: DynamicTagKey;

    #[doc(hidden)]
    fn storage(&self) -> &DynamicGaugeStorage<Self::Key>;

    #[doc(hidden)]
    #[inline(always)]
    unsafe fn decr_with_key(&self, key: Self::Key, value: u64) {
        if value == 0 {
            return;
        }

        let gauge_inner = get_gauge_inner_for_key(self, key);
        gauge_inner.decr_by(value);
    }

    #[doc(hidden)]
    #[inline(always)]
    unsafe fn incr_with_key(&self, key: Self::Key, value: u64) {
        if value == 0 {
            return;
        }

        let gauge_inner = get_gauge_inner_for_key(self, key);
        gauge_inner.incr_by(value);
    }

    #[doc(hidden)]
    #[inline(always)]
    fn acquire_many_with_key_owned(&self, key: Self::Key, value: u64) -> GaugeHandleOwned<Many> {
        let gauge_inner = get_gauge_inner_for_key(self, key);
        gauge_inner.incr_by(value);
        GaugeHandleOwned {
            value: Many(value),
            inner: Arc::clone(&gauge_inner),
        }
    }

    #[doc(hidden)]
    #[inline(always)]
    fn acquire_with_key_owned(&self, key: Self::Key) -> GaugeHandleOwned<One> {
        let gauge_inner = get_gauge_inner_for_key(self, key);
        gauge_inner.incr_by(1);
        GaugeHandleOwned {
            value: One,
            inner: Arc::clone(&gauge_inner),
        }
    }

    #[doc(hidden)]
    #[inline(always)]
    fn set_with_key(&self, key: Self::Key, value: u64) {
        let gauge_inner = get_gauge_inner_for_key(self, key);
        gauge_inner.set(value);
    }
}

#[inline(always)]
fn get_gauge_inner_for_key<T: DynamicGauge>(
    dynamic_gauge: &T,
    key: <T as DynamicGauge>::Key,
) -> Arc<GaugeInner> {
    let storage = dynamic_gauge.storage();

    // First, try the *fast* path, for a dynamic key pair we've seen before.
    let locked = storage.read();
    if let Some(gauge) = locked.get(&key) {
        return Arc::clone(gauge);
    }
    drop(locked);

    // Second, try the *slow* path, which is that we have not discovered this dynamic key pair before,
    // so we must insert it into the storage.
    let mut locked = storage.write();
    match locked.entry(key) {
        Entry::Occupied(ent) => Arc::clone(ent.get()),
        Entry::Vacant(ent) => Arc::clone(ent.insert(Arc::new(GaugeInner::default()))),
    }
}
/// A wrapper that disambiguates between static and dynamic gauges for the purpose
/// of the EmitMetrics auto trait impl.
#[derive(Default)]
pub struct DynamicGaugeWrapper<Gauge: DynamicGauge>(Gauge);

impl<Gauge: DynamicGauge> std::ops::Deref for DynamicGaugeWrapper<Gauge> {
    type Target = Gauge;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Gauge: DynamicGauge> Debug for DynamicGaugeWrapper<Gauge> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicGauge")
            .field("name", &Gauge::NAME)
            .field("static_tags", &Gauge::STATIC_TAGS)
            .field("dynamic_tag_keys", &Gauge::Key::get_tag_keys())
            .field("num_distinct_keys", &self.storage().read().len())
            .finish()
    }
}

#[async_trait]
impl<Gauge> EmitMetrics for DynamicGaugeWrapper<Gauge>
where
    Gauge: DynamicGauge,
{
    async fn emit_metrics(&self, metrics: &SharedMetrics) {
        let storage = self.storage();
        let locked = storage.read();
        for (key, value) in locked.iter() {
            value.snapshot().emit::<Gauge, _>(metrics, |builder| {
                key.fold_tag_pairs(builder, |b, k, v| b.with_tag(k, v))
            });
        }
    }
}
