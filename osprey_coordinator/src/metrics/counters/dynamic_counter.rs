use std::{
    collections::hash_map::Entry,
    fmt::Debug,
    sync::atomic::{AtomicI64, Ordering},
};

use async_trait::async_trait;
use fxhash::FxHashMap;
use parking_lot::RwLock;

use crate::metrics::{BaseMetric, DynamicTagKey, EmitMetrics, SharedMetrics};

pub type DynamicCounterStorage<Key> = RwLock<FxHashMap<Key, AtomicI64>>;

/// A dynamic counter holds counters for a key/value pair for a singular metric.
pub trait DynamicCounter: BaseMetric {
    type Key: DynamicTagKey;

    #[doc(hidden)]
    fn storage(&self) -> &DynamicCounterStorage<Self::Key>;

    #[doc(hidden)]
    #[inline(always)]
    fn decr_with_key(&self, key: Self::Key, value: i64) {
        self.incr_with_key(key, -value);
    }

    #[doc(hidden)]
    #[inline(always)]
    fn incr_with_key(&self, key: Self::Key, value: i64) {
        if value == 0 {
            return;
        }

        let storage = self.storage();

        // First, try the *fast* path, for a dynamic key pair we've seen before.
        let locked = storage.read();
        if let Some(counter) = locked.get(&key) {
            counter.fetch_add(value, Ordering::Relaxed);
            return;
        }
        drop(locked);

        // Second, try the *slow* path, which is that we have not discovered this dynamic key pair before,
        // so we must insert it into the storage.
        let mut locked = storage.write();
        match locked.entry(key) {
            Entry::Occupied(ent) => {
                ent.get().fetch_add(value, Ordering::Relaxed);
            }
            Entry::Vacant(ent) => {
                ent.insert(AtomicI64::new(value));
            }
        }
    }
}

/// A wrapper that disambiguates between static and dynamic counters for the purpose
/// of the EmitMetrics auto trait impl.
#[derive(Default)]
pub struct DynamicCounterWrapper<Counter: DynamicCounter>(Counter);

impl<Counter: DynamicCounter> std::ops::Deref for DynamicCounterWrapper<Counter> {
    type Target = Counter;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Counter: DynamicCounter> Debug for DynamicCounterWrapper<Counter> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicCounter")
            .field("name", &Counter::NAME)
            .field("static_tags", &Counter::STATIC_TAGS)
            .field("dynamic_tag_keys", &Counter::Key::get_tag_keys())
            .field("num_distinct_keys", &self.storage().read().len())
            .finish()
    }
}

#[async_trait]
impl<Counter> EmitMetrics for DynamicCounterWrapper<Counter>
where
    Counter: DynamicCounter,
{
    async fn emit_metrics(&self, metrics: &SharedMetrics) {
        let storage = self.storage();
        let locked = storage.read();
        for (key, value) in locked.iter() {
            let count = value.swap(0, Ordering::Relaxed);
            if count == 0 {
                continue;
            }

            let mut builder = metrics.count_with_tags(Counter::NAME, count);
            builder = Counter::fold_static_tag_pairs(builder, |b, k, v| b.with_tag(k, v));
            builder = key.fold_tag_pairs(builder, |b, k, v| b.with_tag(k, v));
            builder.try_send().ok();
        }
    }
}
