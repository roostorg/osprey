use std::{
    collections::HashMap,
    sync::atomic::{AtomicI64, Ordering},
};

use cadence::Counted;
use fxhash::FxHashMap;
use parking_lot::RwLock;

use crate::metrics::v2::{send_submetric, BaseMetric, TagKey};

/// An integer counter.
#[derive(Debug)]
pub struct Counter<K> {
    storage: RwLock<FxHashMap<K, AtomicI64>>,
}

impl<K: TagKey> Counter<K> {
    #[inline]
    pub fn new() -> Self {
        Counter {
            storage: Default::default(),
        }
    }

    /// Increments the counter by one.
    ///
    /// # Example
    ///
    /// ```
    /// # use crate::metrics::v2::Counter;
    /// let counter = Counter::<()>::new();
    /// counter.incr(());
    /// assert_eq!(counter.snapshot().get(&()).copied(), Some(1));
    /// ```
    #[inline]
    pub fn incr(&self, key: K) {
        self.incr_by(key, 1);
    }

    /// Increments the counter by the given value.
    pub fn incr_by(&self, key: K, value: i64) {
        if value == 0 {
            return;
        }

        // Fast path: key we've already seen before.
        {
            let locked = self.storage.read();
            if let Some(counter) = locked.get(&key) {
                counter.fetch_add(value, Ordering::Relaxed);
                return;
            }
        }

        let mut locked = self.storage.write();
        locked
            .entry(key)
            .and_modify(|counter| {
                counter.fetch_add(value, Ordering::Relaxed);
            })
            .or_insert(AtomicI64::new(value));
    }

    // Decrements the counter by one.
    #[inline]
    pub fn decr(&self, key: K) {
        self.incr_by(key, -1);
    }

    // Decrements the counter by the given value.
    #[inline]
    pub fn decr_by(&self, key: K, value: i64) {
        self.incr_by(key, -value);
    }

    /// Reads the current values of the metrics.
    /// This is primarily intended for testing
    /// and may have eventually consistent results
    /// unless explicit synchronization is used.
    ///
    /// # Example
    ///
    /// ```
    /// # use crate::metrics::v2::Counter;
    /// let counter = Counter::<()>::new();
    /// counter.incr(());
    /// assert_eq!(counter.snapshot().get(&()).copied(), Some(1));
    /// ```
    pub fn snapshot(&self) -> HashMap<K, i64>
    where
        K: Clone,
    {
        let storage = self.storage.read();
        storage
            .iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect()
    }
}

// Manual implementation because we do not require `K: Default`.
impl<K: TagKey> Default for Counter<K> {
    fn default() -> Self {
        Counter::new()
    }
}

impl<K: TagKey> BaseMetric for Counter<K> {
    fn send_metric(
        &self,
        client: &cadence::StatsdClient,
        name: &str,
        base_tags: &mut dyn Iterator<Item = (&str, &str)>,
    ) -> Result<(), cadence::MetricError> {
        let storage = self.storage.read();
        let base_tags: Vec<_> = base_tags.collect();
        for (key, counter) in storage.iter() {
            let count = counter.swap(0, Ordering::Relaxed);
            if count == 0 {
                continue;
            }

            send_submetric(
                client.count_with_tags(name, count),
                base_tags.iter().copied(),
                key,
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::iter;

    use cadence::{SpyMetricSink, StatsdClient};

    use super::*;
    use crate::metrics::v2::statsd_output::StatsdOutput;

    #[test]
    fn counter_send_zero() {
        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let counter = Counter::<()>::new();

        counter
            .send_metric(&client, "foo", &mut iter::empty())
            .unwrap();
        drop(client); // close the sending end

        let got = StatsdOutput::from_packets(receiver);
        assert_eq!(got, StatsdOutput::new());
    }

    #[test]
    fn counter_send_one() {
        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let counter = Counter::<()>::new();

        counter.incr(());
        counter
            .send_metric(&client, "foo", &mut iter::empty())
            .unwrap();
        drop(client); // close the sending end

        let got = StatsdOutput::from_packets(receiver);
        assert_eq!(got, StatsdOutput::from([(b"foo:1|c".to_vec(), 1),]));
    }

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    struct TestKey(&'static str, &'static str);

    impl TagKey for TestKey {
        type TagIterator<'a> = iter::Once<(&'a str, &'a str)>;

        fn tags<'a>(&'a self) -> Self::TagIterator<'a> {
            iter::once((self.0, self.1))
        }
    }

    #[test]
    fn counter_send_tagged() {
        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let counter = Counter::<TestKey>::new();

        counter.incr(TestKey("mykey", "xyzzy"));
        counter
            .send_metric(&client, "foo", &mut iter::empty())
            .unwrap();
        drop(client); // close the sending end

        let got = StatsdOutput::from_packets(receiver);
        assert_eq!(
            got,
            StatsdOutput::from([(b"foo:1|c|#mykey:xyzzy".to_vec(), 1),])
        );
    }

    #[test]
    fn counter_send_multiple_tagged_with_base() {
        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let counter = Counter::<TestKey>::new();
        let base_tags = &[("base", "ball")];

        counter.incr(TestKey("mykey", "xyzzy"));
        counter.incr(TestKey("mykey", "another"));
        counter
            .send_metric(&client, "foo", &mut base_tags.iter().copied())
            .unwrap();
        drop(client); // close the sending end

        let got = StatsdOutput::from_packets(receiver);
        assert_eq!(
            got,
            StatsdOutput::from([
                (b"foo:1|c|#base:ball,mykey:xyzzy".to_vec(), 1),
                (b"foo:1|c|#base:ball,mykey:another".to_vec(), 1)
            ])
        );
    }
}
