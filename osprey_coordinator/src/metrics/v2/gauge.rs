use std::{
    collections::HashMap,
    ops::{Add, Sub},
};

use cadence::Gauged;
use fxhash::FxHashMap;
use parking_lot::{Mutex, RwLock};

use crate::metrics::v2::{send_submetric, BaseMetric, TagKey};

use self::private::Sealed;

/// The type of a gauge: either a `u64` or an `f64`.
/// This trait is sealed and not meant to be implemented by other crates.
pub trait GaugeValue:
    cadence::ext::ToGaugeValue + Add + Sub + PartialEq + PartialOrd + Copy + Sealed
{
}

impl GaugeValue for u64 {}
impl GaugeValue for f64 {}

// Grouped metrics for a gauge at a particular instant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GaugeSnapshot<V> {
    current: V,
    min: V,
    max: V,
}

impl<V: GaugeValue + Add<Output = V> + Sub<Output = V>> GaugeSnapshot<V> {
    fn new(v: V) -> Self {
        GaugeSnapshot {
            current: v,
            min: v,
            max: v,
        }
    }

    fn set(&mut self, v: V) {
        self.current = v;
        if v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }
    }

    fn incr(&mut self, v: V) {
        self.set(self.current.add(v));
    }

    fn decr(&mut self, v: V) {
        self.set(self.current.sub(v))
    }

    /// Returns the most recently set value of the gauge.
    pub fn current(&self) -> V {
        self.current
    }

    /// Returns the low watermark for the gauge.
    pub fn min(&self) -> V {
        self.min
    }

    /// Returns the high watermark for the gauge.
    pub fn max(&self) -> V {
        self.max
    }
}

/// A metric that is reported continuously,
/// like disk space or memory used.
/// For a base name of `foo`, this type will send three gauges to Datadog:
///
/// - `foo`: the most recent value of `foo`
/// - `foo.min`: the lowest value of `foo` observed since the gauge's creation
/// - `foo.max`: the highest value of `foo` observed since the gauge's creation
///
/// # Example
///
/// ```
/// # use crate::metrics::v2::Gauge;
/// let gauge = Gauge::<(), f64>::new();
/// gauge.set((), 3.142);
/// gauge.set((), 2.718);
/// ```
#[derive(Debug)]
pub struct Gauge<K, V> {
    storage: RwLock<FxHashMap<K, Mutex<GaugeSnapshot<V>>>>,
}

impl<K: TagKey, V: GaugeValue + Add<Output = V> + Sub<Output = V>> Gauge<K, V> {
    #[inline]
    pub fn new() -> Self {
        Gauge {
            storage: Default::default(),
        }
    }

    pub fn set(&self, key: K, value: V) {
        // Fast path: key we've already seen before.
        {
            let locked = self.storage.read();
            if let Some(snapshot) = locked.get(&key) {
                snapshot.lock().set(value);
                return;
            }
        }

        let mut locked = self.storage.write();
        locked
            .entry(key)
            .and_modify(|snapshot| {
                snapshot.lock().set(value);
            })
            .or_insert(Mutex::new(GaugeSnapshot::new(value)));
    }

    pub fn incr(&self, key: K, value: V) {
        {
            let locked = self.storage.read();
            if let Some(snapshot) = locked.get(&key) {
                snapshot.lock().incr(value);
                return;
            }
        }

        let mut locked = self.storage.write();
        locked
            .entry(key)
            .and_modify(|snapshot| {
                snapshot.lock().incr(value);
            })
            .or_insert(Mutex::new(GaugeSnapshot::new(value)));
    }

    pub fn decr(&self, key: K, value: V) {
        {
            let locked = self.storage.read();
            if let Some(snapshot) = locked.get(&key) {
                snapshot.lock().decr(value);
                return;
            }
        }

        let mut locked = self.storage.write();
        locked
            .entry(key)
            .and_modify(|snapshot| {
                snapshot.lock().decr(value);
            })
            .or_insert(Mutex::new(GaugeSnapshot::new(value)));
    }

    /// Reads the current values of the metrics.
    /// This is primarily intended for testing.
    ///
    /// # Example
    ///
    /// ```
    /// # use crate::metrics::v2::Gauge;
    /// let gauge = Gauge::<(), u64>::new();
    /// gauge.set((), 42);
    /// assert_eq!(gauge.snapshot().get(&()).map(|x| x.current()), Some(42));
    /// ```
    pub fn snapshot(&self) -> HashMap<K, GaugeSnapshot<V>>
    where
        K: Clone,
    {
        let storage = self.storage.read();
        storage
            .iter()
            .map(|(k, v)| (k.clone(), v.lock().clone()))
            .collect()
    }
}

// Manual implementation because we do not require `K: Default` nor `V: Default`.
impl<K: TagKey, V: GaugeValue + Add<Output = V> + Sub<Output = V>> Default for Gauge<K, V> {
    fn default() -> Self {
        Gauge::new()
    }
}

impl<K: TagKey, V: GaugeValue + Add<Output = V> + Sub<Output = V>> BaseMetric for Gauge<K, V> {
    fn send_metric(
        &self,
        client: &cadence::StatsdClient,
        name: &str,
        base_tags: &mut dyn Iterator<Item = (&str, &str)>,
    ) -> Result<(), cadence::MetricError> {
        let storage = self.storage.read();
        let base_tags: Vec<_> = base_tags.collect();
        for (key, snapshot) in storage.iter() {
            let snapshot = snapshot.lock().clone();
            send_submetric(
                client.gauge_with_tags(name, snapshot.current()),
                base_tags.iter().copied(),
                key,
            );
            send_submetric(
                client.gauge_with_tags(&format!("{name}.min"), snapshot.min()),
                base_tags.iter().copied(),
                key,
            );
            send_submetric(
                client.gauge_with_tags(&format!("{name}.max"), snapshot.max()),
                base_tags.iter().copied(),
                key,
            );
        }
        Ok(())
    }
}

mod private {
    pub trait Sealed {}

    impl Sealed for u64 {}

    impl Sealed for f64 {}
}

#[cfg(test)]
mod tests {
    use std::iter;

    use cadence::{SpyMetricSink, StatsdClient};

    use super::*;
    use crate::metrics::v2::statsd_output::StatsdOutput;

    #[test]
    fn gauge_snapshot_set_many() {
        let mut snapshot = GaugeSnapshot::new(42u64);
        snapshot.set(1);
        snapshot.set(100);
        snapshot.set(40);
        assert_eq!(snapshot.current(), 40);
        assert_eq!(snapshot.min(), 1);
        assert_eq!(snapshot.max(), 100);
    }

    #[test]
    fn gauge_send_nothing() {
        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let gauge = Gauge::<(), u64>::new();

        gauge
            .send_metric(&client, "foo", &mut iter::empty())
            .unwrap();
        drop(client); // close the sending end

        let got = StatsdOutput::from_packets(receiver);
        assert_eq!(got, StatsdOutput::new());
    }

    #[test]
    fn gauge_set_once() {
        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let gauge = Gauge::<(), u64>::new();

        gauge.set((), 42);
        gauge
            .send_metric(&client, "foo", &mut iter::empty())
            .unwrap();
        drop(client); // close the sending end

        let got = StatsdOutput::from_packets(receiver);
        assert_eq!(
            got,
            StatsdOutput::from([
                (b"foo:42|g".to_vec(), 1),
                (b"foo.min:42|g".to_vec(), 1),
                (b"foo.max:42|g".to_vec(), 1),
            ])
        );
    }

    #[test]
    fn gauge_set_twice() {
        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let gauge = Gauge::<(), u64>::new();

        gauge.set((), 42);
        gauge.set((), 100);
        gauge
            .send_metric(&client, "foo", &mut iter::empty())
            .unwrap();
        drop(client); // close the sending end

        let got = StatsdOutput::from_packets(receiver);
        assert_eq!(
            got,
            StatsdOutput::from([
                (b"foo:100|g".to_vec(), 1),
                (b"foo.min:42|g".to_vec(), 1),
                (b"foo.max:100|g".to_vec(), 1),
            ])
        );
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
    fn gauge_send_tagged() {
        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let gauge = Gauge::<TestKey, u64>::new();

        gauge.set(TestKey("mykey", "xyzzy"), 42);
        gauge
            .send_metric(&client, "foo", &mut iter::empty())
            .unwrap();
        drop(client); // close the sending end

        let got = StatsdOutput::from_packets(receiver);
        assert_eq!(
            got,
            StatsdOutput::from([
                (b"foo:42|g|#mykey:xyzzy".to_vec(), 1),
                (b"foo.min:42|g|#mykey:xyzzy".to_vec(), 1),
                (b"foo.max:42|g|#mykey:xyzzy".to_vec(), 1),
            ])
        );
    }

    #[test]
    fn gauge_send_multiple_tagged_with_base() {
        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let gauge = Gauge::<TestKey, u64>::new();
        let base_tags = &[("base", "ball")];

        gauge.set(TestKey("mykey", "xyzzy"), 42);
        gauge.set(TestKey("mykey", "another"), 100);
        gauge
            .send_metric(&client, "foo", &mut base_tags.iter().copied())
            .unwrap();
        drop(client); // close the sending end

        let got = StatsdOutput::from_packets(receiver);
        assert_eq!(
            got,
            StatsdOutput::from([
                (b"foo:42|g|#base:ball,mykey:xyzzy".to_vec(), 1),
                (b"foo.min:42|g|#base:ball,mykey:xyzzy".to_vec(), 1),
                (b"foo.max:42|g|#base:ball,mykey:xyzzy".to_vec(), 1),
                (b"foo:100|g|#base:ball,mykey:another".to_vec(), 1),
                (b"foo.min:100|g|#base:ball,mykey:another".to_vec(), 1),
                (b"foo.max:100|g|#base:ball,mykey:another".to_vec(), 1)
            ])
        );
    }
}
