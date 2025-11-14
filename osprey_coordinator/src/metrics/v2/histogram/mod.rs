mod duration;

use std::num::NonZeroU64;
use std::{collections::HashMap, convert::TryFrom};

use cadence::{Counted, Gauged};
use fxhash::FxHashMap;
use hdrhistogram::{Counter, Histogram as HdrHistogram};
use parking_lot::{Mutex, RwLock};
use tracing::warn;

use crate::metrics::v2::{send_submetric, BaseMetric, TagKey};

pub use self::duration::*;

// Grouped metrics for a histogram at a particular instant.
#[derive(Clone, Debug)]
pub struct HistogramSnapshot {
    sample_count: NonZeroU64,
    min: u64,
    max: u64,
    mean: f64,
    median: u64,
    p95: u64,
    p99: u64,
}

impl HistogramSnapshot {
    fn from_histogram<T: Counter>(h: &HdrHistogram<T>) -> Option<Self> {
        if let Some(sample_count) = NonZeroU64::new(h.len()) {
            Some(HistogramSnapshot {
                sample_count,
                min: h.min(),
                max: h.max(),
                mean: h.mean(),
                median: h.value_at_quantile(0.5),
                p95: h.value_at_quantile(0.95),
                p99: h.value_at_quantile(0.99),
            })
        } else {
            None
        }
    }

    /// Returns the number of values recorded
    /// since the last send of the histogram.
    #[inline]
    pub fn sample_count(&self) -> NonZeroU64 {
        self.sample_count
    }

    /// Returns the minimum
    /// of values recorded since the last send of the histogram.
    #[inline]
    pub fn min(&self) -> u64 {
        self.min
    }

    /// Returns the maximum
    /// of values recorded since the last send of the histogram.
    #[inline]
    pub fn max(&self) -> u64 {
        self.max
    }

    /// Returns the arithmetic mean (average)
    /// of values recorded since the last send of the histogram.
    #[inline]
    pub fn mean(&self) -> f64 {
        self.mean
    }

    /// Returns the median (50th percentile)
    /// of values recorded since the last send of the histogram.
    #[inline]
    pub fn median(&self) -> u64 {
        self.median
    }

    /// Returns the 95th percentile
    /// of values recorded since the last send of the histogram.
    #[inline]
    pub fn p95(&self) -> u64 {
        self.p95
    }

    /// Returns the 99th percentile
    /// of values recorded since the last send of the histogram.
    #[inline]
    pub fn p99(&self) -> u64 {
        self.p99
    }
}

/// A metric that reports on a distribution of `u64` values.
/// For a base name of `foo`, this type will send seven metrics to Datadog:
///
/// - `foo.count`: `COUNT` of the number of samples
/// - `foo.min`: `GAUGE` of the lowest value observed since the last send.
/// - `foo.max`: `GAUGE` of the highest value observed since the last send.
/// - `foo.avg`: `GAUGE` of the average value observed since the last send.
/// - `foo.median`: `GAUGE` of the median value observed since the last send.
/// - `foo.95percentile`: `GAUGE` of the P95 value observed since the last send.
/// - `foo.99percentile`: `GAUGE` of the P99 value observed since the last send.
///
/// # Example
///
/// ```
/// # use crate::metrics::v2::Histogram;
/// let histogram = Histogram::<()>::new();
/// histogram.record((), 3142);
/// histogram.record((), 2718);
/// ```
#[derive(Debug)]
pub struct Histogram<K> {
    options: HistogramBuilder,
    storage: RwLock<FxHashMap<K, Mutex<HdrHistogram<u64>>>>,
}

impl<K: TagKey> Histogram<K> {
    /// Returns a new histogram with the default settings.
    /// If you want to customize the precision, use [`HistogramBuilder`].
    pub fn new() -> Self {
        HistogramBuilder::new().create()
    }

    /// Records a value in the histogram.
    pub fn record(&self, key: K, value: u64) {
        // Fast path: key we've already seen before.
        {
            let locked = self.storage.read();
            if let Some(histogram_mutex) = locked.get(&key) {
                if let Err(err) = histogram_mutex.lock().record(value) {
                    warn!("Failed to record histogram value: {}", err);
                }
                return;
            }
        }

        // Creating the histogram outside the critical section
        // because we allocate (and can panic if we have a bug)
        // without affecting others.
        let mut new_histogram = HdrHistogram::new_with_bounds(
            self.options.low.into(),
            self.options.high.into(),
            self.options.sigfig,
        )
        .expect("builder should have validated parameters");
        if let Err(err) = new_histogram.record(value) {
            warn!("Failed to record histogram value: {}", err);
            return;
        }

        let mut locked = self.storage.write();
        locked
            .entry(key)
            .and_modify(|histogram_mutex| {
                if let Err(err) = histogram_mutex.lock().record(value) {
                    warn!("Failed to record histogram value: {}", err);
                }
            })
            .or_insert(Mutex::new(new_histogram));
    }

    /// Reads the current values of the metrics.
    /// This is primarily intended for testing.
    ///
    /// # Example
    ///
    /// ```
    /// # use crate::metrics::v2::Histogram;
    /// let histogram = Histogram::<()>::new();
    /// histogram.record((), 42);
    /// assert_eq!(histogram.snapshot().get(&()).map(|x| x.p95()), Some(42));
    /// ```
    pub fn snapshot(&self) -> HashMap<K, HistogramSnapshot>
    where
        K: Clone,
    {
        let storage = self.storage.read();
        storage
            .iter()
            .filter_map(|(k, mu)| {
                HistogramSnapshot::from_histogram(&mu.lock()).map(|v| (k.clone(), v))
            })
            .collect()
    }
}

// Manual implementation because we do not require `K: Default`.
impl<K: TagKey> Default for Histogram<K> {
    fn default() -> Self {
        Histogram::new()
    }
}

impl<K: TagKey> BaseMetric for Histogram<K> {
    fn send_metric(
        &self,
        client: &cadence::StatsdClient,
        name: &str,
        base_tags: &mut dyn Iterator<Item = (&str, &str)>,
    ) -> Result<(), cadence::MetricError> {
        let storage = self.storage.read();
        let base_tags: Vec<_> = base_tags.collect();
        for (key, histogram_mutex) in storage.iter() {
            let snapshot = {
                let mut histogram = histogram_mutex.lock();
                let snapshot = HistogramSnapshot::from_histogram(&histogram);
                histogram.reset();
                snapshot
            };
            let snapshot = match snapshot {
                Some(snapshot) => snapshot,
                None => continue,
            };
            let sample_count =
                i64::try_from(u64::from(snapshot.sample_count())).unwrap_or(i64::max_value());
            send_submetric(
                client.count_with_tags(&format!("{}.count", name), sample_count),
                base_tags.iter().copied(),
                key,
            );
            send_submetric(
                client.gauge_with_tags(&format!("{}.avg", name), snapshot.mean()),
                base_tags.iter().copied(),
                key,
            );
            send_submetric(
                client.gauge_with_tags(&format!("{}.median", name), snapshot.median()),
                base_tags.iter().copied(),
                key,
            );
            send_submetric(
                client.gauge_with_tags(&format!("{}.95percentile", name), snapshot.p95()),
                base_tags.iter().copied(),
                key,
            );
            send_submetric(
                client.gauge_with_tags(&format!("{}.99percentile", name), snapshot.p99()),
                base_tags.iter().copied(),
                key,
            );
            send_submetric(
                client.gauge_with_tags(&format!("{}.min", name), snapshot.min()),
                base_tags.iter().copied(),
                key,
            );
            send_submetric(
                client.gauge_with_tags(&format!("{}.max", name), snapshot.max()),
                base_tags.iter().copied(),
                key,
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct HistogramBuilder {
    low: NonZeroU64,
    high: NonZeroU64,
    sigfig: u8,
}

impl HistogramBuilder {
    /// Creates a builder with the default options.
    #[inline]
    pub fn new() -> Self {
        // Values recommended in
        // https://docs.rs/hdrhistogram/7.5.2/hdrhistogram/struct.Histogram.html#method.new_with_bounds
        HistogramBuilder {
            low: NonZeroU64::new(1).expect("constant"),
            high: NonZeroU64::new(u64::max_value()).expect("constant"),
            sigfig: 2,
        }
    }

    /// Sets the range of the histogram's values.
    /// `low` is the lowest value that can be distinguished from zero.
    /// `high` is the highest value to be tracked and must be `>= 2 * low`.
    ///
    /// # Panics
    ///
    /// If `high` is not sufficiently larger than `low`.
    pub fn range(&mut self, low: NonZeroU64, high: NonZeroU64) -> &mut Self {
        assert!(
            low.checked_mul(NonZeroU64::new(2).expect("constant"))
                .map(|low2| high >= low2)
                .unwrap_or(false),
            "Maximum value of histogram must be at least 2x the minimum."
        );
        self.low = low;
        self.high = high;
        self
    }

    /// Sets the number of significant digits (after the decimal place)
    /// to which the histogram will maintain value resolution and separation.
    /// This will increase memory usage exponentially, so be careful.
    ///
    /// # Panics
    ///
    /// If `sigfig` is greater than 5.
    pub fn sigfig(&mut self, sigfig: u8) -> &mut Self {
        assert!(sigfig <= 5, "Histogram precision too large");
        self.sigfig = sigfig;
        self
    }

    /// Returns a new histogram with the builder's parameters.
    pub fn create<K: TagKey>(&self) -> Histogram<K> {
        Histogram {
            options: self.clone(),
            storage: Default::default(),
        }
    }
}

impl Default for HistogramBuilder {
    fn default() -> Self {
        HistogramBuilder::new()
    }
}

#[cfg(test)]
mod tests {
    use std::iter;

    use cadence::{SpyMetricSink, StatsdClient};

    use super::*;
    use crate::metrics::v2::statsd_output::StatsdOutput;

    #[test]
    fn histogram_send_nothing() {
        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let histogram = Histogram::<()>::new();

        histogram
            .send_metric(&client, "foo", &mut iter::empty())
            .unwrap();
        drop(client); // close the sending end

        let got = StatsdOutput::from_packets(receiver);
        assert_eq!(got, StatsdOutput::new());
    }

    #[test]
    fn histogram_set_once() {
        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let histogram = Histogram::<()>::new();

        histogram.record((), 42);
        histogram
            .send_metric(&client, "foo", &mut iter::empty())
            .unwrap();
        drop(client); // close the sending end

        let got = StatsdOutput::from_packets(receiver);
        assert_eq!(
            got,
            StatsdOutput::from([
                (b"foo.count:1|c".to_vec(), 1),
                (b"foo.min:42|g".to_vec(), 1),
                (b"foo.max:42|g".to_vec(), 1),
                (b"foo.avg:42|g".to_vec(), 1),
                (b"foo.median:42|g".to_vec(), 1),
                (b"foo.95percentile:42|g".to_vec(), 1),
                (b"foo.99percentile:42|g".to_vec(), 1),
            ])
        );
    }

    #[test]
    fn histogram_clears_on_send() {
        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let histogram = Histogram::<()>::new();

        histogram.record((), 42);
        histogram
            .send_metric(&client, "foo", &mut iter::empty())
            .unwrap();
        drop(client); // close the sending end

        let got = StatsdOutput::from_packets(receiver);
        assert_eq!(
            got,
            StatsdOutput::from([
                (b"foo.count:1|c".to_vec(), 1),
                (b"foo.min:42|g".to_vec(), 1),
                (b"foo.max:42|g".to_vec(), 1),
                (b"foo.avg:42|g".to_vec(), 1),
                (b"foo.median:42|g".to_vec(), 1),
                (b"foo.95percentile:42|g".to_vec(), 1),
                (b"foo.99percentile:42|g".to_vec(), 1),
            ])
        );

        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);

        histogram.record((), 10);
        histogram
            .send_metric(&client, "foo", &mut iter::empty())
            .unwrap();
        drop(client); // close the sending end

        let got = StatsdOutput::from_packets(receiver);
        assert_eq!(
            got,
            StatsdOutput::from([
                (b"foo.count:1|c".to_vec(), 1),
                (b"foo.min:10|g".to_vec(), 1),
                (b"foo.max:10|g".to_vec(), 1),
                (b"foo.avg:10|g".to_vec(), 1),
                (b"foo.median:10|g".to_vec(), 1),
                (b"foo.95percentile:10|g".to_vec(), 1),
                (b"foo.99percentile:10|g".to_vec(), 1),
            ])
        );
    }

    #[test]
    fn histogram_set_twice() {
        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let histogram = Histogram::<()>::new();

        histogram.record((), 42);
        histogram.record((), 100);
        histogram
            .send_metric(&client, "foo", &mut iter::empty())
            .unwrap();
        drop(client); // close the sending end

        let got = StatsdOutput::from_packets(receiver);
        assert_eq!(
            got,
            StatsdOutput::from([
                (b"foo.count:2|c".to_vec(), 1),
                (b"foo.min:42|g".to_vec(), 1),
                (b"foo.max:100|g".to_vec(), 1),
                (b"foo.median:42|g".to_vec(), 1),
                (b"foo.avg:71|g".to_vec(), 1),
                (b"foo.95percentile:100|g".to_vec(), 1),
                (b"foo.99percentile:100|g".to_vec(), 1),
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
    fn histogram_send_tagged() {
        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let histogram = Histogram::<TestKey>::new();

        histogram.record(TestKey("mykey", "xyzzy"), 42);
        histogram
            .send_metric(&client, "foo", &mut iter::empty())
            .unwrap();
        drop(client); // close the sending end

        let got = StatsdOutput::from_packets(receiver);
        assert_eq!(
            got,
            StatsdOutput::from([
                (b"foo.count:1|c|#mykey:xyzzy".to_vec(), 1),
                (b"foo.min:42|g|#mykey:xyzzy".to_vec(), 1),
                (b"foo.max:42|g|#mykey:xyzzy".to_vec(), 1),
                (b"foo.avg:42|g|#mykey:xyzzy".to_vec(), 1),
                (b"foo.median:42|g|#mykey:xyzzy".to_vec(), 1),
                (b"foo.95percentile:42|g|#mykey:xyzzy".to_vec(), 1),
                (b"foo.99percentile:42|g|#mykey:xyzzy".to_vec(), 1),
            ])
        );
    }

    #[test]
    fn histogram_send_multiple_tagged_with_base() {
        let (receiver, sink) = SpyMetricSink::with_capacity(100);
        let client = StatsdClient::from_sink("", sink);
        let histogram = Histogram::<TestKey>::new();
        let base_tags = &[("base", "ball")];

        histogram.record(TestKey("mykey", "xyzzy"), 42);
        histogram.record(TestKey("mykey", "another"), 100);
        histogram
            .send_metric(&client, "foo", &mut base_tags.iter().copied())
            .unwrap();
        drop(client); // close the sending end

        let got = StatsdOutput::from_packets(receiver);
        assert_eq!(
            got,
            StatsdOutput::from([
                (b"foo.count:1|c|#base:ball,mykey:xyzzy".to_vec(), 1),
                (b"foo.min:42|g|#base:ball,mykey:xyzzy".to_vec(), 1),
                (b"foo.max:42|g|#base:ball,mykey:xyzzy".to_vec(), 1),
                (b"foo.avg:42|g|#base:ball,mykey:xyzzy".to_vec(), 1),
                (b"foo.median:42|g|#base:ball,mykey:xyzzy".to_vec(), 1),
                (b"foo.95percentile:42|g|#base:ball,mykey:xyzzy".to_vec(), 1),
                (b"foo.99percentile:42|g|#base:ball,mykey:xyzzy".to_vec(), 1),
                (b"foo.count:1|c|#base:ball,mykey:another".to_vec(), 1),
                (b"foo.min:100|g|#base:ball,mykey:another".to_vec(), 1),
                (b"foo.max:100|g|#base:ball,mykey:another".to_vec(), 1),
                (b"foo.avg:100|g|#base:ball,mykey:another".to_vec(), 1),
                (b"foo.median:100|g|#base:ball,mykey:another".to_vec(), 1),
                (
                    b"foo.95percentile:100|g|#base:ball,mykey:another".to_vec(),
                    1
                ),
                (
                    b"foo.99percentile:100|g|#base:ball,mykey:another".to_vec(),
                    1
                ),
            ])
        );
    }
}
