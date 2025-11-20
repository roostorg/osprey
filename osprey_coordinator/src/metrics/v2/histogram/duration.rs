use std::collections::HashMap;
use std::convert::TryFrom;
use std::num::NonZeroU64;
use std::time::Duration;

use tracing::warn;

use crate::metrics::v2::{BaseMetric, TagKey};

use super::{Histogram, HistogramBuilder, HistogramSnapshot};

/// A [`Histogram`] preconfigured for handling timing measurements.
///
/// # Example
///
/// ```
/// # use std::time::Duration;
/// # use crate::metrics::v2::{DurationHistogram, DurationResolution};
/// let histogram = DurationHistogram::<()>::new(DurationResolution::Milliseconds);
/// histogram.record((), Duration::from_millis(3142));
/// histogram.record((), Duration::from_secs(27));
/// ```
#[derive(Debug)]
pub struct DurationHistogram<K> {
    resolution: DurationResolution,
    inner: Histogram<K>,
}

impl<K: TagKey> DurationHistogram<K> {
    /// Returns a new histogram with the given resolution.
    pub fn new(resolution: DurationResolution) -> Self {
        let high = match resolution {
            // [1 msec..1 hour]
            DurationResolution::Milliseconds => 60 * 60 * 1_000,
            // [1 usec..10 minutes]
            DurationResolution::Microseconds => 10 * 60 * 1_000_000,
        };
        DurationHistogram {
            resolution,
            inner: HistogramBuilder::new()
                .range(
                    NonZeroU64::new(1).expect("constant"),
                    NonZeroU64::new(high).expect("constant"),
                )
                .sigfig(2)
                .create(),
        }
    }

    /// Records a value in the histogram.
    pub fn record(&self, key: K, duration: Duration) {
        let v128 = match self.resolution {
            DurationResolution::Milliseconds => duration.as_millis(),
            DurationResolution::Microseconds => duration.as_micros(),
        };
        match u64::try_from(v128) {
            Ok(v) => {
                self.inner.record(key, v);
            }
            Err(_) => {
                warn!(
                    "Unable to store duration {:?} in histogram at {:?} resolution",
                    duration, self.resolution
                );
            }
        }
    }

    /// Reads the current values of the metrics.
    /// This is primarily intended for testing.
    /// The integers will be in units of the histogram's resolution.
    #[inline]
    pub fn snapshot(&self) -> HashMap<K, HistogramSnapshot>
    where
        K: Clone,
    {
        self.inner.snapshot()
    }
}

// Manual implementation because we do not require `K: Default`.
impl<K: TagKey> Default for DurationHistogram<K> {
    /// Returns a histogram with `Milliseconds` resolution.
    fn default() -> Self {
        DurationHistogram::new(DurationResolution::default())
    }
}

impl<K: TagKey> BaseMetric for DurationHistogram<K> {
    fn send_metric(
        &self,
        client: &cadence::StatsdClient,
        name: &str,
        base_tags: &mut dyn Iterator<Item = (&str, &str)>,
    ) -> Result<(), cadence::MetricError> {
        self.inner.send_metric(client, name, base_tags)
    }
}

/// The resolution of a [`DurationHistogram`].
/// Lower values are less precise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
#[non_exhaustive]
pub enum DurationResolution {
    Milliseconds,
    Microseconds,
}

impl Default for DurationResolution {
    /// Returns `Milliseconds`.
    fn default() -> Self {
        DurationResolution::Milliseconds
    }
}

#[cfg(test)]
mod tests {
    use std::iter;

    use cadence::{SpyMetricSink, StatsdClient};

    use super::*;
    use crate::metrics::v2::statsd_output::StatsdOutput;

    #[test]
    fn test_milliseconds() {
        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let histogram = DurationHistogram::<()>::new(DurationResolution::Milliseconds);

        histogram.record((), Duration::from_millis(42));
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
    fn test_microseconds() {
        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let histogram = DurationHistogram::<()>::new(DurationResolution::Microseconds);

        histogram.record((), Duration::from_micros(42));
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
}
