use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use std::{borrow::Borrow, collections::hash_map::Entry};

use bytes_utils::Str;
use cadence::{MetricSink, StatsdClient};
use lazy_static::lazy_static;
use parking_lot::RwLock;
use thiserror::Error;
use tracing::error;

use crate::metrics::recycling_socket_metrics_sink::BufferedUdpMetricSink;
use crate::metrics::v2::MetricsEmitter;

const METRIC_REPORTING_FREQUENCY: Duration = Duration::from_millis(300);

lazy_static! {
    static ref EMITTERS: RwLock<HashMap<u128, Arc<dyn MetricsEmitter + Send + Sync + 'static>>> =
        RwLock::new(HashMap::new());
}

static SENDER_RUNNING: AtomicBool = AtomicBool::new(false);

/// Registers a metric emitter to be sent by [`MetricsSender`].
/// This is safe to call concurrently with a running [`MetricsSender`].
///
/// Panics if the same emitter is registered twice.
pub fn register_emitter(
    emitter: Arc<dyn MetricsEmitter + Send + Sync + 'static>,
) -> RegisteredEmitter {
    let id = unsafe { std::mem::transmute(Arc::as_ptr(&emitter)) };
    tracing::debug!("registered {id} as emitter");
    match EMITTERS.write().entry(id) {
        Entry::Occupied(_) => panic!("This emitter has already been registered."),
        Entry::Vacant(ent) => {
            ent.insert(emitter);
        }
    }
    RegisteredEmitter { id }
}

/// A handle to an emitter registed with [`register_emitter`].
/// Dropping the handle will cause the metrics to stop being emitted.
/// If this is undesirable, you can use [`RegisteredEmitter::emit_forever`]
/// to emit the metrics until the [`MetricsSender`] stops running.
#[derive(Debug)]
#[must_use]
pub struct RegisteredEmitter {
    id: u128,
}

impl RegisteredEmitter {
    /// Keep emitting metrics and consume this handle.
    pub fn emit_forever(mut self) {
        self.id = 0;
    }
}

impl Drop for RegisteredEmitter {
    fn drop(&mut self) {
        if self.id != 0 {
            tracing::debug!("dropping emitter: {}", self.id);
            EMITTERS.write().remove(&self.id);
        }
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum SendMetricsError {
    #[error("metrics are already being sent")]
    AlreadySending,
    #[error("{0}")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}

/// Builder for [`MetricsSender`].
#[derive(Debug, Default)]
pub struct MetricsSenderBuilder {
    client: Option<StatsdClient>,
    base_tags: Vec<(Str, Str)>,
    exclude_standard_tags: bool,
}

impl MetricsSenderBuilder {
    /// Returns a new builder.
    #[inline]
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the statsd client to use.
    /// The default is derived from the `DD_AGENT_HOST` environment variable.
    pub fn client(mut self, client: StatsdClient) -> Self {
        self.client = Some(client);
        self
    }

    /// Excludes the `dd.internal.entity_id` and `owner` tags
    /// that are automatically derived from environment variables.
    /// If you don't provide these tags,
    /// Datadog will not set the `env`, `service`, or `version` tags.
    pub fn without_standard_tags(mut self) -> Self {
        self.exclude_standard_tags = true;
        self
    }

    /// Add a single tag to be attached to all sent metrics.
    /// This method may be called multiple times to add multiple tags.
    pub fn tag(mut self, k: impl Into<Str>, v: impl Into<Str>) -> Self {
        self.base_tags.push((k.into(), v.into()));
        self
    }

    /// Add tags to be attached to all sent metrics.
    /// This method may be called multiple times to add multiple sets of tags.
    pub fn add_tags<K: Into<Str>, V: Into<Str>>(
        mut self,
        tags: impl IntoIterator<Item = (K, V)>,
    ) -> Self {
        self.base_tags
            .extend(tags.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }

    /// Spawn a thread to periodically send the metrics registered with [`register_emitter`].
    pub fn start(self) -> Result<MetricsSender, SendMetricsError> {
        let MetricsSenderBuilder {
            client,
            mut base_tags,
            exclude_standard_tags,
        } = self;
        if !exclude_standard_tags {
            if let Ok(tag) = env::var("DD_ENTITY_ID") {
                base_tags.insert(
                    0,
                    (Str::from_static("dd.internal.entity_id"), Str::from(tag)),
                );
            }
        }
        let (flush_sink, client) = if let Some(client) = client {
            (None, client)
        } else {
            let host = std::env::var("DD_AGENT_HOST")
                .or_else(|_| std::env::var("DATADOG_TRACE_AGENT_HOSTNAME"))
                .unwrap_or_else(|_| "127.0.0.1".into());
            let host = (host.as_ref(), 8125);
            tracing::debug!("connecting to statsd on {host:?}");
            let sink = BufferedUdpMetricSink::from(host)
                .map_err(|e| SendMetricsError::Other(Box::new(e)))?;
            (Some(sink.clone()), StatsdClient::from_sink("", sink))
        };

        if SENDER_RUNNING
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(SendMetricsError::AlreadySending);
        }

        let interrupt_tx = Arc::new(AtomicBool::new(false));
        let interrupt_rx = interrupt_tx.clone();
        let join_handle = thread::spawn(move || {
            tracing::debug!("reporting metrics every {METRIC_REPORTING_FREQUENCY:#?}");
            let mut next_emit = Instant::now() + METRIC_REPORTING_FREQUENCY;
            // Do a force flush of metrics in our buffers every 3 ticks to ensure we still report metrics in
            // low-throughput scenarios.
            let mut next_flush = Instant::now() + (METRIC_REPORTING_FREQUENCY * 3);
            loop {
                if let Some(remaining) = next_emit.checked_duration_since(Instant::now()) {
                    if interrupt_rx.load(Ordering::SeqCst) {
                        tracing::debug!("metrics-sender thread interrupted, exiting");
                        return;
                    }
                    thread::park_timeout(remaining);
                }
                if interrupt_rx.load(Ordering::SeqCst) {
                    tracing::debug!("metrics-sender thread interrupted, exiting");
                    return;
                }
                let now = Instant::now();
                if now < next_emit {
                    // Park can be awoken spuriously.
                    continue;
                }

                next_emit = now + METRIC_REPORTING_FREQUENCY;
                for emitter in EMITTERS.read().values() {
                    let result = emitter.emit_metrics(
                        &client,
                        &mut base_tags.iter().map(|(k, v)| (k.borrow(), v.borrow())),
                    );
                    if let Err(err) = result {
                        error!("emitting metrics failed: {err}");
                    }
                    if interrupt_rx.load(Ordering::SeqCst) {
                        tracing::debug!("metrics-sender thread interrupted, exiting");
                        return;
                    }
                }
                // Now attempt to force-flush to ensure timely delivery of metrics in low-throughput scenarios
                if now >= next_flush {
                    if let Some(sink) = flush_sink.as_ref() {
                        if let Err(e) = sink.flush() {
                            tracing::error!("failed to flush metrics: {e}")
                        }
                    }
                    next_flush = now + (METRIC_REPORTING_FREQUENCY * 3);
                }
            }
        });

        Ok(MetricsSender {
            interrupt: interrupt_tx,
            join_handle: Some(join_handle),
        })
    }
}

/// Handle to a running metrics sender thread.
/// `Drop`ping the handle will stop the thread.
#[derive(Debug)]
#[must_use = "Dropping a MetricsSender stops its thread"]
pub struct MetricsSender {
    interrupt: Arc<AtomicBool>,
    join_handle: Option<JoinHandle<()>>,
}

impl MetricsSender {
    /// Return a new sender builder.
    #[inline]
    pub fn builder() -> MetricsSenderBuilder {
        MetricsSenderBuilder::new()
    }
}

impl Drop for MetricsSender {
    fn drop(&mut self) {
        tracing::debug!("MetricsSender dropped, shutting down");
        self.interrupt.store(true, Ordering::SeqCst);
        let join_handle = self.join_handle.take().unwrap();
        join_handle.thread().unpark();
        join_handle.join().ok();
        SENDER_RUNNING.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use std::iter;

    use cadence::{Counted, SpyMetricSink, StatsdClient};

    use super::*;
    use crate::metrics::v2::statsd_output::StatsdOutput;

    #[test]
    fn test_send() {
        struct MyMetricsEmitter {}

        impl MetricsEmitter for MyMetricsEmitter {
            fn emit_metrics(
                &self,
                client: &cadence::StatsdClient,
                base_tags: &mut dyn Iterator<Item = (&str, &str)>,
            ) -> Result<(), cadence::MetricError> {
                let mut builder = client.count_with_tags("foo", 1);
                for (k, v) in base_tags {
                    builder = builder.with_tag(k, v);
                }
                builder.try_send()?;
                Ok(())
            }
        }

        let emitter = Arc::new(MyMetricsEmitter {});
        let _registration = register_emitter(emitter);

        let (receiver, sink) = SpyMetricSink::with_capacity(10);
        let client = StatsdClient::from_sink("", sink);
        let _sender = MetricsSender::builder().client(client).start();

        let packet = receiver.recv().unwrap();
        let got = StatsdOutput::from_packets(iter::once(packet));
        assert_eq!(got, StatsdOutput::from([(b"foo:1|c".to_vec(), 1),]));
    }
}
