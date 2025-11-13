use crate::metrics::{
    define_metrics, emit_worker::SpawnEmitWorker, string_interner::StringInterner, SharedMetrics,
};
use crate::tokio_utils::AbortOnDrop;
use anyhow::Result;
use prost::Message;
use std::{
    collections::HashMap,
    fmt::Display,
    marker::PhantomData,
    mem::take,
    sync::{mpsc::SendError, Arc},
    time::Duration,
};
// Use backoff 0.4 explicitly (there's also 0.1.6 in the dep tree)
extern crate backoff as backoff_v04;
use backoff_v04 as backoff;
use tokio::{
    sync::{broadcast, mpsc},
    time::MissedTickBehavior,
};
use tonic::codegen::InterceptedService;

use crate::gcloud::{
    auth::AuthorizationHeaderInterceptor,
    google::pubsub::v1::{self as proto, publisher_client, subscriber_client},
    grpc::connection::Connection,
};

pub const GOOGLE_PUBSUB_DOMAIN: &str = "pubsub.googleapis.com";

impl Connection {
    /// Creates a pubsub publisher for a specific topic. The publisher ends when all handles are dropped.
    ///
    /// Messages are published when:
    /// - `max_interval` time has passed
    /// - `max_buffer_size` for the queue buffer has been reached
    pub fn create_pubsub_publisher(
        &self,
        topic: PubSubTopic,
        max_interval: Duration,
        max_buffer_size: usize,
        metrics: SharedMetrics,
    ) -> TopicPublisherHandle {
        let publisher = TopicPublisher {
            client: self.create_publisher_client(),
            topic,
            pubsub_stats: PubsubStats::new(),
            string_interner: StringInterner::new(8),
        };

        let (send_queue_tx, closed_rx) =
            publisher.spawn_send_queue_flusher(max_interval, max_buffer_size, metrics);

        TopicPublisherHandle {
            send_queue_tx,
            closed_rx,
        }
    }

    /// Creates a **synchronous** pubsub publisher for a specific topic.
    ///
    /// Synchronous, not meaning "blocking", but rather, meaning, that there is no batching. When you call `publish`,
    /// if it succeeds, it means that your message was accepted by the remote server.
    pub fn create_synchronous_pubsub_publisher(
        &self,
        topic: PubSubTopic,
        metrics: SharedMetrics,
    ) -> SynchronousTopicPublisher {
        SynchronousTopicPublisher::new(topic, self.create_publisher_client(), metrics)
    }

    /// Creates a subscriber client, which is used to provide a client to `pub_sub_streaming_pull::StreamingPullManager`
    pub fn create_subscriber_client(
        &self,
    ) -> subscriber_client::SubscriberClient<
        InterceptedService<tonic::transport::Channel, AuthorizationHeaderInterceptor>,
    > {
        subscriber_client::SubscriberClient::with_interceptor(
            self.channel.clone(),
            self.authorization_header_interceptor.clone(),
        )
    }

    /// Creates a publisher client, which is used to provide a client to `pub_sub_streaming_pull::StreamingPullManager`
    pub fn create_publisher_client(
        &self,
    ) -> publisher_client::PublisherClient<
        InterceptedService<tonic::transport::Channel, AuthorizationHeaderInterceptor>,
    > {
        publisher_client::PublisherClient::with_interceptor(
            self.channel.clone(),
            self.authorization_header_interceptor.clone(),
        )
    }
}

/// Represents a pub-sub subscription
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct PubSubSubscription {
    project: String,
    subscription: String,
}

impl PubSubSubscription {
    pub fn new(project: impl Into<String>, subscription: impl Into<String>) -> Self {
        Self {
            project: project.into(),
            subscription: subscription.into(),
        }
    }

    pub fn project(&self) -> &str {
        &self.project
    }

    pub fn subscription(&self) -> &str {
        &self.subscription
    }
}

impl Display for PubSubSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "projects/{}/subscriptions/{}",
            self.project, self.subscription
        )
    }
}

/// Represents a pub-sub topic
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct PubSubTopic {
    project: String,
    topic: String,
}

impl PubSubTopic {
    pub fn new(project: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            project: project.into(),
            topic: topic.into(),
        }
    }

    pub fn project(&self) -> &str {
        &self.project
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }
}

impl Display for PubSubTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "projects/{}/topics/{}", self.project, self.topic)
    }
}

pub struct TopicPublisher {
    client: publisher_client::PublisherClient<
        InterceptedService<tonic::transport::Channel, AuthorizationHeaderInterceptor>,
    >,
    topic: PubSubTopic,
    pubsub_stats: Arc<PubsubStats>,
    string_interner: StringInterner,
}

impl TopicPublisher {
    // FIXME: To make this a more generalizable abstraction, we should add a few things:
    // - the ability to "close" the sender,
    // - ability to wait for the send queue after close to finish flushing, to allow an application to delay shutdown
    //   until all messages are successfully delivered to pub-sub.

    /// Starts a background task that flushes the queue when:
    /// - `max_queue_flush_interval` time has passed, or:
    /// - `max_buffer_size` for the queue buffer has been reached.
    fn spawn_send_queue_flusher(
        mut self,
        max_queue_flush_interval: Duration,
        max_buffer_size: usize,
        metrics: SharedMetrics,
    ) -> (
        mpsc::UnboundedSender<proto::PubsubMessage>,
        broadcast::Receiver<()>,
    ) {
        let (send_queue_tx, mut send_queue_rx) = mpsc::unbounded_channel();

        // We use a broadcast channel to notify any outstanding [`TopicPublisherHandle`] instances that we have flushed
        // everything. The sender (closed_tx) is moved into the background thread and dropped when the publisher shuts
        // down.
        let (closed_tx, closed_rx) = broadcast::channel(1);

        tokio::spawn(async move {
            let _closed_tx = closed_tx;
            let mut interval = tokio::time::interval(max_queue_flush_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let _metrics_emitter = self.pubsub_stats.clone().spawn_emit_worker(metrics);

            let mut messages = Vec::new();

            tracing::info!(
                "Pubsub: publisher created for {} with {} buffer @ {:?} flush interval",
                self.topic,
                max_buffer_size,
                max_queue_flush_interval
            );

            let mut sender_dropped = false;

            while !sender_dropped {
                let should_flush_queue = tokio::select! {
                    // Force a flush when the interval is elapsed, if we have something in the queue.
                    _ = interval.tick() => !messages.is_empty(),
                    message = send_queue_rx.recv() => {
                        if let Some(message) = message {
                            messages.push(message);
                            messages.len() >= max_buffer_size
                        } else {
                            // Getting None back from send_queue_rx, means that we have no more senders, so we can stop,
                            // after we've completed all our work.
                            tracing::info!(
                                "Pubsub: publisher for {} shutting down, because no more senders are alive, {} messages in queue to flush, before shutdown",
                                self.topic,
                                messages.len()
                            );

                            sender_dropped = true;
                            !messages.is_empty()
                        }

                    },
                };

                if should_flush_queue {
                    // Reset since we are flushing, and this could've been triggered
                    // by a request rather than the interval
                    interval.reset();

                    let req = proto::PublishRequest {
                        topic: self.topic.to_string(),
                        messages: take(&mut messages),
                    };

                    self.send_publish(req).await;
                }
            }
        });

        (send_queue_tx, closed_rx)
    }

    async fn send_publish(&mut self, req: proto::PublishRequest) {
        let queue_size = req.messages.len() as i64;

        let req_ref = &req;
        let publish = || {
            // NOTE(rossdylan): In order to handle the lifetime requirements on future we return here, we need to either
            // clone or take references to the fields we need out of self.
            let mut client_clone = self.client.clone();
            let stats = self.pubsub_stats.clone();
            let project = self.string_interner.intern(&self.topic.project);
            let topic = self.string_interner.intern(&self.topic.topic);
            async move {
                // NOTE(rossdylan): Tonic requires a brand new message for every attempt so we unfortunately need this clone
                match client_clone.publish(req_ref.clone()).await {
                    Err(e) => {
                        tracing::error!("failed to flush pubsub messages due to {:?}", e);
                        stats.publish_failed.incr_by(queue_size, project, topic);
                        // Mapping of transient/permanent errors taken from
                        // https://cloud.google.com/pubsub/docs/reference/error-codes
                        let err = match e.code() {
                            tonic::Code::NotFound => backoff::Error::Permanent(e),
                            tonic::Code::AlreadyExists => backoff::Error::Permanent(e),
                            tonic::Code::PermissionDenied => backoff::Error::Permanent(e),
                            tonic::Code::FailedPrecondition => backoff::Error::Permanent(e),
                            tonic::Code::Unauthenticated => backoff::Error::Permanent(e),
                            _ => backoff::Error::transient(e),
                        };
                        Err(err)
                    }
                    _ => {
                        stats.publish_sends.incr_by(queue_size, project, topic);
                        Ok(())
                    }
                }
            }
        };
        let final_res =
            backoff::future::retry(backoff::ExponentialBackoff::default(), publish).await;
        if let Err(err) = final_res {
            tracing::error!("permanent failure to flush {queue_size} messages due to {err:?}");
        }
    }
}

#[derive(Debug, Clone)]
pub struct PubSubMessage<State = PubSubMessageValid> {
    data: Vec<u8>,
    attributes: HashMap<String, String>,
    ordering_key: Option<String>,
    _state: PhantomData<State>,
}

// Two different type-states to represent a message that is fully formed (valid), versus partially formed (invalid),
// a message is fully formed when it has a non-empty data, or when it has an empty data, but non-empty attributes.
pub struct PubSubMessageInvalid;

#[derive(Clone, Debug)]
pub struct PubSubMessageValid;

impl PubSubMessage<PubSubMessageInvalid> {
    /// Constructs a message with no data, you must call [`PubSubMessage::set_attribute`] at least once, before you
    /// can publish this message. If data is empty, an attribute, *must* be specified.
    pub fn empty() -> Self {
        Self {
            data: Default::default(),
            attributes: Default::default(),
            ordering_key: Default::default(),
            _state: PhantomData,
        }
    }
}

/// Returned from `PubsubMessage::with_data` when the provided data is an empty Vec.
#[derive(Debug, Clone)]
pub struct PubsubMessageDataEmpty;

impl std::fmt::Display for PubsubMessageDataEmpty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("creating an empty PubsubMessage via with_data is not supported, use PubsubMessage::empty() instead")
    }
}
impl std::error::Error for PubsubMessageDataEmpty {}

impl PubSubMessage<PubSubMessageValid> {
    /// Constructs a message with the given data.
    ///
    /// Returns an error if the provided `data` is Empty. Prefer to use `PubsubMessage::empty()` instead.
    pub fn with_data(data: impl Into<Vec<u8>>) -> Result<Self, PubsubMessageDataEmpty> {
        let data: Vec<_> = data.into();
        if data.is_empty() {
            return Err(PubsubMessageDataEmpty);
        }

        Ok(Self {
            data,
            attributes: Default::default(),
            ordering_key: Default::default(),
            _state: PhantomData,
        })
    }

    /// Constructs a message with the given proto data.
    ///
    /// The program will panic if the provided proto message `data` is empty.
    pub fn with_proto_data<T: prost::Message>(data: T) -> Self {
        PubSubMessage::with_data(data.encode_to_vec()).expect("proto message can't be empty")
    }

    /// Creates a `PubSubMessage` with no data, and the given attribute set.
    pub fn with_attribute(key: impl Into<String>, value: impl Into<String>) -> Self {
        let mut attributes = HashMap::new();
        attributes.insert(key.into(), value.into());

        Self {
            attributes,
            data: Default::default(),
            ordering_key: Default::default(),
            _state: PhantomData,
        }
    }

    pub fn with_serde_data<T: serde::Serialize>(data: T) -> Result<Self, PubsubMessageDataEmpty> {
        let data = serde_json::to_string(&data).map_err(|_| PubsubMessageDataEmpty)?;
        Self::with_data(data)
    }

    /// Creates a `PubSubMessage` with no data, and the given attribute item set.
    pub fn with_attribute_item<MessageAttributeItemT: MessageAttributeItem>(
        attribute_item: MessageAttributeItemT,
    ) -> Self {
        Self::with_attribute(MessageAttributeItemT::KEY, attribute_item.into_value())
    }

    /// Converts this to an encoded proto message.
    pub fn encode_to_vec(self) -> Vec<u8> {
        self.to_proto_message().encode_to_vec()
    }

    // Converts this to a proto message.
    fn to_proto_message(self) -> proto::PubsubMessage {
        proto::PubsubMessage {
            attributes: self.attributes,
            data: self.data,
            ordering_key: self.ordering_key.unwrap_or_default(),
            // These fields are set by the server.
            message_id: Default::default(),
            publish_time: None,
        }
    }
}

impl<T> PubSubMessage<T> {
    /// Sets a given attribute on the message.
    pub fn set_attribute(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> PubSubMessage<PubSubMessageValid> {
        self.attributes.insert(key.into(), value.into());

        PubSubMessage {
            data: self.data,
            attributes: self.attributes,
            ordering_key: self.ordering_key,
            _state: PhantomData,
        }
    }

    pub fn set_attribute_item<MessageAttributeItemT: MessageAttributeItem>(
        self,
        attribute_item: MessageAttributeItemT,
    ) -> PubSubMessage<PubSubMessageValid> {
        self.set_attribute(MessageAttributeItemT::KEY, attribute_item.into_value())
    }

    /// Sets the ordering key for the message.
    pub fn set_ordering_key(mut self, ordering_key: impl Into<String>) -> Self {
        self.ordering_key = Some(ordering_key.into());

        self
    }
}

/// A message attribute item which encodes the key and value in a single struct.
pub trait MessageAttributeItem {
    const KEY: &'static str;

    fn into_value(self) -> String;
}

#[derive(Debug)]
pub struct TopicPublisherHandle {
    closed_rx: broadcast::Receiver<()>,
    send_queue_tx: mpsc::UnboundedSender<proto::PubsubMessage>,
}

impl Clone for TopicPublisherHandle {
    fn clone(&self) -> Self {
        Self {
            closed_rx: self.closed_rx.resubscribe(),
            send_queue_tx: self.send_queue_tx.clone(),
        }
    }
}

impl TopicPublisherHandle {
    pub fn new(
        send_queue_tx: mpsc::UnboundedSender<proto::PubsubMessage>,
        closed_rx: broadcast::Receiver<()>,
    ) -> Self {
        Self {
            send_queue_tx,
            closed_rx,
        }
    }
}

impl TopicPublisherHandle {
    pub fn publish(&self, message: PubSubMessage) -> Result<(), SendError<PubSubMessage>> {
        let did_have_ordering_key = message.ordering_key.is_some();

        self.send_queue_tx
            .send(message.to_proto_message())
            .map_err(|e| {
                SendError(PubSubMessage {
                    data: e.0.data,
                    attributes: e.0.attributes,
                    ordering_key: did_have_ordering_key.then_some(e.0.ordering_key),
                    _state: PhantomData,
                })
            })
    }

    /// Consume this handle and wait for the underlying publisher to exit.
    /// NOTE: If you clone and move this handle around this method will block until all handles have been dropped
    pub async fn close(self) {
        // Split our fields out so we can explicitly drop the send queue and wait for the closed notification
        let Self {
            mut closed_rx,
            send_queue_tx,
        } = self;
        drop(send_queue_tx);
        // This is fine because all we care about is that this completed
        let _res = closed_rx.recv().await;
    }

    pub async fn close_timeout(self, timeout: Duration) -> Result<()> {
        // Split our fields out so we can explicitly drop the send queue and wait for the closed notification
        let Self {
            mut closed_rx,
            send_queue_tx,
        } = self;
        drop(send_queue_tx);
        let _res = tokio::time::timeout(timeout, closed_rx.recv()).await?;
        Ok(())
    }
}

struct SynchronousTopicPublisherInner {
    topic: PubSubTopic,
    client: publisher_client::PublisherClient<
        InterceptedService<tonic::transport::Channel, AuthorizationHeaderInterceptor>,
    >,
    pubsub_stats: Arc<PubsubStats>,
    string_interner: StringInterner,
    _metrics_emitter: AbortOnDrop,
}

#[derive(Clone)]
pub struct SynchronousTopicPublisher {
    inner: Arc<SynchronousTopicPublisherInner>,
}

impl SynchronousTopicPublisher {
    pub fn new(
        topic: PubSubTopic,
        client: publisher_client::PublisherClient<
            InterceptedService<tonic::transport::Channel, AuthorizationHeaderInterceptor>,
        >,
        metrics: SharedMetrics,
    ) -> Self {
        let pubsub_stats = PubsubStats::new();
        let _metrics_emitter = pubsub_stats.clone().spawn_emit_worker(metrics);

        Self {
            inner: Arc::new(SynchronousTopicPublisherInner {
                topic,
                client,
                pubsub_stats,
                string_interner: StringInterner::new(8),
                _metrics_emitter,
            }),
        }
    }

    pub async fn publish(&self, messages: &[PubSubMessage]) -> Result<(), tonic::Status> {
        let mut client = self.inner.client.clone();
        let num_messages = messages.len() as i64;
        match client
            .publish(proto::PublishRequest {
                topic: self.inner.topic.to_string(),
                messages: messages
                    .iter()
                    .cloned()
                    .map(|x| x.to_proto_message())
                    .collect(),
            })
            .await
        {
            Ok(_) => {
                self.inner.pubsub_stats.publish_sends.incr_by(
                    num_messages,
                    self.inner.string_interner.intern(&self.inner.topic.project),
                    self.inner.string_interner.intern(&self.inner.topic.topic),
                );
                Ok(())
            }
            Err(err) => {
                self.inner.pubsub_stats.publish_failed.incr_by(
                    num_messages,
                    self.inner.string_interner.intern(&self.inner.topic.project),
                    self.inner.string_interner.intern(&self.inner.topic.topic),
                );
                Err(err)
            }
        }
    }
}

define_metrics!(PubsubStats, [
    publish_failed => DynamicCounter("discord_gcloud.pubsub.publish.failed", [project, topic]),
    publish_sends => DynamicCounter("discord_gcloud.pubsub.publish.sends", [project, topic]),
]);
