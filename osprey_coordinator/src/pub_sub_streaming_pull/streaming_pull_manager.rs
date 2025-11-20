use std::{
    future::{pending, Future},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::backoff_utils::AsyncBackoff;
use crate::tokio_utils::AbortOnDrop;
use hdrhistogram::Histogram;
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    task::JoinSet,
    time::{interval, interval_at, MissedTickBehavior},
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{
    codegen::{BoxFuture, InterceptedService},
    service::Interceptor,
    Status,
};

use crate::gcloud::{
    google::pubsub::v1::{
        subscriber_client::SubscriberClient, AcknowledgeRequest, ModifyAckDeadlineRequest,
        StreamingPullRequest, StreamingPullResponse,
    },
    pubsub::PubSubSubscription,
};
use crate::metrics::counters::StaticCounter;
use crate::metrics::emit_worker::SpawnEmitWorker;
use crate::metrics::gauges::StaticGauge;
use crate::metrics::histograms::StaticHistogram;
use crate::metrics::MetricsClientBuilder;
use crate::pub_sub_streaming_pull::{
    message::MessageAckGuard, AckId, AckOrNack, DetachedMessage, FlowControl, Message,
    MessageAckQueue, MessageHandler, MessagesInFlight, MessagesOnHold, StreamingPullManagerHandle,
    StreamingPullManagerMetrics,
};

/// The maximum number of ACK IDs to send in a single StreamingPullRequest.
const ACK_IDS_MAX_BATCH_SIZE: usize = 2500;

/// How often we should update gauge based metrics.
const METRICS_REPORTING_INTERVAL: Duration = Duration::from_secs(1);

/// How often we should attempt to flush leases.
const LEASE_RENEWAL_INTERVAL: Duration = Duration::from_secs(1);

type TonicSubscriberClient<I> = SubscriberClient<InterceptedService<tonic::transport::Channel, I>>;

impl FlowControl {
    fn clamp_lease_duration(&self, lease_duration_secs: u32) -> u32 {
        lease_duration_secs.clamp(
            self.min_duration_per_lease_extension,
            self.max_duration_per_lease_extension,
        )
    }
}

/// The [`StreamingPullManager`] is responsible for managing a connection to pub-sub, pulling messages over the
/// StreamingPull interface. The streaming pull manager runs a tokio task which maintains the connection to pub-sub
/// and handles re-connection, and lease/message management and buffering.
pub struct StreamingPullManager<T, M> {
    /// The recieve end of of [`StreamingPullManagerState::sender`], receives the messages and processes them.
    receiver: UnboundedReceiver<StreamingPullManagerMessage>,
    /// Internal state of the streaming pull manager. Refer to it's documentation.
    state: StreamingPullManagerState<T>,
    /// The GRPC Streaming channel that pub-sub sends us messages on.
    channel: StreamingPullChannel,
    /// The [`MessageHandler`] that is invoked when messages are received and need to be processed.
    message_handler: M,
    /// Binds the lifetime of the metrics emitter to the lifetime of the streaming pull manager, which will cause the
    /// metrics emitter to stop when the manager is stopped.
    _metrics_emit_worker_abort_on_drop: AbortOnDrop,
}

/// Holds the stateful bits of the streaming pull manager.
///
/// This is a separate struct because the state is used to construct the channel, and thus we must have a state,
/// and channel before we can make the [`StreamingPullManager`]. See the `new` method's implementation.
struct StreamingPullManagerState<I> {
    /// The GRPC client that is used to communicate with pub-sub.
    client: TonicSubscriberClient<I>,
    /// The flow control configuration.
    flow_control: FlowControl,
    /// How long the lease on messages should be (computed by clamping the min/max lease duration from flow control,
    /// along with the p99 of message handling latency)
    message_lease_renewal_duration_secs: u32,
    /// The messages that are currently on hold for processing, either because too many messages are in-flight, or
    /// we need to satisfy ordering_key constraints.
    messages_on_hold: MessagesOnHold,
    /// The client-id we are using to identify ourselves to pub-sub. This is a random Uuid that is generated at the
    /// time the streaming pull manager is created.
    client_id: uuid::Uuid,
    /// The name of the pub-sub subscription we are trying to pull messages from, in the format:
    /// `projects/{project}/subscriptions/{sub}`.
    subscription: String,
    /// The queue which holds all the ack-ids of messages received that are either on hold or processing, and batches
    /// chunks of ack/nack requests to be flushed to pub-sub.
    message_ack_queue: MessageAckQueue,
    /// Mapping of AckId -> OrderingKey (for request activation), and to track which and how many requests are
    /// currently in-flight.
    messages_in_flight: MessagesInFlight,
    /// When set to Some(...), the streaming pull manager has gracefully stopped. The join handle will resolve when
    /// we have nacked all messages that were on hold with the pub-sub server.
    graceful_stop_join_handles: Option<GracefulStopJoinHandles>,
    /// A struct used to record metrics.
    metrics: Arc<StreamingPullManagerMetrics>,
    /// Histogram for message latency, histogram is measured in milliseconds.
    message_latency_histogram: Histogram<u32>,
    /// A sender that is used to be [`Clone`]d into the [`MessageAckGuard`] part of a [`Message`], which is primarily
    /// used to send an ack/nack message back to the manager, so it can send that back to pub-sub. Additionally, it is
    /// used by the [`StreamingPullManagerHandle`] in order to stop the manager.
    sender: UnboundedSender<StreamingPullManagerMessage>,
    /// JoinSet of the background tasks that are currently running.
    background_tasks: JoinSet<()>,
    /// Async backoff to track retries for the pub-sub channel reconnection.
    streaming_pull_channel_async_backoff: AsyncBackoff,
}

struct GracefulStopJoinHandles {
    message_handler: Option<AbortOnDrop>,
}

impl<I> StreamingPullManagerState<I>
where
    I: Interceptor + Clone + Send + Sync + 'static,
{
    /// Creates the initial streaming pull requests necessary to connect to the grpc channel!
    ///
    /// The initial streaming pull requests contain the flow control settings and grpc subscription to connect to.
    fn initial_streaming_pull_request(&self) -> StreamingPullRequest {
        StreamingPullRequest {
            client_id: self.client_id.to_string(),
            subscription: self.subscription.clone(),
            max_outstanding_bytes: self.flow_control.max_bytes as _,
            max_outstanding_messages: self.flow_control.max_messages as _,
            stream_ack_deadline_seconds: self.message_lease_renewal_duration_secs as _,
            ..Default::default()
        }
    }

    /// Performs the given request as a background task, retrying up to 10 times, with exponential backoff.
    ///
    /// Background task processing will block the streaming pull manager's graceful stop cycle.
    fn perform_request_in_background_with_retries<
        RequestExecutor,
        ResponseResultOk,
        RequestBody,
        ResponseFuture,
    >(
        &mut self,
        request_type: &'static str,
        request_body: RequestBody,
        request_executor: RequestExecutor,
    ) where
        ResponseFuture: Future<Output = Result<ResponseResultOk, tonic::Status>> + Send,
        ResponseResultOk: Send,
        RequestBody: Clone + Send + Sync + tonic::IntoRequest<RequestBody> + 'static,
        RequestExecutor: Fn(TonicSubscriberClient<I>, tonic::Request<RequestBody>) -> ResponseFuture
            + Send
            + Sync
            + 'static,
    {
        const MAX_BACKGROUND_REQUEST_ATTEMPTS: u8 = 10;
        const BACKGROUND_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

        // Process retries in the background, keeping track of in-flight background requests.
        let subscription = self.subscription.clone();
        let client_id = self.client_id;
        let client = self.client.clone();
        let metrics = self.metrics.clone();
        let mut async_backoff = async_backoff();
        let root_start_time = Instant::now();

        self.background_tasks.spawn(async move {
            for attempt in 1..=MAX_BACKGROUND_REQUEST_ATTEMPTS {
                let start_time = Instant::now();
                let result = tokio::time::timeout(
                    BACKGROUND_REQUEST_TIMEOUT,
                    (request_executor)(client.clone(), request_body.clone().into_request())
                )
                .await
                .map_err(|_| Status::deadline_exceeded("timeout exceeded"))
                .and_then(|e| e);

                metrics.pubsub_grpc_request_latency.record_with_key(
                    request_type,
                    start_time.elapsed(),
                );
                match result {
                    Ok(_) => {
                        metrics
                            .pubsub_grpc_request_success
                            .incr(request_type);

                        tracing::debug!(
                            {%subscription, %client_id, %request_type},
                            "background request {} has has succeeded on attempt {} ({:?} from when the request was intially attempted)",
                            request_type, attempt, root_start_time.elapsed(),
                        );
                        return;
                    }
                    Err(error) => {
                        if attempt < MAX_BACKGROUND_REQUEST_ATTEMPTS {
                            metrics
                                .pubsub_grpc_request_error
                                .incr(request_type, error.code());

                            let retry_in = async_backoff.fail();
                            tracing::error!(
                                {%subscription, %client_id, ?error, %request_type},
                                "background request {} has failed on attempt {}, will retry in {:?}",
                                request_type, attempt, retry_in
                            );
                            tokio::time::sleep(retry_in).await;
                        } else {
                            metrics.pubsub_grpc_background_request_dropped.incr(request_type);

                            tracing::error!(
                                {%subscription, %client_id, ?error, %request_type},
                                "background request {} has failed on attempt {}, giving up forever.",
                                request_type, attempt
                            );
                        }
                    }
                }
            }
        });
    }

    /// Tries to create the streaming pull channel.
    ///
    /// Does not retry!
    fn create_streaming_pull_channel(&mut self, disconnected: bool) -> StreamingPullChannel {
        let (sender, receiver) = unbounded_channel();
        self.recompute_message_lease_renewal_duration_secs();
        sender.send(self.initial_streaming_pull_request()).ok();

        let mut client = self.client.clone();
        let subscription = self.subscription.clone();
        let client_id = self.client_id;
        let connection_delay = if disconnected {
            let retry_in = self.streaming_pull_channel_async_backoff.fail();
            tracing::info!(
                {%subscription, %client_id},
                "will attempt to re-connect to the streaming pull channel in {:?}",
                retry_in,
            );
            retry_in
        } else {
            tracing::info!(
                {%subscription, %client_id},
                "connecting to the streaming channel",
            );
            Duration::ZERO
        };

        let connection_future = async move {
            tokio::time::sleep(connection_delay).await;

            let start_time = Instant::now();
            let streaming = client
                .streaming_pull(UnboundedReceiverStream::new(receiver))
                .await?
                .into_inner();

            tracing::info!(
                {%subscription, %client_id},
                "connection to streaming pull channel received first message after {:?}",
                start_time.elapsed()
            );

            Ok(streaming)
        };

        StreamingPullChannel::new(Box::pin(connection_future), sender)
    }

    /// Returns the maximum amount of time we should wait before renewing the lease on a message.
    fn get_buffered_lease_renewal_duration(&self) -> Duration {
        // Allow for 20% buffer, or 5 seconds, which ever is greater.
        let buffer = Duration::from_millis(
            ((((self.message_lease_renewal_duration_secs * 1000) as f64) * 0.2) as u64).min(5_000),
        );

        Duration::from_secs(self.message_lease_renewal_duration_secs as _) - buffer
    }

    /// Recomputes `message_lease_renewal_duration_secs` based upon the p99 of message processing latency.
    fn recompute_message_lease_renewal_duration_secs(&mut self) {
        let mut p99_message_latency_millis =
            self.message_latency_histogram.value_at_percentile(99.0);

        // Round up to nearest second.
        if p99_message_latency_millis % 1000 != 0 {
            p99_message_latency_millis += 1000 - (p99_message_latency_millis % 1000);
        }

        let next_lease_renewal_duration_secs = self
            .flow_control
            .clamp_lease_duration((p99_message_latency_millis / 1000) as _);

        if self.message_lease_renewal_duration_secs != next_lease_renewal_duration_secs {
            tracing::info!(
                {subscription = %self.subscription, client_id = %self.client_id},
                "updating message lease renewal duration secs from {} to {}",
                self.message_lease_renewal_duration_secs,
                next_lease_renewal_duration_secs
            );
            self.message_lease_renewal_duration_secs = next_lease_renewal_duration_secs;
        }
    }
}

/// Creates an [`AsyncBackoff`] for use where exponential backoff is needed with sensible defaults.
fn async_backoff() -> AsyncBackoff {
    AsyncBackoff::new(crate::backoff_utils::Config {
        min_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(10),
        ..Default::default()
    })
}

impl<I, M> StreamingPullManager<I, M>
where
    I: Interceptor + Clone + Send + Sync + 'static,
    M: MessageHandler,
{
    /// Creates and starts a new [`StreamingPullManager`], returning a [`StreamingPullManagerHandle`] that can be used
    /// to stop the manager.
    ///
    /// The function arguments are as follows:
    ///  - `client` the tonic grpc client that can make requests to the pub-sub GRPC server.
    ///  - `subscription` see [`PubSubSubscription`].
    ///  - `flow_control` see [`FlowControl`].
    ///  - `message_handler` see [`MessageHandler`],
    ///  - `metrics_client_builder` see [`MetricsClientBuilder`]
    pub fn new(
        client: TonicSubscriberClient<I>,
        subscription: PubSubSubscription,
        flow_control: FlowControl,
        message_handler: M,
        metrics_client_builder: MetricsClientBuilder,
    ) -> StreamingPullManagerHandle {
        let client_id = uuid::Uuid::new_v4();

        let metrics = StreamingPullManagerMetrics::new();
        let metrics_client = metrics_client_builder
            .with_tag("subscription", subscription.to_string())
            .with_tag("client_id", client_id.to_string())
            .build()
            .expect("invariant: failed to create metrics client");
        let _metrics_emit_worker_abort_on_drop = metrics.clone().spawn_emit_worker(metrics_client);

        let (sender, receiver) = unbounded_channel();
        let message_latency_histogram = Histogram::new_with_max(
            (flow_control.max_duration_per_lease_extension * 1000) as _,
            3
        ).expect("invariant: histogram creation should never fail, as the bounds are checked in FlowControl");
        let message_lease_renewal_duration_secs = flow_control.min_duration_per_lease_extension;
        let message_ack_queue = MessageAckQueue::new(
            flow_control.max_messages,
            std::cmp::min(ACK_IDS_MAX_BATCH_SIZE, flow_control.max_messages),
            Duration::from_millis(100),
        );

        let mut state = StreamingPullManagerState {
            client,
            metrics,
            client_id,
            flow_control,
            subscription: subscription.to_string(),
            message_ack_queue,
            message_latency_histogram,
            message_lease_renewal_duration_secs,
            sender: sender.clone(),
            background_tasks: Default::default(),
            messages_on_hold: Default::default(),
            messages_in_flight: Default::default(),
            graceful_stop_join_handles: Default::default(),
            streaming_pull_channel_async_backoff: async_backoff(),
        };

        let channel = state.create_streaming_pull_channel(false);
        let manager = Self {
            state,
            channel,
            receiver,
            message_handler,
            _metrics_emit_worker_abort_on_drop,
        };

        StreamingPullManagerHandle::new(
            subscription,
            manager.state.client_id,
            sender,
            tokio::task::spawn(manager.main_loop()).into(),
        )
    }

    /// Runs the main loop of the streaming pull manager. This runs inside a tokio task and is spawned by `new`.
    async fn main_loop(mut self) -> Instant {
        tracing::info!(
            {subscription = %self.state.subscription, client_id = %self.state.client_id},
            "streaming pull manager starting."
        );

        enum Action {
            /// Streaming pull caused an error, we'll need to re-connect to pub-sub.
            StreamingPullError(Option<tonic::Status>),
            /// Streaming pull returned a result that we need to process.
            StreamingPullResult(StreamingPullResponse),
            /// The mpsc to this task has received a message we need to process.
            ManagerMessage(StreamingPullManagerMessage),
            /// The message ack queue has asked us to flush a batch of ack_ids to pub_sub.
            FlushAckOrNackChunk(Vec<AckOrNack<String>>),
            /// We need to refresh our leases on messages that we currently are processing.
            RenewLeases,
            /// We need to report metrics.
            ReportMetrics,
        }

        let mut metrics_reporting_interval = interval(METRICS_REPORTING_INTERVAL);
        metrics_reporting_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut lease_renewal_interval = interval(LEASE_RENEWAL_INTERVAL);
        lease_renewal_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        while !self.should_stop() {
            // NOTE: We are keeping the bodies of the match arms in the select! block simple, because
            // rust-analyzer autocomplete and syntax highlighting sucks inside of the tokio::select macro.
            let action = tokio::select! {
                // The ordering of each of the matches below is deliberate, and ranked in order of importance with
                // the function that it performs and how urgent it is we must perform it when there is competing work.
                //
                // The rationale behind the ordering is explained below.
                biased;

                // bias: metrics is a trivial task, and one that is important enough that in the event of resource
                // starvation or contention, having metrics working will allow us to debug the system eaiser.
                _ = metrics_reporting_interval.tick() => {
                    Action::ReportMetrics
                }
                // bias: Renewing leased messages is the most important to ensure we keep up on when we're under
                // pressure.
                _ = lease_renewal_interval.tick() => {
                    Action::RenewLeases
                }
                // bias: receiving messages from the mpsc channel is weighed over the message_ack_queue queue flushing,
                // because it ensures that we fully empty the mpsc queue of all acks before we try to flush a chunk of
                // acks to pub-sub. The send-end of the receiver is guaranteed to eventually stop producing messages,
                // because we limit the number of messages we have in-flight at once. This means that we should never
                // starve the tasks below.
                Some(message) = self.receiver.recv() => {
                    Action::ManagerMessage(message)
                }
                // bias: it's important to handle acks/nacks before we read more messages from the pub-sub channel.
                ack_batch = self.state.message_ack_queue.next_chunk() => {
                    Action::FlushAckOrNackChunk(ack_batch)
                }
                // bias: once all other tasks above are complete, we can receive messages from the pub-sub channel.
                streaming_pull_result = self.channel.next_message() => {
                    match streaming_pull_result {
                        Ok(Some(response)) => Action::StreamingPullResult(response),
                        Ok(None) => Action::StreamingPullError(None),
                        Err(e) => Action::StreamingPullError(Some(e))
                    }
                },
                // bias: handle completion of background tasks, this is the lowest priority, and only used
                // to report metrics, and determine when we should fully shutdown.
                _ = self.state.background_tasks.join_next(), if !self.state.background_tasks.is_empty() => {
                    continue;
                }
            };

            match action {
                Action::ManagerMessage(StreamingPullManagerMessage::AckOrNack {
                    ack_or_nack,
                    latency,
                }) => self.handle_ack_or_nack(ack_or_nack, latency),
                Action::StreamingPullResult(streaming_pull_response) => {
                    self.handle_streaming_pull_response(streaming_pull_response)
                }
                Action::FlushAckOrNackChunk(chunk) => self.flush_ack_or_nack_chunk(chunk),
                Action::RenewLeases => self.handle_renew_leases(),
                Action::ReportMetrics => self.handle_report_metrics(),
                Action::StreamingPullError(error) => self.handle_streaming_pull_error(error),
                Action::ManagerMessage(StreamingPullManagerMessage::GracefulStop { sender }) => {
                    self.handle_graceful_stop(sender)
                }
            }
        }

        tracing::info!(
            {subscription = %self.state.subscription, client_id = %self.state.client_id},
            "all in-fight requests have been completed, stopping streaming pull manager"
        );

        // As part of graceful stop, we'll ensure that the graceful stop task has completed successfully,
        // before exiting the manager loop.
        if let Some(graceful_join_handle) = self.state.graceful_stop_join_handles.take() {
            if let Some(message_handler) = graceful_join_handle.message_handler {
                tracing::info!(
                    {subscription = %self.state.subscription, client_id = %self.state.client_id},
                    "waiting for graceful stop tasks to complete"
                );

                message_handler
                    .await
                    .expect("message handler graceful stop task panicked");
            }
        }

        tracing::info!(
            {subscription = %self.state.subscription, client_id = %self.state.client_id},
            "streaming pull manager has stopped."
        );

        // Return the instant in which the streaming manager has stopped, which is used to fulfill the manager handle
        // API (notably, tracking how long the graceful stop took.)
        Instant::now()
    }

    /// Determines whether or not we may be flow control limited from the server, by looking at the messages we
    /// have on hold.
    fn is_flow_control_limited(&self) -> bool {
        self.state.messages_on_hold.len() > self.state.flow_control.max_messages
            || self.state.messages_on_hold.size_in_bytes() > self.state.flow_control.max_bytes
    }

    /// Handles a [`StreamingPullResponse`] that has been received from pub-sub.
    ///
    /// Immediate order of business is to mod-ack those messages to set their leases properly, and then will attempt
    /// to schedule processing of the messages, respecting the `ordering_key` of the message, using [`MessagesOnHold`].
    fn handle_streaming_pull_response(&mut self, streaming_pull_response: StreamingPullResponse) {
        self.state.streaming_pull_channel_async_backoff.succeed();
        self.state.metrics.streaming_pull_response_received.incr();
        self.state
            .metrics
            .messages_received
            .incr_by(streaming_pull_response.received_messages.len() as _);

        self.transform_and_insert_messages_into_messages_on_hold(streaming_pull_response);
        self.try_flush_messages_on_hold_batch();
    }

    /// Handles a message being acked or nacked, enqueueing the ack/nack within the [`MessageAckQueue`] and
    /// scheduling any messages on hold that would have been activated by the completion of the message (if it has
    /// an `ordering_key`).
    fn handle_ack_or_nack(&mut self, ack_or_nack: AckOrNack<AckId>, latency: Duration) {
        match &ack_or_nack {
            AckOrNack::Ack(_) => {
                self.state.metrics.messages_acked.incr();
                self.state.metrics.ack_latency.record(latency)
            }
            AckOrNack::Nack(_) => {
                self.state.metrics.messages_nacked.incr();
                self.state.metrics.nack_latency.record(latency)
            }
        }

        self.state
            .message_latency_histogram
            .saturating_record(latency.as_millis() as _);

        let ack_id = *ack_or_nack.ack_id();
        self.state
            .message_ack_queue
            .enqueue_ack_or_nack(ack_or_nack);

        if let Some(Some(ordering_key)) = self.state.messages_in_flight.remove(&ack_id) {
            self.state
                .messages_on_hold
                .activate_ordering_key(&ordering_key);
        }

        // When a message is acked, or nacked, the message is removed from `messages_in_flight`,
        // this means that we may be able to process more messages, so we should try to flush a
        // batch.
        self.try_flush_messages_on_hold_batch();
    }

    /// Transforms the pub-sub messages into [`Message`]s, and inserts them into the [`MessagesOnHold`].
    fn transform_and_insert_messages_into_messages_on_hold(
        &mut self,
        streaming_pull_response: StreamingPullResponse,
    ) {
        tracing::debug!(
            {subscription = %self.state.subscription, client_id = %self.state.client_id},
            "received {} messages from pub-sub",
            streaming_pull_response.received_messages.len()
        );

        let received_at = Instant::now();
        // The rationale of this being 1 second from now is that we'll delay these by 1 second so that we can build
        // up a batch of lease renewals within the message ack queue's lease delay queue.
        let lease_renew_at = tokio::time::Instant::from(received_at + Duration::from_secs(1));

        streaming_pull_response
            .received_messages
            .into_iter()
            .filter_map(|received_message| {
                let proto_message = received_message.message?;
                Some(Message::new(
                    DetachedMessage::new(proto_message, received_message.delivery_attempt as _),
                    MessageAckGuard::new(
                        self.state.sender.clone(),
                        received_at,
                        self.state.message_ack_queue.transform_and_store_ack_id(
                            received_message.ack_id,
                            received_at,
                            lease_renew_at,
                        ),
                    ),
                ))
            })
            .for_each(|message| {
                self.state.messages_on_hold.push(message);
            });
    }

    /// Attempts to flush messages from [`MessagesOnHold`] to the [`MessageHandler`].
    fn try_flush_messages_on_hold_batch(&mut self) {
        // How many more messages we can process until we hit the max_processing_messages limit.
        let num_additional_messages_that_can_be_processed = self
            .state
            .flow_control
            .max_processing_messages
            .saturating_sub(self.state.messages_in_flight.len());

        // This has the fun property of calling Vec::with_capacity(0), which will not allocate on the heap
        // if there are no additional messages to process, and precisely allocating a vec sized to the number
        // of messages we'll be handling in this batch.
        let mut messages = Vec::with_capacity(std::cmp::min(
            self.state.messages_on_hold.num_ready_messages(),
            num_additional_messages_that_can_be_processed,
        ));

        while self.state.messages_in_flight.len() < self.state.flow_control.max_processing_messages
        {
            let message = match self.state.messages_on_hold.pop_ready() {
                Some(message) => message,
                None => break,
            };

            self.state.messages_in_flight.insert(&message);

            messages.push(message);
        }

        if messages.is_empty() {
            return;
        }

        tracing::debug!(
            {subscription = %self.state.subscription, client_id = %self.state.client_id},
            "submitting {} messages to message handler",
            messages.len()
        );

        self.state
            .metrics
            .messages_dispatched_to_handler
            .incr_by(messages.len() as _);

        self.message_handler.handle_messages(messages);
    }

    /// Reports metric gauges.
    fn handle_report_metrics(&self) {
        self.state
            .metrics
            .messages_on_hold
            .set(self.state.messages_on_hold.len() as _);

        self.state
            .metrics
            .message_bytes_on_hold
            .set(self.state.messages_on_hold.size_in_bytes() as _);

        self.state
            .metrics
            .message_ack_queue_size
            .set(self.state.message_ack_queue.len() as _);

        self.state
            .metrics
            .messages_in_flight
            .set(self.state.messages_in_flight.len() as _);

        self.state
            .metrics
            .message_bytes_in_flight
            .set(self.state.messages_in_flight.bytes_in_flight() as _);

        self.state
            .metrics
            .background_tasks_running
            .set(self.state.background_tasks.len() as _);

        self.state
            .metrics
            .message_lease_renewal_duration_secs
            .set(self.state.message_lease_renewal_duration_secs as _);

        self.state
            .metrics
            .streaming_pull_flow_control_limited
            .set(if self.is_flow_control_limited() { 1 } else { 0 });
    }

    /// Begins graceful stop process by closing out the channel, and nacking all un-processed messages.
    fn handle_graceful_stop(
        &mut self,
        sender: oneshot::Sender<Result<(), GracefulStopAlreadyRequested>>,
    ) {
        if self.state.graceful_stop_join_handles.is_some() {
            sender.send(Err(GracefulStopAlreadyRequested)).ok();
            return;
        }

        tracing::info!(
            {subscription = %self.state.subscription, client_id = %self.state.client_id},
            "graceful stop requested"
        );
        // Graceful stop first closes the channel to pub-sub, which should prevent google from sending us any more
        // messages.
        self.channel.close();

        // Now, we need to drain all the messages we have yet to submit to the message handler.
        let ack_or_nacks = self.state.messages_on_hold.drain_and_collect_all_ack_ids();

        // Create the mod-ack requests we need in order to release our lease on the messages we had on hold.
        for chunk in ack_or_nacks.chunks(ACK_IDS_MAX_BATCH_SIZE) {
            let chunk = chunk
                .into_iter()
                .filter_map(|ack_id| self.state.message_ack_queue.remove_ack_id(ack_id))
                .collect();

            let modack_request = make_modack_request(&self.state.subscription, chunk, 0);
            if modack_request.ack_ids.is_empty() {
                continue;
            }

            self.state.perform_request_in_background_with_retries(
                "nack_messages_on_hold",
                modack_request,
                |mut c, r| async move { c.modify_ack_deadline(r).await },
            )
        }

        let message_handler_graceful_stop = self.message_handler.handle_graceful_stop();
        self.state.graceful_stop_join_handles = Some(GracefulStopJoinHandles {
            message_handler: message_handler_graceful_stop,
        });
        sender.send(Ok(())).ok();
    }

    /// Returns `true` if the manager's main loop should exit.
    ///
    /// Checks to make sure all in-flight messages and acks have been flushed after a graceful stop was requested.
    pub(crate) fn should_stop(&self) -> bool {
        if self.state.graceful_stop_join_handles.is_none() {
            return false;
        }

        self.state.messages_in_flight.is_empty()
            && self.state.message_ack_queue.is_empty()
            && self.state.background_tasks.is_empty()
    }

    /// Issues modack requests to pub-sub in order to renew leases for messages that are either in-flight or on hold.
    ///
    /// Returns true if a full batch was flushed, and the next lease renewal flush should perhaps be expedited.
    fn handle_renew_leases(&mut self) {
        loop {
            self.state.recompute_message_lease_renewal_duration_secs();
            let lease_renewal_duration =
                Duration::from_secs(self.state.message_lease_renewal_duration_secs as _);

            let chunk = self
                .state
                .message_ack_queue
                .collect_ack_ids_that_need_to_be_renewed(
                    ACK_IDS_MAX_BATCH_SIZE,
                    self.state.get_buffered_lease_renewal_duration(),
                );

            if chunk.is_empty() {
                break;
            }

            let chunk_is_full = chunk.len() == ACK_IDS_MAX_BATCH_SIZE;

            tracing::debug!(
                {subscription = %self.state.subscription, client_id = %self.state.client_id},
                "renewing leases for {} messages with a lease duration of {} seconds",
                chunk.len(),
                self.state.message_lease_renewal_duration_secs,
            );

            self.state
                .metrics
                .message_leases_renewed
                .incr_by(chunk.len() as _);

            let mut ack_ids = Vec::with_capacity(chunk.len());

            for (elapsed, ack_id) in chunk {
                if elapsed > lease_renewal_duration {
                    self.state
                        .message_latency_histogram
                        .saturating_record(elapsed.as_millis() as _);
                }
                ack_ids.push(ack_id);
            }

            self.state.perform_request_in_background_with_retries(
                "renew_leases",
                make_modack_request(
                    &self.state.subscription,
                    ack_ids,
                    self.state.message_lease_renewal_duration_secs,
                ),
                |mut c, r| async move { c.modify_ack_deadline(r).await },
            );

            if !chunk_is_full {
                break;
            }
        }
    }

    /// Flushes a chunk of acks or nacks to pub-sub server.
    fn flush_ack_or_nack_chunk(&mut self, ack_or_nack_chunk: Vec<AckOrNack<String>>) {
        let (ack_request, nack_request) =
            make_ack_messages(&self.state.subscription, ack_or_nack_chunk);

        if let Some(ack_request) = ack_request {
            tracing::debug!(
                {subscription = %self.state.subscription, client_id = %self.state.client_id},
                "acking {} messages.",
                ack_request.ack_ids.len()
            );

            self.state.perform_request_in_background_with_retries(
                "ack",
                ack_request,
                |mut c, r| async move { c.acknowledge(r).await },
            );
        }

        if let Some(nack_request) = nack_request {
            tracing::debug!(
                {subscription = %self.state.subscription, client_id = %self.state.client_id},
                "nacking {} messages.",
                nack_request.ack_ids.len()
            );
            self.state.perform_request_in_background_with_retries(
                "nack",
                nack_request,
                |mut c, r| async move { c.modify_ack_deadline(r).await },
            );
        }
    }

    fn handle_streaming_pull_error(&mut self, error: Option<tonic::Status>) {
        match error {
            Some(error) => {
                tracing::error!(
                    {subscription = %self.state.subscription, client_id = %self.state.client_id, ?error},
                    "streaming pull channel encountered an error, will reconnect."
                )
            }
            None => {
                tracing::error!(
                    {subscription = %self.state.subscription, client_id = %self.state.client_id},
                    "streaming pull channel abruptly ended, will reconnect."
                )
            }
        }
        self.state.metrics.pubsub_channel_disconnected.incr();
        self.channel = self.state.create_streaming_pull_channel(true);
    }
}

fn make_modack_request(
    subscription: &str,
    ack_ids: Vec<String>,
    deadline_secs: u32,
) -> ModifyAckDeadlineRequest {
    ModifyAckDeadlineRequest {
        subscription: subscription.to_string(),
        ack_deadline_seconds: deadline_secs as _,
        ack_ids,
    }
}

/// Makes messages that need to be sent to flush the pending ack/nacks.
fn make_ack_messages(
    subscription: &str,
    ack_chunks: Vec<AckOrNack<String>>,
) -> (Option<AcknowledgeRequest>, Option<ModifyAckDeadlineRequest>) {
    let mut num_acks = 0;
    let mut num_nacks = 0;
    for ack_or_nack in &ack_chunks {
        match ack_or_nack {
            AckOrNack::Ack(_) => num_acks += 1,
            AckOrNack::Nack(_) => num_nacks += 1,
        }
    }

    let mut ack_ids = Vec::with_capacity(num_acks);
    let mut nack_ids = Vec::with_capacity(num_nacks);
    for ack_or_nack in ack_chunks {
        match ack_or_nack {
            AckOrNack::Ack(ack_id) => ack_ids.push(ack_id),
            AckOrNack::Nack(ack_id) => nack_ids.push(ack_id),
        }
    }

    let ack_request = (!ack_ids.is_empty()).then(|| AcknowledgeRequest {
        subscription: subscription.to_string(),
        ack_ids,
    });

    let nack_request = (!nack_ids.is_empty()).then(|| ModifyAckDeadlineRequest {
        subscription: subscription.to_string(),
        ack_ids: nack_ids,
        ack_deadline_seconds: 0,
    });

    (ack_request, nack_request)
}

pub(crate) enum StreamingPullManagerMessage {
    AckOrNack {
        ack_or_nack: AckOrNack,
        latency: Duration,
    },
    GracefulStop {
        sender: oneshot::Sender<Result<(), GracefulStopAlreadyRequested>>,
    },
}

/// Graceful stop has already been requested.
#[derive(Debug)]
pub(crate) struct GracefulStopAlreadyRequested;

/// Holds the tonic streaming channel, and the sender to push messages into that bi-directional channel.
pub enum StreamingPullChannel {
    /// Hackfix for: https://github.com/hyperium/tonic/issues/515
    Connecting {
        future: BoxFuture<tonic::Streaming<StreamingPullResponse>, tonic::Status>,
        heartbeater: Option<AbortOnDrop>,
    },
    /// The channel is connected.
    Connected {
        stream: tonic::Streaming<StreamingPullResponse>,
        #[allow(unused)]
        heartbeater: AbortOnDrop,
    },
    /// The channel has been closed by [`StreamingPullChannel::close`].
    ///
    /// This should only occur during shutdown.
    Closed,
}

impl StreamingPullChannel {
    fn new(
        future: BoxFuture<tonic::Streaming<StreamingPullResponse>, tonic::Status>,
        sender: UnboundedSender<StreamingPullRequest>,
    ) -> Self {
        /// How often to send heartbeats in seconds. Determined as half the period of
        /// time where the Pub/Sub server will close the stream as inactive, which is
        /// 60 seconds.
        const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

        // The heartbeater is deliberately *not* run in the streaming pull manager, incase it becomes backlogged
        // on work, we still want heartbeats to proceed accordingly.
        let heartbeater = tokio::task::spawn(async move {
            let mut interval = interval_at(
                tokio::time::Instant::now() + HEARTBEAT_INTERVAL,
                HEARTBEAT_INTERVAL,
            );
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                interval.tick().await;

                // We simply send an empty StreamingPullRequest when the heartbeat needs to be renewed.
                if sender.send(StreamingPullRequest::default()).is_err() {
                    break;
                }
            }
        })
        .into();

        Self::Connecting {
            future,
            heartbeater: Some(heartbeater),
        }
    }

    /// Closes the channel, and makes it so that `next_message` will never resolve.
    fn close(&mut self) {
        // Close the stream by simply dropping it. There doesn't appear to be any more method within tonic
        // that will explicitly "close" the stream. We simply rely on the drop handler to clean things up.
        *self = Self::Closed;
    }

    /// Polls for the next message. If the underlying stream is closed, then this method will simply
    /// never resolve and be pending perpetually.
    ///
    /// NOTE: This method is cancel-safe. Cancelling this future will not cause messages to be lost.
    async fn next_message(&mut self) -> Result<Option<StreamingPullResponse>, tonic::Status> {
        loop {
            match self {
                StreamingPullChannel::Connected { stream, .. } => return stream.message().await,
                StreamingPullChannel::Connecting {
                    future,
                    heartbeater,
                } => {
                    match future.await {
                        Ok(stream) => {
                            let heartbeater = heartbeater
                                .take()
                                .expect("invariant: heartbeater should exist");
                            *self = StreamingPullChannel::Connected {
                                stream,
                                heartbeater,
                            };
                        }
                        Err(status) => {
                            *self = StreamingPullChannel::Closed;
                            return Err(status);
                        }
                    };
                }
                StreamingPullChannel::Closed => return pending().await,
            }
        }
    }
}
