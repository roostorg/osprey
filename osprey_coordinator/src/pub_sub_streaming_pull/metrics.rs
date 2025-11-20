use crate::metrics::define_metrics;

define_metrics!(StreamingPullManagerMetrics, [
    // How many messages the streaming pull subscriber has received.
    messages_received => StaticCounter(),
    // How many messages have been acked.
    messages_acked => StaticCounter(),
    // How many messages have been nacked.
    messages_nacked => StaticCounter(),
    // How many messages are currently buffered on hold.
    messages_on_hold => StaticGauge(),
    // How many bytes are on hold.
    message_bytes_on_hold => StaticGauge(),
    // How many messages are currently in-flight (aka processing)
    messages_in_flight => StaticGauge(),
    // How many messages (in terms of bytes), is currently in-flight (aka processing)
    message_bytes_in_flight => StaticGauge(),
    // How many messages were dispatched to the registered MessageHandler.
    messages_dispatched_to_handler => StaticCounter(),
    // Should be equivalent to the sum of messages on hold + messages in flight.
    message_ack_queue_size => StaticGauge(),
    // The current lease renewal duration that the streaming pull manager is using.
    message_lease_renewal_duration_secs => StaticGauge(),
    // How many message leases were renewed.
    message_leases_renewed => StaticCounter(),
    // How many background tasks the streaming pull manager is running (this is generally tasks spawned to handle
    // background retries of requests to pub-sub). Should be 0 if everything is healthy.
    background_tasks_running => StaticGauge(),
    // How many streaming pull responses have been received from the server.
    streaming_pull_response_received => StaticCounter(),
    // Set to 1 when streaming pull has reached its flow control limits as negotiated with the server.
    streaming_pull_flow_control_limited => StaticGauge(),

    // How long it takes once a message has been received for it to be acked. This counts
    // time spent in the messages on hold either because there were too many messages
    // in flight, or because ordering key activation.
    ack_latency => StaticHistogram(),
    // Same as ack_latency, but for nacks.
    nack_latency => StaticHistogram(),

    // How many times the pub-sub streaming pull channel has abruptly ended.
    pubsub_channel_disconnected => StaticCounter(),
    // How many times the pub-sub streaming pull channel has encountered an error (while trying to connect).
    pubsub_channel_connection_error => StaticCounter(),

    // Request latency of requests to the pub-sub API.
    pubsub_grpc_request_latency => DynamicHistogram([request_type]),
    // How many requests succeeded to the pub-sub API.
    pubsub_grpc_request_success => DynamicCounter([request_type]),
    // How many requests errored to the pub-sub api.
    pubsub_grpc_request_error => DynamicCounter([request_type, code]),
    // How many background requests errored so many times they were dropped.
    pubsub_grpc_background_request_dropped => DynamicCounter([request_type]),
]);
