use crate::metrics::define_metrics;

define_metrics!(SmiteCoordinatorMetrics, [
    // How long an action has been processed by a worker for before it is acked/nacked
    action_outstanding_duration => StaticHistogram(),
    // How long an action has been held in the async queue for before it is sent to a worker
    action_time_in_async_queue => StaticHistogram("action_time_in_queue", ["async" => "true"]),
    // How many new connections have been established to the coordinator
    new_connection_established => StaticCounter(),

    // How many messages are currently buffered in the priority queue
    priority_queue_size_sync => StaticGauge("priority_queue_size",["type" => "sync"]),
    priority_queue_size_async => StaticGauge("priority_queue_size",["type" => "async"]),

    // How many receivers are open for the priority queue
    // can be used as a proxy for number of connections open from the smite worker
    priority_queue_receiver_count_sync => StaticGauge("priority_queue_receiver_count",["type" => "sync"]),
    priority_queue_receiver_count_async => StaticGauge("priority_queue_receiver_count",["type" => "async"]),

    client_disconnected_gracefully => StaticCounter("client_disconnected", ["error" => "false"]),
    client_disconnected_broken_pipe => StaticCounter("client_disconnected", ["error" => "broken_pipe"]),
    client_disconnected_stream_error => StaticCounter("client_disconnected", ["error" => "stream_error"]),
    client_disconnected_receiver_closed => StaticCounter("client_disconnected", ["error" => "pq_closed"]),
    client_disconnected_receiver_timeout => StaticCounter("client_disconnected", ["error" => "pq_timeout"]),

    bidi_stream_ended_no_outstanding_action => StaticCounter("bidi_stream_ended", ["outstanding_action" => "false"]),
    bidi_stream_ended_yes_outstanding_action => StaticCounter("bidi_stream_ended", ["outstanding_action" => "true"]),

    // How many sync actions have been recieved
    sync_classification_action_received => StaticCounter(),

    // How many sync actions have failed due to deserialization issues
    sync_classification_failure_deserialization => StaticCounter("sync_classification_failure", ["error" => "deserialization"]),
    // How many sync actions have failed due to the acking oneshot being dropped
    sync_classification_failure_oneshot_dropped => StaticCounter("sync_classification_failure", ["error" => "oneshot_dropped"]),
    // How many sync actions have failed because we couldn't send to the priority queue
    sync_classification_failure_pq_send => StaticCounter("sync_classification_failure", ["error" => "pq_send"]),
    sync_classification_failure_label_service => StaticCounter("sync_classification_failure", ["error" => "label_service"]),

    // How many acks for sync actions
    sync_classification_result_ack => StaticCounter("sync_classification_result", ["ack" => "true"]),
    // How many nacks for sync actions
    sync_classification_result_nack => StaticCounter("sync_classification_result", ["ack" => "false"]),

    // How many messages we've added to the async queue
    async_classification_added_to_queue => StaticCounter("async_classification_added_to_queue"),
    // How many acks for async actions
    async_classification_result_ack => StaticCounter("async_classification_result", ["ack" => "true"]),
    // How many nacks for async actions
    async_classification_result_nack => StaticCounter("async_classification_result", ["ack" => "false"]),

    // Time it took to send a message to the Async-Channel priority queue
    priority_queue_send_time_async => StaticHistogram("priority_queue_send_time", ["type" => "async"]),
    // Time it took to send a message to the sync-Channel priority queue
    priority_queue_send_time_sync => StaticHistogram("priority_queue_send_time", ["type" => "sync"]),
    // Time it took to receive a message from the async/sync priority queue
    priority_queue_receive_time => StaticHistogram("priority_queue_receive_time"),
    // Time it took for the acking receiver in the MessageHandler to receive an ack/nack
    receiver_ack_time_async => StaticHistogram("receiver_ack_time", ["type" => "async"]),
    // Time it took for the acking receiver in the MessageHandler to receive an ack/nack
    receiver_ack_time_sync => StaticHistogram("receiver_ack_time", ["type" => "sync"]),

    // How many times action ID generation from snowflake is used (when pubsub_action.id is None)
    action_id_snowflake_generation_json => StaticCounter("action_id_snowflake_generation", ["proto"=>"false"]),
    action_id_snowflake_generation_proto => StaticCounter("action_id_snowflake_generation", ["proto"=>"true"]),
]);
