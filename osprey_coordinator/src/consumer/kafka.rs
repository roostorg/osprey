use crate::consumer::message_consumer::{ConsumerConfig, ConsumerMessage, MessageConsumer};
use crate::consumer::message_decoder;
use crate::coordinator_metrics::OspreyCoordinatorMetrics;
use crate::metrics::counters::StaticCounter;
use crate::metrics::histograms::StaticHistogram;
use crate::priority_queue::{AckOrNack, AckableAction, PriorityQueueSender};
use crate::signals::exit_signal;
use crate::snowflake_client::SnowflakeClient;
use anyhow::Result;
use async_trait::async_trait;
use prost_types::Timestamp;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Headers, Message as KafkaRawMessage};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{timeout, Instant};

pub struct KafkaConsumer {
    consumer: StreamConsumer,
}

pub struct KafkaMessage {
    data: Vec<u8>,
    attributes: HashMap<String, String>,
    timestamp: Timestamp,
    id: String,
}

impl KafkaMessage {
    pub fn new(
        data: Vec<u8>,
        attributes: HashMap<String, String>,
        timestamp: Timestamp,
        id: String,
    ) -> Self {
        Self {
            data,
            attributes,
            timestamp,
            id,
        }
    }
}

impl ConsumerMessage for KafkaMessage {
    fn data(&self) -> &[u8] {
        &self.data
    }

    fn attributes(&self) -> &HashMap<String, String> {
        &self.attributes
    }

    fn timestamp(&self) -> Timestamp {
        self.timestamp.clone()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[async_trait]
impl MessageConsumer for KafkaConsumer {
    type Message = KafkaMessage;
    type Error = KafkaError;

    async fn receive(&mut self) -> Result<Self::Message, Self::Error> {
        let msg = self.consumer.recv().await?;

        let data = msg.payload().unwrap_or(&[]).to_vec();

        let attributes: HashMap<String, String> = msg
            .headers()
            .map(|headers| {
                headers
                    .iter()
                    .filter_map(|header| {
                        let key = header.key.to_string();
                        let value = header
                            .value
                            .and_then(|v| String::from_utf8(v.to_vec()).ok())
                            .unwrap_or_default();
                        Some((key, value))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let timestamp_millis = msg.timestamp().to_millis().unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
        });

        let timestamp = Timestamp {
            seconds: timestamp_millis / 1000,
            nanos: ((timestamp_millis % 1000) * 1_000_000) as i32,
        };

        let partition = msg.partition();
        let offset = msg.offset();
        let id = format!("kafka-{}-{}", partition, offset);

        Ok(KafkaMessage::new(data, attributes, timestamp, id))
    }

    async fn ack(&self, _message: &Self::Message) -> Result<(), Self::Error> {
        self.consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Async)?;
        Ok(())
    }

    async fn nack(&self, _message: &Self::Message) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl KafkaConsumer {
    pub async fn new() -> Result<Self> {
        let input_topic = std::env::var("OSPREY_KAFKA_INPUT_STREAM_TOPIC")
            .unwrap_or("osprey.actions_input".to_string());
        let input_bootstrap_servers =
            std::env::var("OSPREY_KAFKA_BOOTSTRAP_SERVERS").unwrap_or("localhost:9092".to_string());
        let group_id = std::env::var("OSPREY_KAFKA_GROUP_ID")
            .unwrap_or("osprey_coordinator_group".to_string());

        tracing::info!(
            "Creating Kafka consumer for topic: {} with bootstrap servers: {}",
            input_topic,
            input_bootstrap_servers
        );

        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", &group_id)
            .set("bootstrap.servers", &input_bootstrap_servers)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create::<StreamConsumer>()?;

        consumer.subscribe(&[&input_topic])?;

        Ok(Self { consumer })
    }
}

pub async fn start_kafka_consumer(
    snowflake_client: Arc<SnowflakeClient>,
    priority_queue_sender: PriorityQueueSender,
    metrics: Arc<OspreyCoordinatorMetrics>,
) -> Result<()> {
    tracing::info!("Kafka consumer starting...");

    let mut consumer = KafkaConsumer::new().await?;
    let config = ConsumerConfig::default();

    loop {
        tokio::select! {
            _ = exit_signal() => {
                tracing::info!("Received exit signal, shutting down Kafka consumer");
                return Ok(());
            }
            message_result = consumer.receive() => {
                let message = match message_result {
                    Ok(msg) => msg,
                    Err(e) => {
                        tracing::error!({error = %e}, "[kafka] error receiving message");
                        continue;
                    }
                };

                let ack_id: u64 = rand::Rng::gen(&mut rand::thread_rng());
                let message_id = message.id();

                let action = match message.attributes().get("encoding").map(|s| s.as_str()) {
                    Some("proto") => {
                        message_decoder::decode_proto_message(
                            message.data(),
                            ack_id,
                            message.timestamp(),
                            &snowflake_client,
                            &metrics,
                        )
                        .await
                    }
                    _ => {
                        message_decoder::decode_msgpack_json_message(
                            message.data(),
                            ack_id,
                            message.timestamp(),
                            &snowflake_client,
                            &metrics,
                        )
                        .await
                    }
                };

                let action = match action {
                    Ok(action) => action,
                    Err(e) => {
                        tracing::error!(
                            {error = %e, ack_id = %ack_id, message_id = %message_id},
                            "[kafka] failed to decode message"
                        );
                        if let Err(nack_err) = consumer.nack(&message).await {
                            tracing::error!(
                                {error = %nack_err, message_id = %message_id},
                                "[kafka] failed to nack message"
                            );
                        }
                        continue;
                    }
                };

                let (ackable_action, acking_receiver) = AckableAction::new(action);

                tracing::debug!(
                    {ack_id = %ack_id, message_id = %message_id},
                    "[kafka] received message"
                );

                let send_start_time = Instant::now();
                match timeout(
                    config.max_time_to_send_to_async_queue,
                    priority_queue_sender.send_async(ackable_action),
                )
                .await
                {
                    Ok(Ok(())) => {
                        tracing::debug!(
                            {message_id = %message_id, ack_id = %ack_id},
                            "[kafka] sent message to priority queue"
                        );
                        metrics.async_classification_added_to_queue.incr();
                    }
                    Ok(Err(e)) => {
                        tracing::error!(
                            {error = %e, message_id = %message_id},
                            "[kafka] priority queue send error"
                        );
                        if let Err(nack_err) = consumer.nack(&message).await {
                            tracing::error!(
                                {error = %nack_err, message_id = %message_id},
                                "[kafka] failed to nack message"
                            );
                        }
                        continue;
                    }
                    Err(_) => {
                        tracing::error!(
                            {message_id = %message_id},
                            "[kafka] sending to priority queue timed out"
                        );
                        if let Err(nack_err) = consumer.nack(&message).await {
                            tracing::error!(
                                {error = %nack_err, message_id = %message_id},
                                "[kafka] failed to nack message"
                            );
                        }
                        continue;
                    }
                }
                metrics
                    .priority_queue_send_time_async
                    .record(send_start_time.elapsed());

                tracing::debug!(
                    {message_id = %message_id, ack_id = %ack_id},
                    "[kafka] waiting on ack or nack"
                );

                let receive_start_time = Instant::now();
                match timeout(config.max_acking_receiver_wait_time, acking_receiver).await {
                    Ok(Ok(ack_or_nack)) => match ack_or_nack {
                        AckOrNack::Ack(_) => {
                            tracing::debug!(
                                {message_id = %message_id, ack_id = %ack_id},
                                "[kafka] acking message"
                            );
                            metrics.async_classification_result_ack.incr();
                            metrics
                                .receiver_ack_time_async
                                .record(receive_start_time.elapsed());

                            if let Err(e) = consumer.ack(&message).await {
                                tracing::error!(
                                    {error = %e, message_id = %message_id},
                                    "[kafka] failed to ack message"
                                );
                            }
                        }
                        AckOrNack::Nack => {
                            tracing::debug!(
                                {message_id = %message_id, ack_id = %ack_id},
                                "[kafka] nacking message"
                            );
                            metrics.async_classification_result_nack.incr();
                            metrics
                                .receiver_ack_time_async
                                .record(receive_start_time.elapsed());

                            if let Err(e) = consumer.nack(&message).await {
                                tracing::error!(
                                    {error = %e, message_id = %message_id},
                                    "[kafka] failed to nack message"
                                );
                            }
                        }
                    },
                    Ok(Err(recv_error)) => {
                        tracing::error!(
                            {message_id = %message_id, recv_error = %recv_error, ack_id = %ack_id},
                            "[kafka] acking sender dropped"
                        );
                        metrics
                            .receiver_ack_time_async
                            .record(receive_start_time.elapsed());

                        if let Err(e) = consumer.nack(&message).await {
                            tracing::error!(
                                {error = %e, message_id = %message_id},
                                "[kafka] failed to nack message"
                            );
                        }
                    }
                    Err(_) => {
                        tracing::error!(
                            {message_id = %message_id, ack_id = %ack_id},
                            "[kafka] waiting for ack/nack timed out"
                        );
                        metrics
                            .receiver_ack_time_async
                            .record(receive_start_time.elapsed());

                        if let Err(e) = consumer.nack(&message).await {
                            tracing::error!(
                                {error = %e, message_id = %message_id},
                                "[kafka] failed to nack message"
                            );
                        }
                    }
                }
            }
        }
    }
}
