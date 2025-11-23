use crate::{
    coordinator_metrics::OspreyCoordinatorMetrics,
    metrics::counters::StaticCounter,
    metrics::histograms::StaticHistogram,
    priority_queue::{AckOrNack, AckableAction, PriorityQueueSender},
    proto::{self, osprey_coordinator_action::ActionData, osprey_coordinator_action::SecretData},
    signals::exit_signal,
    snowflake_client::SnowflakeClient,
};
use anyhow::{anyhow, Result};
use convert_case::{Case, Casing};
use prost::Message as ProstMessage;
use prost_types::Timestamp;
use rand::Rng;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedHeaders, Headers, Message};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{timeout, Duration as TokioDuration, Instant};

use crate::proto::Action as OspreyProtoAction;

async fn decode_proto_message(
    message_data: &[u8],
    ack_id: u64,
    message_timestamp: Timestamp,
    snowflake_client: &SnowflakeClient,
    metrics: &OspreyCoordinatorMetrics,
) -> Result<proto::OspreyCoordinatorAction> {
    let osprey_proto_action = OspreyProtoAction::decode(message_data)?;
    let action_id = if osprey_proto_action.id == 0 {
        metrics.action_id_snowflake_generation_proto.incr();
        snowflake_client.generate_id().await?
    } else {
        osprey_proto_action.id
    };
    let action_name = osprey_proto_action
        .data
        .ok_or_else(|| anyhow!("missing action data"))?
        .to_string()
        .to_case(Case::Snake);
    Ok(proto::OspreyCoordinatorAction {
        ack_id,
        action_id,
        action_name,
        action_data: Some(ActionData::ProtoActionData(message_data.into())),
        secret_data: None,
        timestamp: Some(message_timestamp),
    })
}

async fn decode_msgpack_json_message(
    message_data: &[u8],
    ack_id: u64,
    message_timestamp: Timestamp,
    snowflake_client: &SnowflakeClient,
    metrics: &OspreyCoordinatorMetrics,
) -> Result<proto::OspreyCoordinatorAction> {
    #[derive(Deserialize, Debug)]
    struct KafkaAction {
        action_id: Option<String>,
        action_name: String,
        data: Value,
        secret_data: Option<Value>,
    }

    let root: Value = serde_json::from_slice(message_data)?;
    let kafka_action: KafkaAction =
        serde_json::from_value(root.get("data").cloned().unwrap_or_default())?;

    let serde_json_vec = serde_json::to_vec(&kafka_action.data)?;
    let optional_secret_data = match &kafka_action.secret_data {
        Some(secret_data) => Some(SecretData::JsonSecretData(serde_json::to_vec(secret_data)?)),
        _ => None,
    };

    let action_id = match kafka_action.action_id {
        Some(id) => id.parse::<u64>()?,
        None => {
            metrics.action_id_snowflake_generation_json.incr();
            snowflake_client.generate_id().await?
        }
    };

    Ok(proto::OspreyCoordinatorAction {
        ack_id,
        action_id,
        action_name: kafka_action.action_name,
        action_data: Some(ActionData::JsonActionData(serde_json_vec)),
        secret_data: optional_secret_data,
        timestamp: Some(message_timestamp),
    })
}

async fn create_action_from_kafka_message(
    message_data: &[u8],
    message_headers: Option<&BorrowedHeaders>,
    ack_id: u64,
    message_timestamp: Timestamp,
    snowflake_client: &SnowflakeClient,
    metrics: &OspreyCoordinatorMetrics,
) -> Result<proto::OspreyCoordinatorAction> {
    let headers_map: HashMap<String, String> = message_headers
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

    match headers_map.get("encoding") {
        Some(encoding) if encoding == "proto" => {
            decode_proto_message(
                message_data,
                ack_id,
                message_timestamp,
                snowflake_client,
                metrics,
            )
            .await
        }
        _ => {
            decode_msgpack_json_message(
                message_data,
                ack_id,
                message_timestamp,
                snowflake_client,
                metrics,
            )
            .await
        }
    }
}

async fn create_kafka_consumer() -> Result<StreamConsumer> {
    let input_topic = std::env::var("OSPREY_KAFKA_INPUT_STREAM_TOPIC")
        .unwrap_or("osprey.actions_input".to_string());
    let input_bootstrap_servers =
        std::env::var("OSPREY_KAFKA_BOOTSTRAP_SERVERS").unwrap_or("localhost:9092".to_string());
    let group_id =
        std::env::var("OSPREY_KAFKA_GROUP_ID").unwrap_or("osprey_coordinator_group".to_string());

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

    Ok(consumer)
}

pub async fn start_kafka_consumer(
    snowflake_client: Arc<SnowflakeClient>,
    priority_queue_sender: PriorityQueueSender,
    metrics: Arc<OspreyCoordinatorMetrics>,
) -> Result<()> {
    tracing::info!("Kafka consumer starting...");
    let consumer = create_kafka_consumer().await?;

    let max_time_to_send_to_async_queue = TokioDuration::from_millis(
        std::env::var("MAX_TIME_TO_SEND_TO_ASYNC_QUEUE_MS")
            .unwrap_or("500".to_string())
            .parse::<u64>()
            .unwrap(),
    );
    let max_acking_receiver_wait_time = TokioDuration::from_millis(
        std::env::var("MAX_ACKING_RECEIVER_WAIT_TIME_MS")
            .unwrap_or("60000".to_string())
            .parse::<u64>()
            .unwrap(),
    );

    loop {
        tokio::select! {
            _ = exit_signal() => {
                tracing::info!("Received exit signal, shutting down Kafka consumer");
                break;
            }
            message_result = consumer.recv() => {
                match message_result {
                    Err(e) => {
                        tracing::error!("Kafka error: {}", e);
                        continue;
                    }
                    Ok(message) => {
                        let metrics = metrics.clone();
                        let priority_queue_sender = priority_queue_sender.clone();
                        let snowflake_client = snowflake_client.clone();

                        let payload = message.payload().expect("kafka message must have payload");
                        let ack_id: u64 = rand::thread_rng().gen();

                        let timestamp_millis = message.timestamp().to_millis().unwrap_or_else(|| {
                            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
                        });

                        let message_timestamp = Timestamp {
                            seconds: timestamp_millis / 1000,
                            nanos: ((timestamp_millis % 1000) * 1_000_000) as i32,
                        };

                        let action_result = create_action_from_kafka_message(
                            payload,
                            message.headers(),
                            ack_id,
                            message_timestamp,
                            snowflake_client.as_ref(),
                            &metrics,
                        )
                        .await;

                        match action_result {
                            Ok(action) => {
                                let (ackable_action, acking_receiver) = AckableAction::new(action);

                                tracing::info!({ack_id = %ack_id}, "[kafka] received kafka message");
                                let send_start_time = Instant::now();
                                match timeout(
                                    max_time_to_send_to_async_queue,
                                    priority_queue_sender.send_async(ackable_action),
                                )
                                .await
                                {
                                    Ok(Ok(())) => {
                                        tracing::info!({ack_id=ack_id}, "[kafka] sent kafka message to priority queue");
                                        metrics.async_classification_added_to_queue.incr();
                                    }
                                    Ok(Err(e)) => {
                                        tracing::error!({error=%e},"[kafka] priority queue send error");
                                        continue;
                                    }
                                    Err(_) => {
                                        tracing::error!({ack_id=ack_id}, "[kafka] sending to priority queue timed out");
                                        continue;
                                    }
                                };
                                metrics.priority_queue_send_time_async.record(send_start_time.elapsed());
                                tracing::info!({ack_id=ack_id},"[kafka] waiting on ack or nack");

                                let receive_start_time = Instant::now();
                                match timeout(max_acking_receiver_wait_time, acking_receiver).await {
                                    Ok(Ok(ack_or_nack)) => match ack_or_nack {
                                        AckOrNack::Ack(_optional_execution_result) => {
                                            tracing::info!({ack_id=ack_id},"[kafka] acking message");
                                            metrics.async_classification_result_ack.incr();
                                            metrics.receiver_ack_time_async.record(receive_start_time.elapsed());
                                            if let Err(e) = consumer.commit_message(
                                                &message,
                                                rdkafka::consumer::CommitMode::Async,
                                            ) {
                                                tracing::error!({error=%e, ack_id=ack_id}, "[kafka] failed to commit message");
                                            }
                                        }
                                        AckOrNack::Nack => {
                                            tracing::info!({ack_id=ack_id},"[kafka] nacking message");
                                            metrics.async_classification_result_nack.incr();
                                            metrics.receiver_ack_time_async.record(receive_start_time.elapsed());
                                        }
                                    },
                                    Ok(Err(recv_error)) => {
                                        tracing::error!({recv_error=%recv_error, ack_id=ack_id},"[kafka] acking sender dropped");
                                        metrics.receiver_ack_time_async.record(receive_start_time.elapsed());
                                    }
                                    Err(_) => {
                                        tracing::error!({ack_id=ack_id}, "[kafka] waiting for ack/nack timed out");
                                        metrics.receiver_ack_time_async.record(receive_start_time.elapsed());
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!({error=%e, ack_id=ack_id}, "[kafka] failed to create action from message");
                            }
                        }
                    }
                }
            }
        }
    }

    Result::Ok(())
}
