use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::consumer::message_consumer::{ConsumerConfig, ConsumerMessage};
use crate::gcloud::grpc::connection::Connection;
use crate::gcloud::{
    auth::AuthorizationHeaderInterceptor,
    gcp_metadata::GCPMetadataClient,
    google::pubsub::v1::subscriber_client::SubscriberClient,
    kms::{AesGcmEnvelope, GOOGLE_KMS_DOMAIN},
    pubsub::{PubSubSubscription, GOOGLE_PUBSUB_DOMAIN},
};
use crate::metrics::counters::StaticCounter;
use crate::metrics::histograms::StaticHistogram;
use crate::metrics::MetricsClientBuilder;
use crate::{
    consumer::message_decoder,
    coordinator_metrics::OspreyCoordinatorMetrics,
    priority_queue::{AckOrNack, AckableAction, PriorityQueueSender},
    proto,
    pub_sub_streaming_pull::DetachedMessage,
    pub_sub_streaming_pull::{FlowControl, SpawnTaskPerMessageHandler, StreamingPullManager},
};
use anyhow::{anyhow, Result};
use prost_types::Timestamp;
use rand::Rng;
use tokio::time::{timeout, Instant};
use tonic::{codegen::InterceptedService, transport::Channel};

use crate::signals::exit_signal;
use crate::snowflake_client::SnowflakeClient;

pub struct PubSubMessage {
    inner: DetachedMessage,
}

impl ConsumerMessage for PubSubMessage {
    fn data(&self) -> &[u8] {
        &self.inner.data
    }

    fn attributes(&self) -> &HashMap<String, String> {
        self.inner.attributes()
    }

    fn timestamp(&self) -> Timestamp {
        self.inner.publish_time()
    }

    fn id(&self) -> String {
        self.inner.message_id.clone()
    }
}

impl From<DetachedMessage> for PubSubMessage {
    fn from(msg: DetachedMessage) -> Self {
        PubSubMessage { inner: msg }
    }
}

async fn decrypt_pubsub_message(
    kms_envelope: Arc<AesGcmEnvelope>,
    message_data: &[u8],
) -> Result<Vec<u8>> {
    kms_envelope
        .decrypt(message_data)
        .await
        .map_err(|err| anyhow!("message decryption failed: {}", err.to_string()))
}

async fn create_action_from_pubsub_message(
    kms_envelope: Arc<AesGcmEnvelope>,
    message_data: &[u8],
    message_attributes: &HashMap<String, String>,
    ack_id: u64,
    message_timestamp: Timestamp,
    snowflake_client: &SnowflakeClient,
    metrics: &OspreyCoordinatorMetrics,
) -> Result<proto::OspreyCoordinatorAction> {
    let decrypted_message_vector = match message_attributes.get("encrypted") {
        Some(is_encrypted) if is_encrypted == "true" => {
            Some(decrypt_pubsub_message(kms_envelope, message_data).await?)
        }
        _ => None,
    };
    let message_data = match &decrypted_message_vector {
        Some(data) => &data[..],
        None => message_data,
    };

    match message_attributes.get("encoding") {
        Some(encoding) if encoding == "proto" => {
            message_decoder::decode_proto_message(
                message_data,
                ack_id,
                message_timestamp,
                snowflake_client,
                metrics,
            )
            .await
        }
        _ => {
            message_decoder::decode_msgpack_json_message(
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

async fn create_pubsub_subscription_client(
) -> SubscriberClient<InterceptedService<Channel, AuthorizationHeaderInterceptor>> {
    let emulator_host = std::env::var("PUBSUB_EMULATOR_HOST").ok();

    let timeout = Duration::from_secs(5);

    if let Some(emulator_host) = emulator_host {
        tracing::info!("Creating subscription client to emulator");
        Connection::new_no_auth(
            format!("http://{}", emulator_host).try_into().unwrap(),
            timeout,
        )
        .create_subscriber_client()
    } else {
        tracing::info!("Creating subscription client to real pubsub");
        let service_account =
            std::env::var("OSPREY_COORDINATOR_SERVICE_ACCOUNT").unwrap_or("default".to_string());
        let client = GCPMetadataClient::new(service_account).unwrap();
        Connection::from_metadata_client(
            client,
            timeout,
            Duration::from_secs(24000),
            GOOGLE_PUBSUB_DOMAIN,
        )
        .await
        .unwrap()
        .create_subscriber_client()
    }
}

pub async fn start_pubsub_subscriber(
    snowflake_client: Arc<SnowflakeClient>,
    priority_queue_sender: PriorityQueueSender,
    metrics: Arc<OspreyCoordinatorMetrics>,
) -> Result<()> {
    let subscriber_client = create_pubsub_subscription_client().await;
    let subscription_name = {
        let project_id =
            std::env::var("PUBSUB_SUBSCRIPTION_PROJECT_ID").unwrap_or("osprey-dev".to_string());

        let subscription_id = std::env::var("PUBSUB_SUBSCRIPTION_ID")
            .unwrap_or("osprey-coordinator-actions".to_string());

        PubSubSubscription::new(project_id, subscription_id)
    };

    let kek_uri = std::env::var("PUBSUB_ENCRYPTION_KEY_URI").unwrap_or("".to_string());

    let kms_envelope = Connection::from_metadata_client(
        GCPMetadataClient::new("default".into())?,
        Duration::from_secs(5),
        Duration::from_secs(24000),
        GOOGLE_KMS_DOMAIN,
    )
    .await?
    .create_kms_aes_gcm_envelope(kek_uri, Vec::new(), true)?;

    let kms_envelope = Arc::new(kms_envelope);
    let max_messages = std::env::var("PUBSUB_MAX_MESSAGES")
        .unwrap_or("5000".to_string())
        .parse::<usize>()
        .unwrap();
    let max_processing_messages = std::env::var("PUBSUB_MAX_PROCESSING_MESSAGES")
        .unwrap_or("5000".to_string())
        .parse::<usize>()
        .unwrap();

    let config = ConsumerConfig::default();
    let max_time_to_send_to_async_queue = config.max_time_to_send_to_async_queue;
    let max_acking_receiver_wait_time = config.max_acking_receiver_wait_time;

    tracing::info!(
        {subscription_name = %subscription_name},
        "creating streaming pull manager"
    );
    let flow_control = FlowControl::default()
        .set_max_messages(max_messages)
        .set_max_processing_messages(max_processing_messages)
        .set_max_bytes(1024 * 1024 * 1024);
    StreamingPullManager::new(
        subscriber_client,
        subscription_name,
        flow_control,
        SpawnTaskPerMessageHandler::new(move |message: DetachedMessage| {
            let metrics = metrics.clone();
            let pubsub_message = PubSubMessage::from(message);
            let message_id = pubsub_message.id();
            let priority_queue_sender = priority_queue_sender.clone();
            let snowflake_client = snowflake_client.clone();
            let kms_envelope = kms_envelope.clone();

            async move {
                let ack_id: u64 = rand::thread_rng().gen();

                let action = create_action_from_pubsub_message(
                    kms_envelope,
                    pubsub_message.data(),
                    pubsub_message.attributes(),
                    ack_id,
                    pubsub_message.timestamp(),
                    snowflake_client.as_ref(),
                    &metrics,
                )
                .await
                .map_err(|_| ())?;

                let (ackable_action, acking_receiver) = AckableAction::new(action);

                tracing::debug!(
                    {ack_id = %ack_id, message_id = %message_id},
                    "[pubsub] received message"
                );

                let send_start_time = Instant::now();
                match timeout(
                    max_time_to_send_to_async_queue,
                    priority_queue_sender.send_async(ackable_action),
                )
                .await
                {
                    Ok(Ok(())) => {
                        tracing::debug!(
                            {message_id = %message_id, ack_id = %ack_id},
                            "[pubsub] sent message to priority queue"
                        );
                        metrics.async_classification_added_to_queue.incr();
                    }
                    Ok(Err(e)) => {
                        tracing::error!(
                            {error = %e, message_id = %message_id},
                            "[pubsub] priority queue send error"
                        );
                        return Err(());
                    }
                    Err(_) => {
                        tracing::error!(
                            {message_id = %message_id},
                            "[pubsub] sending to priority queue timed out"
                        );
                        return Err(());
                    }
                }
                metrics
                    .priority_queue_send_time_async
                    .record(send_start_time.elapsed());

                tracing::debug!(
                    {message_id = %message_id, ack_id = %ack_id},
                    "[pubsub] waiting on ack or nack"
                );

                let receive_start_time = Instant::now();
                match timeout(max_acking_receiver_wait_time, acking_receiver).await {
                    Ok(Ok(ack_or_nack)) => match ack_or_nack {
                        AckOrNack::Ack(_) => {
                            tracing::debug!(
                                {message_id = %message_id, ack_id = %ack_id},
                                "[pubsub] acking message"
                            );
                            metrics.async_classification_result_ack.incr();
                            metrics
                                .receiver_ack_time_async
                                .record(receive_start_time.elapsed());
                            Ok(())
                        }
                        AckOrNack::Nack => {
                            tracing::debug!(
                                {message_id = %message_id, ack_id = %ack_id},
                                "[pubsub] nacking message"
                            );
                            metrics.async_classification_result_nack.incr();
                            metrics
                                .receiver_ack_time_async
                                .record(receive_start_time.elapsed());
                            Err(())
                        }
                    },
                    Ok(Err(recv_error)) => {
                        tracing::error!(
                            {message_id = %message_id, recv_error = %recv_error, ack_id = %ack_id},
                            "[pubsub] acking sender dropped"
                        );
                        metrics
                            .receiver_ack_time_async
                            .record(receive_start_time.elapsed());
                        Err(())
                    }
                    Err(_) => {
                        tracing::error!(
                            {message_id = %message_id, ack_id = %ack_id},
                            "[pubsub] waiting for ack/nack timed out"
                        );
                        metrics
                            .receiver_ack_time_async
                            .record(receive_start_time.elapsed());
                        Err(())
                    }
                }
            }
        }),
        MetricsClientBuilder::new("osprey_coordinator.pull"),
    )
    .gracefully_stop_on_signal(exit_signal(), Duration::from_secs(30))
    .await;
    Result::Ok(())
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use prost_types::Timestamp;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::create_action_from_pubsub_message;
    use crate::coordinator_metrics::OspreyCoordinatorMetrics;
    use crate::proto;
    use crate::snowflake_client::SnowflakeClient;

    #[tokio::test]
    async fn test_create_action_from_pubsub_message_1() {
        use crate::gcloud::grpc::connection::Connection;
        use msgpack_simple::MsgPack;

        let action_json = json!({
            "id": "123456789",
            "name": "guild_invite_created",
            "data": {
                "char": "abc",
                "int": 1i64,
                "float2": 1.1_f64
            },
        });
        let encoded = MsgPack::String(action_json.to_string()).encode();

        let snowflake = SnowflakeClient::new("http://localhost:8088".to_string());
        let metrics = OspreyCoordinatorMetrics::new();
        // Create a mock KMS envelope (won't be used since encrypted != true)
        let connection = Connection::new_no_auth(
            "http://localhost:8080".try_into().unwrap(),
            std::time::Duration::from_secs(5),
        );
        let kms_envelope = Arc::new(
            connection
                .create_kms_aes_gcm_envelope("gcp-kms://test".to_string(), Vec::new(), false)
                .unwrap(),
        );

        let attributes = HashMap::new();

        let prost_action = create_action_from_pubsub_message(
            kms_envelope,
            encoded.as_slice(),
            &attributes,
            12344242,
            Timestamp::default(),
            &snowflake,
            &metrics,
        )
        .await;

        println!("{:?}", prost_action);
        assert!(prost_action.is_ok(), "prost action decoding failed");
        let prost_action = prost_action.unwrap();
        assert_eq!(prost_action.action_name, "guild_invite_created");

        let action_data = prost_action
            .action_data
            .expect("action_data should be present");
        let proto::osprey_coordinator_action::ActionData::JsonActionData(json_bytes) = action_data
        else {
            panic!()
        };
        let data: serde_json::Value = serde_json::from_slice(&json_bytes).unwrap();
        assert_eq!(data["char"], "abc");
        assert_eq!(data["int"], 1);
        assert_eq!(data["float2"], 1.1);
    }

    #[tokio::test]
    async fn test_create_action_from_pubsub_message_2() {
        use crate::gcloud::grpc::connection::Connection;
        use msgpack_simple::MsgPack;

        let action_json = json!({
            "id": "123456789",
            "name": "guild_invite_created",
            "data": {
                "char": "abc",
                "int": 1i64,
                "float2": 1.1_f64
            },
        });

        let encoded = MsgPack::String(action_json.to_string()).encode();

        let snowflake = SnowflakeClient::new("http://localhost:8088".to_string());
        let metrics = OspreyCoordinatorMetrics::new();
        let connection = Connection::new_no_auth(
            "http://localhost:8080".try_into().unwrap(),
            std::time::Duration::from_secs(5),
        );
        let kms_envelope = Arc::new(
            connection
                .create_kms_aes_gcm_envelope("gcp-kms://test".to_string(), Vec::new(), false)
                .unwrap(),
        );
        let attributes = HashMap::new();

        let prost_action = create_action_from_pubsub_message(
            kms_envelope,
            encoded.as_slice(),
            &attributes,
            12344242,
            Timestamp::default(),
            &snowflake,
            &metrics,
        )
        .await;

        println!("{:?}", prost_action);
        assert!(prost_action.is_ok(), "prost action decoding failed");
        let prost_action = prost_action.unwrap();
        assert_eq!(prost_action.action_name, "guild_invite_created");

        let proto::osprey_coordinator_action::ActionData::JsonActionData(json_bytes) = prost_action
            .action_data
            .expect("action_data should be present")
        else {
            panic!("Expected JsonActionData variant")
        };

        let data: serde_json::Value = serde_json::from_slice(&json_bytes).unwrap();
        assert_eq!(data["char"], "abc");
        assert_eq!(data["int"], 1);
        assert_eq!(data["float2"], 1.1);
    }

    #[tokio::test]
    async fn test_create_action_from_pubsub_proto_action() {
        use crate::gcloud::grpc::connection::Connection;
        use std::fs::File;
        use std::io::Read;

        let mut file = File::open("test_data/pubsub_proto_message.json").unwrap();
        let mut data = String::new();
        file.read_to_string(&mut data).unwrap();
        let json: serde_json::Value = serde_json::from_str(&data).unwrap();
        let action_jsons = json.as_array().expect("was not array");
        let action_json = action_jsons[0].as_object().expect("is not map");

        let action_data_str = action_json
            .get("message")
            .unwrap()
            .as_object()
            .unwrap()
            .get("data")
            .unwrap()
            .as_str()
            .unwrap();

        let action_bytes = base64::engine::general_purpose::STANDARD
            .decode(action_data_str)
            .unwrap();

        let snowflake = SnowflakeClient::new("http://localhost:8088".to_string());
        let metrics = OspreyCoordinatorMetrics::new();
        let connection = Connection::new_no_auth(
            "http://localhost:8080".try_into().unwrap(),
            std::time::Duration::from_secs(5),
        );
        let kms_envelope = Arc::new(
            connection
                .create_kms_aes_gcm_envelope("gcp-kms://test".to_string(), Vec::new(), false)
                .unwrap(),
        );
        let mut attributes = HashMap::new();
        attributes.insert("encoding".to_string(), "proto".to_string());

        let prost_action = create_action_from_pubsub_message(
            kms_envelope,
            action_bytes.as_slice(),
            &attributes,
            12344242,
            Timestamp::default(),
            &snowflake,
            &metrics,
        )
        .await;

        assert!(prost_action.is_ok(), "proto action decoding failed");
        let prost_action = prost_action.unwrap();

        // Validate we got ProtoActionData (not JsonActionData) and the bytes match input
        let proto::osprey_coordinator_action::ActionData::ProtoActionData(proto_bytes) =
            prost_action
                .action_data
                .expect("action_data should be present")
        else {
            panic!("Expected ProtoActionData variant")
        };
        assert_eq!(proto_bytes, action_bytes);
    }
}
