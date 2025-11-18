use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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
    coordinator_metrics::OspreyCoordinatorMetrics,
    priority_queue::{AckOrNack, AckableAction, PriorityQueueSender},
    proto::{self, osprey_coordinator_action::SecretData},
    pub_sub_streaming_pull::DetachedMessage,
    pub_sub_streaming_pull::{FlowControl, SpawnTaskPerMessageHandler, StreamingPullManager},
};
use anyhow::{anyhow, Result};
use msgpack_simple::MsgPack;
use prost::Message;
use prost_types::Timestamp;
use rand::Rng;
use serde::Deserialize;
use serde_json::Value;
use tokio::time::{timeout, Duration as TokioDuration, Instant};
use tonic::{codegen::InterceptedService, transport::Channel};

use crate::proto::Action as OspreyProtoAction;
use crate::signals::exit_signal;
use crate::snowflake_client::SnowflakeClient;
use convert_case::{Case, Casing};
use proto::osprey_coordinator_action::ActionData;

async fn decode_proto_message(
    message_data: &[u8],
    ack_id: u64,
    message_timestamp: Timestamp,
    snowflake_client: &SnowflakeClient,
    metrics: &OspreyCoordinatorMetrics,
) -> Result<proto::OspreyCoordinatorAction> {
    let osprey_proto_action = OspreyProtoAction::decode(message_data).unwrap();
    let action_id = if osprey_proto_action.id == 0 {
        metrics.action_id_snowflake_generation_proto.incr();
        snowflake_client.generate_id().await?
    } else {
        osprey_proto_action.id
    };
    let action_name = osprey_proto_action
        .data
        .unwrap()
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
    // This whole function can probably be optimized way better, but in the interest of time I am leaving
    // it in a working state for now.
    #[derive(Deserialize, Debug)]
    struct PubsubAction {
        id: Option<String>,
        name: String,
        data: Value,
        secret_data: Option<Value>,
    }

    let decoded = MsgPack::parse(message_data)?;
    let decoded = decoded.as_string()?;
    let pubsub_action: PubsubAction = serde_json::from_str(decoded.as_str())?;

    let serde_json_vec = serde_json::to_vec(&pubsub_action.data)?;
    let optional_secret_data = match &pubsub_action.secret_data {
        Some(secret_data) => Some(SecretData::JsonSecretData(serde_json::to_vec(secret_data)?)),
        _ => None,
    };

    // old msgpack parsing
    // let mut out = Vec::with_capacity(1024 * 6);
    // let mut de = serde_json::Deserializer::from_slice(serde_json_vec.as_slice());
    // let mut se = rmp_serde::Serializer::new(&mut out);
    // serde_transcode::transcode(&mut de, &mut se).unwrap();

    let action_id = match pubsub_action.id {
        Some(id) => id.parse::<u64>()?,
        None => {
            metrics.action_id_snowflake_generation_json.incr();
            snowflake_client.generate_id().await?
        }
    };

    Ok(proto::OspreyCoordinatorAction {
        ack_id,
        action_id,
        action_name: pubsub_action.name,
        action_data: Some(ActionData::JsonActionData(serde_json_vec)),
        secret_data: optional_secret_data,
        timestamp: Some(message_timestamp),
    })
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
    let subscription_name = if std::env::var("PUBSUB_EMULATOR_HOST").is_ok() {
        PubSubSubscription::new("discord-dev", "rules-sink")
    } else {
        let project_id = std::env::var("PUBSUB_SUBSCRIPTION_PROJECT_ID")
            .unwrap_or("discord-anti-abuse-prd".to_string());

        let subscription_id = std::env::var("PUBSUB_SUBSCRIPTION_ID")
            .unwrap_or("osprey-coordinator-test".to_string());

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
            let message_id = message.message_id.clone();
            let priority_queue_sender = priority_queue_sender.clone();
            let snowflake_client = snowflake_client.clone();
            let kms_envelope = kms_envelope.clone();
            async move {
                let message_attributes = message.attributes();

                let ack_id: u64 = {
                    let mut rng = rand::thread_rng();
                    rng.gen()
                };

                let action = create_action_from_pubsub_message(
                    kms_envelope,
                    message.data.as_slice(),
                    message_attributes,
                    ack_id,
                    message.publish_time(),
                    snowflake_client.as_ref(),
                    &metrics,
                ).await
                .map_err(|_| ())?;
                let (ackable_action, acking_receiver) = AckableAction::new(action);

                tracing::debug!({ack_id = %ack_id, message_id=%message_id}, "[pubsub] received pubsub message");
                let send_start_time = Instant::now();
                match timeout(max_time_to_send_to_async_queue, priority_queue_sender.send_async(ackable_action)).await {
                    Ok(Ok(())) => {
                        tracing::debug!({message_id=%message_id, ack_id=ack_id}, "[pubsub] sent pubsub message to priority queue");
                        metrics.async_classification_added_to_queue.incr();
                    },
                    Ok(Err(e)) => {
                        tracing::error!({error=%e},"[pubsub] priority queue send error");
                    },
                    Err(_) => {
                        tracing::error!({message_id=%message_id}, "[pubsub] sending to priority queue timed out");
                    }
                };
                metrics.priority_queue_send_time_async.record(send_start_time.elapsed());
                tracing::debug!({message_id=%message_id, ack_id=ack_id},"[pubsub] waiting on ack or nack");

                let receive_start_time = Instant::now();
                match timeout(max_acking_receiver_wait_time, acking_receiver).await { // 5 Minutes to return
                    Ok(Ok(ack_or_nack)) => match ack_or_nack {
                        AckOrNack::Ack(_optional_execution_result) => {
                            tracing::debug!({message_id=%message_id, ack_id=ack_id},"[pubsub] acking message");
                            metrics.async_classification_result_ack.incr();
                            metrics.receiver_ack_time_async.record(receive_start_time.elapsed());
                            Ok(())
                        },
                        AckOrNack::Nack => {
                            tracing::debug!({message_id=%message_id, ack_id=ack_id},"[pubsub] nacking message");
                            metrics.async_classification_result_nack.incr();
                            metrics.receiver_ack_time_async.record(receive_start_time.elapsed());
                            Err(())
                        },
                    },
                    Ok(Err(recv_error)) => {
                        tracing::error!({message_id=%message_id, recv_error=%recv_error, ack_id=ack_id},"[pubsub] acking sender dropped");
                        metrics.receiver_ack_time_async.record(receive_start_time.elapsed());
                        Err(())
                    },
                    Err(_) => {
                        tracing::error!({message_id=%message_id, ack_id=ack_id}, "[pubsub] waiting for ack/nack timed out");
                        metrics.receiver_ack_time_async.record(receive_start_time.elapsed());
                        Err(())
                    },
                }
            }
        }),
        MetricsClientBuilder::new("osprey_coordinator.pull"),
    )
    .gracefully_stop_on_signal(exit_signal(), Duration::from_secs(30))
    .await;
    Result::Ok(())
}

// TODO: Fix these tests

// #[cfg(test)]
// mod tests {
//     use std::{collections::HashMap, fs::File, io::Read};

//     use msgpack_simple::MsgPack;
//     use prost::Message;
//     use prost_types::Timestamp;
//     // use protobuf_json_mapping;
//     use serde_json::json;

//     // use discord_smite_rpc_actions_proto::SmiteCoordinatorAction as SmiteRpcAction;
//     use crate::proto;
//     use std::io::Cursor;
//     use std::str;

//     use super::create_action_from_pubsub_message;

//     // #[test]
//     // fn test_create_action_from_pubsub_message_1() {
//     //     let action_json = json!({
//     //         "id": "123456789",
//     //         "name": "guild_invite_created",
//     //         "data": {
//     //             "char": "abc",
//     //             "int": 1i64,
//     //             "float2": 1.1_f64
//     //         },

//     //     });
//     //     let encoded = MsgPack::String(action_json.to_string()).encode();

//     //     let prost_action =
//     //         create_action_from_pubsub_message(encoded.as_slice(), 12344242, Timestamp::default());
//     //     println!("{:?}", prost_action);
//     //     assert!(prost_action.is_ok(), "prost action decoding failed");
//     //     let prost_action = prost_action.unwrap();
//     //     let data = MsgPack::parse(&prost_action.action_data).unwrap();
//     //     println!("{:?}", data);
//     //     let mut data: HashMap<String, MsgPack> = data
//     //         .as_map()
//     //         .unwrap()
//     //         .into_iter()
//     //         .map(|v| (v.key.as_string().unwrap(), v.value))
//     //         .collect();

//     //     let x = data.remove("char").unwrap().as_string().unwrap();

//     //     assert_eq!(x, "abc".to_string());
//     // }

//     // #[test]
//     // fn test_create_action_from_pubsub_message_2() {
//     //     let action_json = json!({
//     //         "id": "123456789",
//     //         "name": "guild_invite_created",
//     //         "data": {
//     //             "char": "abc",
//     //             "int": 1i64,
//     //             "float2": 1.1_f64
//     //         },

//     //     });
//     //     // let encoded = MsgPack::String(action_json.to_string()).encode();

//     //     let prost_action = create_action_from_pubsub_message(
//     //         action_json.to_string().bytes().collect(),
//     //         12344242,
//     //         Timestamp::default(),
//     //     );
//     //     println!("{:?}", prost_action);
//     //     assert!(prost_action.is_ok(), "prost action decoding failed");
//     //     let prost_action = prost_action.unwrap();
//     //     let data = MsgPack::parse(&prost_action.action_data).unwrap();
//     //     println!("{:?}", data);
//     //     let mut data: HashMap<String, MsgPack> = data
//     //         .as_map()
//     //         .unwrap()
//     //         .into_iter()
//     //         .map(|v| (v.key.as_string().unwrap(), v.value))
//     //         .collect();

//     //     let x = data.remove("char").unwrap().as_string().unwrap();

//     //     assert_eq!(x, "abc".to_string());
//     // }
//     #[test]
//     fn test_create_action_from_pubsub_proto_action() {
//         let mut file = File::open("pubsub_messages.json").unwrap();
//         let mut data = String::new();
//         file.read_to_string(&mut data).unwrap();
//         let json: serde_json::Value = serde_json::from_str(&data).unwrap();
//         let action_jsons = json.as_array().expect("was not array");
//         let action_json = action_jsons[0].as_object().expect("is not map");
//         println!("{:?}", action_json);
//         let action_data = action_json
//             .get("message")
//             .unwrap()
//             .as_object()
//             .unwrap()
//             .get("data")
//             .unwrap()
//             .as_str()
//             .unwrap();

//         println!("{:?}", action_data);
//         let action_data = str::from_utf8(action_data.as_bytes()).unwrap();
//         println!("{:?}", action_data);
//         // let action_data = base64::decode(action_data).unwrap();

//         // let mut action_prost: SmiteRpcAction =
//         //     SmiteRpcAction::decode(&mut Cursor::new(action_data.to_string())).unwrap();
//         println!("--------");
//         let test_object_data = "CWUQhNrjpmQOGsgBCgsJCwCCONT0dQYQARK4AQogfnvDH9y83BpR5FraKYJSCxTDrQkSb1axRgl9i82pYwgQARoLCI2fiZYGEPeX1CciCwjksNCbBhC8i4FgKgsI5MaGmwYQvIuBYDItCg0KCzk1LjIuMTIuMTY2EgkKB0FuZHJvaWQaEQoPRGlzY29yZCBBbmRyb2lkOi8KDwoNMTc2LjIxOS40Mi4zNBIJCgdBbmRyb2lkGhEKD0Rpc2NvcmQgQW5kcm9pZEILCPqihpsGEOj7p0c=";
//         let decoded_base64 = base64::decode(test_object_data).unwrap();
//         let mut test_action = proto::SmiteCoordinatorAction::default();
//         test_action.id = 20;

//         println!("{:?}", test_action);
//         let x = test_action.encode_to_vec();
//         println!("{:?}", x);
//         println!("--------");
//         println!("{:?}", decoded_base64);
//         let mut action_prost: SmiteRpcAction =
//             SmiteRpcAction::decode(decoded_base64.as_slice()).unwrap();
//         println!("{:?}", action_prost);

//         let output = protobuf_json_mapping::print_to_string(action_prost);
//         println!("{:?}", output);

//         // let prost_action = create_action_from_pubsub_message(
//         //     action_json.to_string().bytes().collect(),
//         //     12344242,
//         //     Timestamp::default(),
//         // );
//         // println!("{:?}", prost_action);
//         // assert!(prost_action.is_ok(), "prost action decoding failed");
//         // let prost_action = prost_action.unwrap();
//         // let data = MsgPack::parse(&prost_action.action_data).unwrap();
//         // println!("{:?}", data);
//         // let mut data: HashMap<String, MsgPack> = data
//         //     .as_map()
//         //     .unwrap()
//         //     .into_iter()
//         //     .map(|v| (v.key.as_string().unwrap(), v.value))
//         //     .collect();

//         // let x = data.remove("char").unwrap().as_string().unwrap();

//         // assert_eq!(x, "abc".to_string());
//     }
// }
