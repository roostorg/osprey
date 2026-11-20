use anyhow::{anyhow, Result};
use convert_case::{Case, Casing};
use prost::Message as ProstMessage;
use prost_types::Timestamp;
use serde::Deserialize;
use serde_json::Value;
use std::str::FromStr;

use crate::{
    coordinator_metrics::OspreyCoordinatorMetrics,
    metrics::counters::StaticCounter,
    proto::{
        self, osprey_coordinator_action::ActionData, osprey_coordinator_action::SecretData,
        Action as OspreyProtoAction,
    },
    snowflake_client::SnowflakeClient,
};

pub async fn decode_proto_message(
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

/// Decodes a JSON action that isn't protobuf-encoded.
///
/// Two wire formats are accepted, told apart by the first non-whitespace byte:
///
/// - The standard Osprey JSON envelope, the same one the Kafka worker consumes and the
///   bundled test-data producer emits (starts with `{`):
///   `{"send_time": "<RFC 3339>", "data": {"action_id": .., "action_name": .., "data": {..}}}`
/// - A msgpack string holding `{"id", "name", "data", "secret_data"}` JSON (msgpack strings
///   never start with `{`).
pub async fn decode_json_message(
    message_data: &[u8],
    ack_id: u64,
    message_timestamp: Timestamp,
    snowflake_client: &SnowflakeClient,
    metrics: &OspreyCoordinatorMetrics,
) -> Result<proto::OspreyCoordinatorAction> {
    let first_byte = message_data
        .iter()
        .find(|byte| !byte.is_ascii_whitespace())
        .copied();
    if first_byte == Some(b'{') {
        decode_json_envelope_message(
            message_data,
            ack_id,
            message_timestamp,
            snowflake_client,
            metrics,
        )
        .await
    } else {
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

/// An action ID given either as a JSON number or as a string holding one.
#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum JsonActionId {
    Number(u64),
    String(String),
}

impl JsonActionId {
    fn parse(&self) -> Result<u64> {
        match self {
            JsonActionId::Number(id) => Ok(*id),
            JsonActionId::String(id) => id
                .trim()
                .parse::<u64>()
                .map_err(|err| anyhow!("invalid action_id {id:?}: {err}")),
        }
    }
}

/// Decodes the standard Osprey JSON envelope. `send_time` (RFC 3339; a timestamp without
/// an offset is read as UTC) becomes the action's timestamp when present; otherwise the
/// message's own timestamp is used. A missing or zero `action_id` gets a snowflake ID, as
/// on the proto path.
pub async fn decode_json_envelope_message(
    message_data: &[u8],
    ack_id: u64,
    message_timestamp: Timestamp,
    snowflake_client: &SnowflakeClient,
    metrics: &OspreyCoordinatorMetrics,
) -> Result<proto::OspreyCoordinatorAction> {
    #[derive(Deserialize, Debug)]
    struct EnvelopeAction {
        action_id: Option<JsonActionId>,
        action_name: String,
        data: Value,
        secret_data: Option<Value>,
    }

    #[derive(Deserialize, Debug)]
    struct Envelope {
        send_time: Option<String>,
        data: EnvelopeAction,
    }

    let envelope: Envelope = serde_json::from_slice(message_data)?;
    let action = envelope.data;

    let timestamp = match &envelope.send_time {
        Some(send_time) => Timestamp::from_str(send_time.trim())
            .map_err(|err| anyhow!("invalid send_time {send_time:?}: {err}"))?,
        None => message_timestamp,
    };

    let action_id = action
        .action_id
        .as_ref()
        .map(JsonActionId::parse)
        .transpose()?;
    let action_id = match action_id {
        Some(id) if id != 0 => id,
        _ => {
            metrics.action_id_snowflake_generation_json.incr();
            snowflake_client.generate_id().await?
        }
    };

    let secret_data = match &action.secret_data {
        Some(secret_data) => Some(SecretData::JsonSecretData(serde_json::to_vec(secret_data)?)),
        None => None,
    };
    let action_data = serde_json::to_vec(&action.data)?;

    Ok(proto::OspreyCoordinatorAction {
        ack_id,
        action_id,
        action_name: action.action_name,
        action_data: Some(ActionData::JsonActionData(action_data)),
        secret_data,
        timestamp: Some(timestamp),
    })
}

pub async fn decode_msgpack_json_message(
    message_data: &[u8],
    ack_id: u64,
    message_timestamp: Timestamp,
    snowflake_client: &SnowflakeClient,
    metrics: &OspreyCoordinatorMetrics,
) -> Result<proto::OspreyCoordinatorAction> {
    use msgpack_simple::MsgPack;

    #[derive(Deserialize, Debug)]
    struct MsgpackAction {
        id: Option<String>,
        name: String,
        data: Value,
        secret_data: Option<Value>,
    }

    let decoded = MsgPack::parse(message_data)?;
    let decoded = decoded.as_string()?;
    let action: MsgpackAction = serde_json::from_str(decoded.as_str())?;

    let serde_json_vec = serde_json::to_vec(&action.data)?;
    let optional_secret_data = match &action.secret_data {
        Some(secret_data) => Some(SecretData::JsonSecretData(serde_json::to_vec(secret_data)?)),
        _ => None,
    };

    let action_id = match action.id {
        Some(id) => id.parse::<u64>()?,
        None => {
            metrics.action_id_snowflake_generation_json.incr();
            snowflake_client.generate_id().await?
        }
    };

    Ok(proto::OspreyCoordinatorAction {
        ack_id,
        action_id,
        action_name: action.name,
        action_data: Some(ActionData::JsonActionData(serde_json_vec)),
        secret_data: optional_secret_data,
        timestamp: Some(message_timestamp),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use msgpack_simple::MsgPack;
    use serde_json::json;

    // Nothing listens here: tests that don't expect a snowflake ID to be generated use it.
    fn snowflake() -> SnowflakeClient {
        SnowflakeClient::new("http://127.0.0.1:1".to_string())
    }

    /// A stub snowflake API that answers every request with ID 555.
    async fn snowflake_returning_555() -> (SnowflakeClient, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0; 1024];
                let _ = stream.read(&mut request).await;
                let body = "[555]";
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (SnowflakeClient::new(format!("http://{address}")), handle)
    }

    fn kafka_timestamp() -> Timestamp {
        Timestamp {
            seconds: 1_600_000_000,
            nanos: 0,
        }
    }

    async fn decode(message: &[u8]) -> Result<proto::OspreyCoordinatorAction> {
        decode_json_message(
            message,
            7,
            kafka_timestamp(),
            &snowflake(),
            &OspreyCoordinatorMetrics::new(),
        )
        .await
    }

    fn json_data(action: &proto::OspreyCoordinatorAction) -> Value {
        match action.action_data.as_ref().expect("action_data") {
            ActionData::JsonActionData(bytes) => serde_json::from_slice(bytes).unwrap(),
            other => panic!("expected JSON action data, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn decodes_test_data_producer_envelope() {
        // Same shape as example_data/template.json: string action_id, nanosecond send_time.
        let message = json!({
            "send_time": "2024-01-01T12:00:00.123456789Z",
            "data": {
                "action_id": "42",
                "action_name": "create_post",
                "data": {
                    "user_id": "user_123",
                    "event_type": "create_post",
                    "post": {"text": "hello the quick brown fox."}
                }
            }
        })
        .to_string();

        let action = decode(message.as_bytes()).await.unwrap();

        assert_eq!(action.ack_id, 7);
        assert_eq!(action.action_id, 42);
        assert_eq!(action.action_name, "create_post");
        assert_eq!(
            action.timestamp,
            Some(Timestamp {
                seconds: 1_704_110_400,
                nanos: 123_456_789
            })
        );
        assert_eq!(
            json_data(&action),
            json!({
                "user_id": "user_123",
                "event_type": "create_post",
                "post": {"text": "hello the quick brown fox."}
            })
        );
        assert!(action.secret_data.is_none());
    }

    #[tokio::test]
    async fn decodes_envelope_with_numeric_id_offset_and_secret_data() {
        let message = format!(
            "\n  {}",
            json!({
                "send_time": "2024-01-01T13:00:00+01:00",
                "data": {
                    "action_id": 9_007_199_254_740_993u64,
                    "action_name": "user_login",
                    "data": {"ip": "192.168.1.1"},
                    "secret_data": {"email": "a@example.com"}
                }
            })
        );

        let action = decode(message.as_bytes()).await.unwrap();

        assert_eq!(action.action_id, 9_007_199_254_740_993);
        assert_eq!(action.action_name, "user_login");
        assert_eq!(
            action.timestamp,
            Some(Timestamp {
                seconds: 1_704_110_400,
                nanos: 0
            })
        );
        match action.secret_data {
            Some(SecretData::JsonSecretData(bytes)) => assert_eq!(
                serde_json::from_slice::<Value>(&bytes).unwrap(),
                json!({"email": "a@example.com"})
            ),
            // Don't print the value: it is secret data.
            _ => panic!("expected JSON secret data"),
        }
    }

    #[tokio::test]
    async fn envelope_without_send_time_uses_message_timestamp() {
        let message = json!({
            "data": {"action_id": 1, "action_name": "create_post", "data": {}}
        })
        .to_string();

        let action = decode(message.as_bytes()).await.unwrap();

        assert_eq!(action.timestamp, Some(kafka_timestamp()));
    }

    #[tokio::test]
    async fn send_time_without_offset_is_utc() {
        for send_time in ["2024-01-01T12:00:00", "2024-01-01 12:00:00"] {
            let message = json!({
                "send_time": send_time,
                "data": {"action_id": 1, "action_name": "a", "data": {}}
            })
            .to_string();

            let action = decode(message.as_bytes()).await.unwrap();

            assert_eq!(
                action.timestamp,
                Some(Timestamp {
                    seconds: 1_704_110_400,
                    nanos: 0
                }),
                "for {send_time}"
            );
        }
    }

    #[tokio::test]
    async fn envelope_without_action_id_gets_snowflake_id() {
        let (snowflake, server) = snowflake_returning_555().await;
        let messages = [
            json!({"send_time": "2024-01-01T12:00:00Z", "data": {"action_name": "a", "data": {}}}),
            json!({"send_time": "2024-01-01T12:00:00Z", "data": {"action_id": 0, "action_name": "a", "data": {}}}),
            json!({"send_time": "2024-01-01T12:00:00Z", "data": {"action_id": "0", "action_name": "a", "data": {}}}),
        ];
        for message in messages {
            let action = decode_json_message(
                message.to_string().as_bytes(),
                7,
                kafka_timestamp(),
                &snowflake,
                &OspreyCoordinatorMetrics::new(),
            )
            .await
            .unwrap();
            assert_eq!(action.action_id, 555, "for {message}");
        }
        server.abort();
    }

    #[tokio::test]
    async fn rejects_malformed_envelopes() {
        let cases = [
            json!({"send_time": "yesterday", "data": {"action_id": 1, "action_name": "a", "data": {}}}),
            json!({"send_time": 1704110400, "data": {"action_id": 1, "action_name": "a", "data": {}}}),
            json!({"send_time": "2024-01-01T12:00:00Z", "data": {"action_id": 1.5, "action_name": "a", "data": {}}}),
            json!({"send_time": "2024-01-01T12:00:00Z", "data": {"action_id": "abc", "action_name": "a", "data": {}}}),
            json!({"send_time": "2024-01-01T12:00:00Z", "data": {"action_id": -1, "action_name": "a", "data": {}}}),
            json!({"send_time": "2024-01-01T12:00:00Z", "data": {"action_id": 1, "data": {}}}),
            json!({"send_time": "2024-01-01T12:00:00Z", "data": {"action_id": 1, "action_name": "a"}}),
            json!({"send_time": "2024-01-01T12:00:00Z"}),
        ];
        for case in cases {
            assert!(
                decode(case.to_string().as_bytes()).await.is_err(),
                "expected {case} to be rejected"
            );
        }
        assert!(decode(b"{not json").await.is_err());
    }

    #[tokio::test]
    async fn still_decodes_msgpack_actions() {
        let action_json = json!({
            "id": "123456789",
            "name": "guild_invite_created",
            "data": {"char": "abc", "int": 1},
        });
        let encoded = MsgPack::String(action_json.to_string()).encode();

        let action = decode(&encoded).await.unwrap();

        assert_eq!(action.action_id, 123_456_789);
        assert_eq!(action.action_name, "guild_invite_created");
        assert_eq!(action.timestamp, Some(kafka_timestamp()));
        assert_eq!(json_data(&action), json!({"char": "abc", "int": 1}));
    }
}
