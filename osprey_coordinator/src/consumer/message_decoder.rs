use anyhow::{anyhow, Result};
use convert_case::{Case, Casing};
use prost::Message as ProstMessage;
use prost_types::Timestamp;
use serde::Deserialize;
use serde_json::Value;

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
