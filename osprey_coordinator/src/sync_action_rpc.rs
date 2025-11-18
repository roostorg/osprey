use crate::metrics::counters::StaticCounter;
use crate::metrics::histograms::StaticHistogram;
use crate::snowflake_client::SnowflakeClient;
use crate::{
    coordinator_metrics::OspreyCoordinatorMetrics,
    priority_queue::AckableAction,
    priority_queue::{AckOrNack, PriorityQueueSender},
    proto::{self, osprey_coordinator_sync_action},
};
use anyhow::{anyhow, Context, Result};
use std::sync::Arc;
use tokio::time::Instant;

use osprey_coordinator_sync_action::osprey_coordinator_sync_action_service_server::OspreyCoordinatorSyncActionService;
use osprey_coordinator_sync_action::ProcessActionRequest;
use rand::Rng;

pub(crate) struct SyncActionServer {
    snowflake_client: Arc<SnowflakeClient>,
    priority_queue_sender: PriorityQueueSender,
    metrics: Arc<OspreyCoordinatorMetrics>,
}

impl SyncActionServer {
    pub fn new(
        snowflake_client: Arc<SnowflakeClient>,
        priority_queue_sender: PriorityQueueSender,
        metrics: Arc<OspreyCoordinatorMetrics>,
    ) -> SyncActionServer {
        SyncActionServer {
            snowflake_client,
            priority_queue_sender,
            metrics,
        }
    }
}

async fn create_smite_coordinator_action(
    ack_id: u64,
    action_request: &osprey_coordinator_sync_action::ProcessActionRequest,
    snowflake_client: &SnowflakeClient,
) -> Result<proto::OspreyCoordinatorAction> {
    // generate snowflake if one is not provided, to match the behaviour in pubsub.rs
    let action_id = match action_request.action_id {
        Some(id) => match id {
            // handle 0 as none-type, since protos default u64 to 0
            0 => snowflake_client.generate_id().await?,
            _ => id,
        },
        None => snowflake_client.generate_id().await?,
    };
    if action_request.action_name.is_empty() {
        return Err(anyhow!("`action_name` must not be empty"));
    }
    let smite_coordinator_action = proto::OspreyCoordinatorAction {
        ack_id,
        action_id,
        action_name: action_request.action_name.clone(),
        action_data: Some(
            proto::osprey_coordinator_action::ActionData::JsonActionData(
                action_request.action_data_json.clone().into(),
            ),
        ),
        secret_data: None,
        timestamp: Some(
            action_request
                .timestamp
                .as_ref()
                .context("`timestamp` not found")?
                .clone(),
        ),
    };

    Ok(smite_coordinator_action)
}

impl SyncActionServer {
    async fn try_process_action(
        &self,
        ack_id: u64,
        action_request: &ProcessActionRequest,
    ) -> Result<tonic::Response<osprey_coordinator_sync_action::ProcessActionResponse>, tonic::Status>
    {
        let unvalidated_action_id = action_request.action_id;

        let smite_coordinator_action = match create_smite_coordinator_action(
            ack_id,
            action_request,
            self.snowflake_client.as_ref(),
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                tracing::error!({error=%error, ack_id=ack_id, action_id=unvalidated_action_id},"[rpc] deserialization error");
                self.metrics
                    .sync_classification_failure_deserialization
                    .incr();
                return Err(tonic::Status::new(tonic::Code::Aborted, error.to_string()));
            }
        };

        let action_id = smite_coordinator_action.action_id;

        let (ackable_action, acking_receiver) = AckableAction::new(smite_coordinator_action);

        let send_start_time = Instant::now();
        match self.priority_queue_sender.send_sync(ackable_action).await {
            Ok(_) => {
                tracing::debug!({action_id=%action_id, ack_id=ack_id}, "[rpc] sent message to priority queue")
            }
            Err(e) => {
                self.metrics.sync_classification_failure_pq_send.incr();
                tracing::error!({error=%e, action_id=%action_id, ack_id=ack_id},"[rpc] tried to send action to closed channel");
                return Err(tonic::Status::new(tonic::Code::Unavailable, e.to_string()));
            }
        };
        self.metrics
            .priority_queue_send_time_sync
            .record(send_start_time.elapsed());
        tracing::debug!({action_id=%action_id, ack_id=ack_id},"[rpc] waiting on ack or nack");

        let receive_start_time = Instant::now();
        match acking_receiver.await {
            Ok(ack_or_nack) => match ack_or_nack {
                AckOrNack::Ack(verdicts) => {
                    tracing::debug!({action_id=%action_id, ack_id=ack_id},"[rpc] acking message");

                    let response =
                        osprey_coordinator_sync_action::ProcessActionResponse { verdicts };

                    self.metrics.sync_classification_result_ack.incr();
                    self.metrics
                        .receiver_ack_time_sync
                        .record(receive_start_time.elapsed());
                    Ok(tonic::Response::new(response))
                }
                AckOrNack::Nack => {
                    tracing::debug!({action_id=%action_id, ack_id=ack_id},"[rpc] nacking message");
                    self.metrics.sync_classification_result_nack.incr();
                    self.metrics
                        .receiver_ack_time_sync
                        .record(receive_start_time.elapsed());
                    Err(tonic::Status::aborted("action nacked"))
                }
            },
            Err(recv_error) => {
                tracing::error!({action_id=%action_id, recv_error=%recv_error, ack_id=ack_id},"[rpc] acking sender dropped");
                self.metrics
                    .sync_classification_failure_oneshot_dropped
                    .incr();
                self.metrics
                    .receiver_ack_time_sync
                    .record(receive_start_time.elapsed());
                Err(tonic::Status::internal("acking onshot dropped"))
            }
        }
    }
}

#[tonic::async_trait]
impl OspreyCoordinatorSyncActionService for SyncActionServer {
    async fn process_action(
        &self,
        request: tonic::Request<osprey_coordinator_sync_action::ProcessActionRequest>,
    ) -> Result<tonic::Response<osprey_coordinator_sync_action::ProcessActionResponse>, tonic::Status>
    {
        self.metrics.sync_classification_action_received.incr();
        let action_request = request.into_inner();
        tracing::debug!({action_request=?action_request}, "[rpc] action request received");

        let ack_id: u64 = {
            let mut rng = rand::thread_rng();
            rng.gen()
        };

        match self.try_process_action(ack_id, &action_request).await {
            response @ Ok(_) => response,
            Err(e) => {
                tracing::error!("initial process_action attempt failed, retrying: {}", e);
                self.try_process_action(ack_id, &action_request).await
            }
        }
    }
}
