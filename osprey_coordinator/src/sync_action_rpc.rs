use crate::gcloud::gcp_metadata::GCPMetadataClient;
use crate::gcloud::grpc::connection::Connection;
use crate::gcloud::kms::{AesGcmEnvelope, GOOGLE_KMS_DOMAIN};
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
use async_trait::async_trait;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

use osprey_coordinator_sync_action::osprey_coordinator_sync_action_service_server::OspreyCoordinatorSyncActionService;
use osprey_coordinator_sync_action::ProcessActionRequest;
use rand::Rng;

#[async_trait]
trait SecretDataDecryptor: Send + Sync {
    async fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>>;
}

struct KmsSecretDataDecryptor {
    envelope: Mutex<Option<Arc<AesGcmEnvelope>>>,
}

impl KmsSecretDataDecryptor {
    fn new() -> Self {
        Self {
            envelope: Mutex::new(None),
        }
    }

    async fn envelope(&self) -> Result<Arc<AesGcmEnvelope>> {
        let mut envelope = self.envelope.lock().await;
        if let Some(envelope) = envelope.as_ref() {
            return Ok(envelope.clone());
        }

        let kek_uri = env::var("PUBSUB_ENCRYPTION_KEY_URI")
            .context("`PUBSUB_ENCRYPTION_KEY_URI` must be set to decrypt sync action secrets")?;
        let connection = Connection::from_metadata_client(
            GCPMetadataClient::new("default".to_owned())?,
            Duration::from_secs(5),
            Duration::from_secs(24000),
            GOOGLE_KMS_DOMAIN,
        )
        .await?;
        let initialized =
            Arc::new(connection.create_kms_aes_gcm_envelope(kek_uri, Vec::new(), true)?);
        *envelope = Some(initialized.clone());
        Ok(initialized)
    }
}

#[async_trait]
impl SecretDataDecryptor for KmsSecretDataDecryptor {
    async fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        self.envelope().await?.decrypt(ciphertext).await
    }
}

pub(crate) struct SyncActionServer {
    snowflake_client: Arc<SnowflakeClient>,
    priority_queue_sender: PriorityQueueSender,
    metrics: Arc<OspreyCoordinatorMetrics>,
    is_shutting_down: Arc<AtomicBool>,
    secret_data_decryptor: Arc<dyn SecretDataDecryptor>,
}

impl SyncActionServer {
    pub fn new(
        snowflake_client: Arc<SnowflakeClient>,
        priority_queue_sender: PriorityQueueSender,
        metrics: Arc<OspreyCoordinatorMetrics>,
        is_shutting_down: Arc<AtomicBool>,
    ) -> SyncActionServer {
        SyncActionServer {
            snowflake_client,
            priority_queue_sender,
            metrics,
            is_shutting_down,
            secret_data_decryptor: Arc::new(KmsSecretDataDecryptor::new()),
        }
    }

    #[cfg(test)]
    fn new_with_decryptor(
        snowflake_client: Arc<SnowflakeClient>,
        priority_queue_sender: PriorityQueueSender,
        metrics: Arc<OspreyCoordinatorMetrics>,
        is_shutting_down: Arc<AtomicBool>,
        secret_data_decryptor: Arc<dyn SecretDataDecryptor>,
    ) -> SyncActionServer {
        SyncActionServer {
            snowflake_client,
            priority_queue_sender,
            metrics,
            is_shutting_down,
            secret_data_decryptor,
        }
    }
}

async fn create_osprey_coordinator_action(
    ack_id: u64,
    action_request: &osprey_coordinator_sync_action::ProcessActionRequest,
    snowflake_client: &SnowflakeClient,
    secret_data_decryptor: &dyn SecretDataDecryptor,
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
    let secret_data = match action_request.secret_data.as_ref() {
        Some(
            osprey_coordinator_sync_action::process_action_request::SecretData::EncryptedJsonSecretData(
                encrypted_json_secret_data,
            ),
        ) => Some(
            proto::osprey_coordinator_action::SecretData::JsonSecretData(
                secret_data_decryptor
                    .decrypt(encrypted_json_secret_data)
                    .await
                    .context("failed to decrypt `encrypted_json_secret_data`")?,
            ),
        ),
        None => None,
    };
    let osprey_coordinator_action = proto::OspreyCoordinatorAction {
        ack_id,
        action_id,
        action_name: action_request.action_name.clone(),
        action_data: Some(
            proto::osprey_coordinator_action::ActionData::JsonActionData(
                action_request.action_data_json.clone().into(),
            ),
        ),
        secret_data,
        timestamp: Some(
            action_request
                .timestamp
                .as_ref()
                .context("`timestamp` not found")?
                .clone(),
        ),
    };

    Ok(osprey_coordinator_action)
}

impl SyncActionServer {
    async fn try_process_action(
        &self,
        ack_id: u64,
        action_request: &ProcessActionRequest,
    ) -> Result<tonic::Response<osprey_coordinator_sync_action::ProcessActionResponse>, tonic::Status>
    {
        // Fast-reject new RPCs once this pod is draining. Returns `Unavailable`
        // before the request is enqueued or a snowflake is allocated. The
        // gRPC client retries `Unavailable`, routing the retry to a healthier
        // coordinator pod. Without this,
        // requests landing during the shutdown window queue up, await workers
        // whose bidi streams are about to tear down, and end up hitting the
        // client-side deadline as `DEADLINE_EXCEEDED`.
        if self.is_shutting_down.load(Ordering::Acquire) {
            self.metrics
                .sync_classification_failure_shutting_down
                .incr();
            return Err(tonic::Status::unavailable("coordinator draining"));
        }

        let unvalidated_action_id = action_request.action_id;

        let osprey_coordinator_action = match create_osprey_coordinator_action(
            ack_id,
            action_request,
            self.snowflake_client.as_ref(),
            self.secret_data_decryptor.as_ref(),
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

        let action_id = osprey_coordinator_action.action_id;

        let (ackable_action, acking_receiver) = AckableAction::new(osprey_coordinator_action);

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

fn log_action_request(action_request: &ProcessActionRequest) {
    tracing::debug!(
        action_id = ?action_request.action_id,
        action_name = %action_request.action_name,
        has_secret_data = action_request.secret_data.is_some(),
        "[rpc] action request received"
    );
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
        log_action_request(&action_request);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::priority_queue::create_ackable_action_priority_queue;
    use crate::proto::osprey_coordinator_action::{
        ActionData, SecretData as CoordinatorSecretData,
    };
    use crate::proto::osprey_coordinator_sync_action::process_action_request::SecretData as RequestSecretData;
    use prost_types::Timestamp;
    use std::io;
    use std::sync::Mutex;

    struct TestSecretDataDecryptor;

    #[async_trait]
    impl SecretDataDecryptor for TestSecretDataDecryptor {
        async fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
            match ciphertext {
                b"encrypted-secret" => Ok(br#"{"private":"secret"}"#.to_vec()),
                b"encrypted-malformed-json" => Ok(b"not-json".to_vec()),
                _ => Err(anyhow!("test decryption failure")),
            }
        }
    }

    #[derive(Clone)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn request(secret_data: Option<Vec<u8>>) -> ProcessActionRequest {
        ProcessActionRequest {
            action_id: Some(123),
            action_name: "test_action".to_owned(),
            action_data_json: r#"{"public":"value"}"#.to_owned(),
            timestamp: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            secret_data: secret_data.map(RequestSecretData::EncryptedJsonSecretData),
        }
    }

    async fn process_request(secret_data: Option<Vec<u8>>) -> proto::OspreyCoordinatorAction {
        let metrics = OspreyCoordinatorMetrics::new();
        let (priority_queue_sender, priority_queue_receiver) =
            create_ackable_action_priority_queue();
        let server = SyncActionServer::new_with_decryptor(
            Arc::new(SnowflakeClient::new("unused".to_owned())),
            priority_queue_sender,
            metrics.clone(),
            Arc::new(AtomicBool::new(false)),
            Arc::new(TestSecretDataDecryptor),
        );
        let request = request(secret_data);

        let response =
            tokio::spawn(async move { server.process_action(tonic::Request::new(request)).await });
        let ackable_action = priority_queue_receiver.recv(metrics).await.unwrap();
        let (action, acker) = ackable_action.into_action();
        acker.ack_or_nack(AckOrNack::Ack(None));
        response.await.unwrap().unwrap();
        action
    }

    #[test]
    fn action_request_log_redacts_payloads() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer = SharedWriter(output.clone());
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_target(false)
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let secret_sentinel = b"private-secret-sentinel".to_vec();
        let mut action_request = request(Some(secret_sentinel));
        action_request.action_data_json = "public-data-sentinel".to_owned();

        tracing::subscriber::with_default(subscriber, || {
            log_action_request(&action_request);
        });

        let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(output.contains("action_id=Some(123)"));
        assert!(output.contains("action_name=test_action"));
        assert!(output.contains("has_secret_data=true"));
        assert!(!output.contains("public-data-sentinel"));
        assert!(!output.contains("private-secret-sentinel"));
        assert!(!output.contains("ProcessActionRequest"));
        assert!(!output.contains("action_data_json"));
        assert!(!output.contains("encrypted_json_secret_data"));
    }

    #[tokio::test]
    async fn process_action_forwards_absent_secret_data() {
        let action = process_request(None).await;

        assert_eq!(
            action.action_data,
            Some(ActionData::JsonActionData(
                br#"{"public":"value"}"#.to_vec()
            ))
        );
        assert_eq!(action.secret_data, None);
    }

    #[tokio::test]
    async fn process_action_decrypts_and_forwards_secret_data_separately() {
        let action = process_request(Some(b"encrypted-secret".to_vec())).await;

        assert_eq!(
            action.action_data,
            Some(ActionData::JsonActionData(
                br#"{"public":"value"}"#.to_vec()
            ))
        );
        assert_eq!(
            action.secret_data,
            Some(CoordinatorSecretData::JsonSecretData(
                br#"{"private":"secret"}"#.to_vec()
            ))
        );
    }

    #[tokio::test]
    async fn process_action_leaves_decrypted_json_validation_to_the_worker() {
        let action = process_request(Some(b"encrypted-malformed-json".to_vec())).await;

        assert_eq!(
            action.secret_data,
            Some(CoordinatorSecretData::JsonSecretData(b"not-json".to_vec()))
        );
    }

    #[tokio::test]
    async fn process_action_rejects_secret_data_that_cannot_be_decrypted() {
        let action_request = request(Some(b"invalid-ciphertext".to_vec()));
        let snowflake_client = SnowflakeClient::new("unused".to_owned());

        let error = create_osprey_coordinator_action(
            1,
            &action_request,
            &snowflake_client,
            &TestSecretDataDecryptor,
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "failed to decrypt `encrypted_json_secret_data`"
        );
    }
}
