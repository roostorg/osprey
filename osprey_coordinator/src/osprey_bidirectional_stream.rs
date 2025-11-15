use crate::coordinator_metrics::OspreyCoordinatorMetrics;
use crate::priority_queue::ActionAcker;
use crate::priority_queue::{PriorityQueueReceiver, PriorityQueueSender};
use crate::proto;
use anyhow::{anyhow, Context, Result};
use proto::action_request::ActionRequest;
use std::sync::Arc;
use std::{error::Error, io::ErrorKind};
use tokio::sync::mpsc::{self, Sender};
use tokio::time::{timeout, Duration, Instant};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

use crate::metrics::counters::StaticCounter;
use crate::metrics::histograms::StaticHistogram;

fn match_for_io_error(err_status: &tonic::Status) -> Option<&std::io::Error> {
    let mut err: &(dyn Error + 'static) = err_status;

    loop {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return Some(io_err);
        }

        // h2::Error do not expose std::io::Error with `source()`
        // https://github.com/hyperium/h2/pull/462
        if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
            if let Some(io_err) = h2_err.get_io() {
                return Some(io_err);
            }
        }

        err = match err.source() {
            Some(err) => err,
            None => return None,
        };
    }
}

#[derive(Debug)]
struct OutstandingActionState {
    action_acker: ActionAcker,
    send_time: Instant,
    client_details: proto::ClientDetails,
}

#[derive(Debug)]
enum ClientState {
    NoOutstandingAction,
    OutstandingAction(OutstandingActionState),
}

pub struct OspreyCoordinatorServer {
    priority_queue_receiver: PriorityQueueReceiver,
    #[allow(unused)]
    priority_queue_sender: PriorityQueueSender, // TODO: use this for retrying sync actions
    metrics: Arc<OspreyCoordinatorMetrics>,
}

impl OspreyCoordinatorServer {
    pub fn new(
        priority_queue_sender: PriorityQueueSender,
        priority_queue_receiver: PriorityQueueReceiver,
        metrics: Arc<OspreyCoordinatorMetrics>,
    ) -> OspreyCoordinatorServer {
        OspreyCoordinatorServer {
            priority_queue_sender,
            priority_queue_receiver,
            metrics,
        }
    }
}

fn handle_action_request(
    action_request: ActionRequest,
    current_client_state: ClientState,
    metrics: Arc<OspreyCoordinatorMetrics>,
) -> Result<proto::ClientDetails> {
    match (action_request, current_client_state) {
        (ActionRequest::Initial(client_details), ClientState::NoOutstandingAction) => {
            Ok(client_details)
        }
        (ActionRequest::Initial(_), ClientState::OutstandingAction(_)) => Err(anyhow!(
            "got an initial action request while there was an outstanding action"
        )),
        (ActionRequest::AckOrNack(ack_or_nack), ClientState::NoOutstandingAction) => Err(anyhow!(
            "got an {:?} with no outstanding action",
            ack_or_nack
        )),
        (ActionRequest::AckOrNack(ack_or_nack), ClientState::OutstandingAction(state)) => {
            let duration = Instant::now().duration_since(state.send_time);
            metrics.action_outstanding_duration.record(duration);
            state.action_acker.ack_or_nack(
                ack_or_nack
                    .ack_or_nack
                    .context("no `ack_or_nack` in proto")?,
            );
            Ok(state.client_details)
        }
    }
}

enum UpdateClientStateOrDisconnect {
    UpdateClientState(ClientState),
    ClientRequestedDisconnect,
    #[allow(dead_code)]
    ActionReceiverClosedDisconnect,
    ActionReceiverTimedOut,
}

async fn handle_request(
    client_state: ClientState,
    sender: &Sender<Result<proto::OspreyCoordinatorAction, tonic::Status>>,
    request: proto::Request,
    action_receiver: &PriorityQueueReceiver,
    metrics: Arc<OspreyCoordinatorMetrics>,
    receive_timeout: Duration,
) -> Result<UpdateClientStateOrDisconnect> {
    match request
        .request
        .context("request object missing from proto")?
    {
        proto::request::Request::ActionRequest(action_request) => {
            let action_request = action_request
                .action_request
                .context("no `action_request.action_request` in `ActionRequest` proto")?;
            let client_details =
                handle_action_request(action_request, client_state, metrics.clone())?;
            tracing::debug!("awaiting action from priority queue");
            let priority_queue_receive_start_time = Instant::now();
            let result = timeout(receive_timeout, action_receiver.recv(metrics.clone())).await;
            let ackable_action = match result {
                Ok(Ok(ackable_action)) => ackable_action,
                Ok(Err(_)) | Err(_) => {
                    tracing::error!(
                        "Took too long to get action from priority queue, disconnecting"
                    );
                    metrics
                        .priority_queue_receive_time
                        .record(Instant::now().duration_since(priority_queue_receive_start_time));
                    return Ok(UpdateClientStateOrDisconnect::ActionReceiverTimedOut);
                }
            };
            metrics
                .priority_queue_receive_time
                .record(Instant::now().duration_since(priority_queue_receive_start_time));
            let (action, action_acker) = ackable_action.into_action();
            sender.send(Ok(action)).await?;
            Ok(UpdateClientStateOrDisconnect::UpdateClientState(
                ClientState::OutstandingAction(OutstandingActionState {
                    action_acker,
                    send_time: Instant::now(),
                    client_details,
                }),
            ))
        }
        proto::request::Request::Disconnect(disconnect) => {
            let ack_or_nack = disconnect
                .ack_or_nack
                .context("no `ack_or_nack` in `disconnect` proto")?
                .ack_or_nack
                .context("no `ack_or_nack.ack_or_nack` in `disconnect` proto")?;
            if let ClientState::OutstandingAction(state) = client_state {
                state.action_acker.ack_or_nack(ack_or_nack);
            }
            Ok(UpdateClientStateOrDisconnect::ClientRequestedDisconnect)
        }
    }
}

#[tonic::async_trait]
impl proto::osprey_coordinator_service_server::OspreyCoordinatorService
    for OspreyCoordinatorServer
{
    type OspreyBidirectionalStreamStream =
        ReceiverStream<Result<proto::OspreyCoordinatorAction, tonic::Status>>;

    async fn osprey_bidirectional_stream(
        &self,
        request: tonic::Request<tonic::Streaming<proto::Request>>,
    ) -> Result<tonic::Response<Self::OspreyBidirectionalStreamStream>, tonic::Status> {
        tracing::debug!(
            { connection =? request.metadata() },
            "New Connection Received"
        );
        let mut in_stream = request.into_inner();
        self.metrics.new_connection_established.incr();
        let (tx, rx) = mpsc::channel(128);
        let action_receiver = self.priority_queue_receiver.clone();
        let metrics = self.metrics.clone();
        let max_pq_receive_await_time_ms = Duration::from_millis(
            std::env::var("MAX_PQ_RECEIVE_AWAIT_TIME_MS")
                .unwrap_or("5000".to_string())
                .parse::<u64>()
                .unwrap(),
        );
        tokio::spawn(async move {
            let mut client_state = ClientState::NoOutstandingAction {};

            // TODO: refactor the code to honor the invariants of: the first request should always be an
            // InitialActionRequest and every request after that will either be an AckingRequest or AckingDisconnect
            // let initial_request = in_stream.next().await.unwrap().unwrap();
            // let client_details = match initial_request.request.expect("request must exist") {
            //     proto::request::Request::ActionRequest(action_request) => {
            //         match action_request.action_request.expect("must exist") {
            //             ActionRequest::Initial(client_details) => client_details,
            //             ActionRequest::AckOrNack(_) => unreachable!(),
            //         }
            //     }
            //     proto::request::Request::Disconnect(_) => unreachable!(),
            // };

            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(request) => {
                        tracing::debug!({request=?request},"got request");
                        client_state = match handle_request(
                            client_state,
                            &tx,
                            request,
                            &action_receiver,
                            metrics.clone(),
                            max_pq_receive_await_time_ms,
                        )
                        .await
                        {
                            Ok(directive) => match directive {
                                UpdateClientStateOrDisconnect::UpdateClientState(
                                    new_client_state,
                                ) => new_client_state,
                                UpdateClientStateOrDisconnect::ClientRequestedDisconnect => {
                                    tracing::debug!("client requested a disconnect");
                                    metrics.client_disconnected_gracefully.incr();
                                    break;
                                }
                                UpdateClientStateOrDisconnect::ActionReceiverClosedDisconnect => {
                                    tracing::debug!("disconnecting client because receiver closed");
                                    metrics.client_disconnected_receiver_closed.incr();
                                    break;
                                }
                                UpdateClientStateOrDisconnect::ActionReceiverTimedOut => {
                                    tracing::debug!(
                                        "disconnecting client because receiver timed out"
                                    );
                                    metrics.client_disconnected_receiver_timeout.incr();
                                    break;
                                }
                            },
                            Err(error) => {
                                tracing::error!({error=%error},"error in stream");
                                metrics.client_disconnected_stream_error.incr();
                                // commenting this out for now
                                // we might not have to send an aborted when we get an error
                                // tx.send(Err(tonic::Status::new(
                                //     tonic::Code::Aborted,
                                //     error.to_string(),
                                // )))
                                // .await
                                // .expect("output stream must be open");
                                break;
                            }
                        }
                    }
                    Err(err) => {
                        if let Some(io_err) = match_for_io_error(&err) {
                            if io_err.kind() == ErrorKind::BrokenPipe {
                                tracing::error!("client disconnected: broken pipe");
                                metrics.client_disconnected_broken_pipe.incr();
                                break;
                            }
                        }

                        match tx.send(Err(err)).await {
                            Ok(_) => (),
                            Err(_err) => break, // response was dropped
                        }
                    }
                }
            }

            tracing::debug!("stream ended");
        });

        let out_stream = ReceiverStream::new(rx);
        Ok(tonic::Response::new(out_stream))
    }
}

#[cfg(test)]
mod tests {

    use crate::coordinator_metrics::OspreyCoordinatorMetrics;
    use crate::metrics::emit_worker::SpawnEmitWorker;
    use crate::metrics::new_client;
    use crate::proto::osprey_coordinator_action::ActionData;
    use crate::proto::osprey_coordinator_action::SecretData;
    use proto::osprey_coordinator_service_server::OspreyCoordinatorService;

    use crate::priority_queue::create_ackable_action_priority_queue;
    use crate::priority_queue::AckableAction;

    use super::*;

    #[tokio::test]
    async fn golden_path_bidirection_streaming_test() -> Result<()> {
        // Simple golden path test that adds two actions to the queue and asserts that a properly
        // formed bidirectional streaming request is returned the actions in that order

        tracing_subscriber::fmt::init();
        let (priority_queue_sender, priority_queue_receiver) =
            create_ackable_action_priority_queue();
        let metrics = OspreyCoordinatorMetrics::new();
        let _worker_guard = metrics
            .clone()
            .spawn_emit_worker(new_client("smite_coordinator").unwrap());

        let ackable_action = proto::OspreyCoordinatorAction {
            ack_id: 1,
            action_id: 1,
            action_name: "test_action".into(),
            timestamp: None,
            action_data: Some(ActionData::JsonActionData(
                "{\"action\": \"test action data 1\"}".into(),
            )),
            secret_data: Some(SecretData::JsonSecretData(
                "{\"secret\": \"test secret data 1\"}".into(),
            )),
        };
        let (ackable_action, _receiver_drop_guard_1) = AckableAction::new(ackable_action);
        priority_queue_sender
            .send_sync(ackable_action)
            .await
            .unwrap();

        let ackable_action_2 = proto::OspreyCoordinatorAction {
            ack_id: 2,
            action_id: 2,
            action_name: "test_action".into(),
            timestamp: None,
            action_data: Some(ActionData::JsonActionData(
                "{\"action\": \"test action data 2\"}".into(),
            )),
            secret_data: Some(SecretData::JsonSecretData(
                "{\"secret\": \"test secret data 2\"}".into(),
            )),
        };
        let (ackable_action, _receiver_drop_guard_2) = AckableAction::new(ackable_action_2);
        priority_queue_sender
            .send_sync(ackable_action)
            .await
            .unwrap();

        let server = OspreyCoordinatorServer::new(
            priority_queue_sender.clone(),
            priority_queue_receiver,
            metrics.clone(),
        );

        let initial_action_request = proto::Request {
            request: Some(proto::request::Request::ActionRequest(
                proto::ActionRequest {
                    action_request: Some(proto::action_request::ActionRequest::Initial(
                        proto::ClientDetails::default(),
                    )),
                },
            )),
        };

        let acking_action_request = proto::Request {
            request: Some(proto::request::Request::ActionRequest(
                proto::ActionRequest {
                    action_request: Some(proto::action_request::ActionRequest::AckOrNack(
                        proto::AckOrNack {
                            ack_id: 0,
                            ack_or_nack: Some(proto::ack_or_nack::AckOrNack::Ack(proto::Ack {
                                execution_result: None,
                                verdicts: None,
                            })),
                        },
                    )),
                },
            )),
        };

        let req = crate::tonic_mock::streaming_request(vec![
            initial_action_request.clone(),
            acking_action_request.clone(),
        ]);

        let res = server
            .osprey_bidirectional_stream(req)
            .await
            .expect("error in stream");

        println!("finish connection");

        let mut result = Vec::new();
        let mut messages = res.into_inner();
        while let Some(v) = messages.next().await {
            println!("got message: {:?}", &v);
            result.push(v.expect("error from stream"))
        }

        print!("{:?}", result);

        assert_eq!(result[0].action_id, 1);
        assert_eq!(result[1].action_id, 2);
        assert_eq!(
            result[0].action_data,
            Some(ActionData::JsonActionData(
                "{\"action\": \"test action data 1\"}".into()
            ))
        );
        assert_eq!(
            result[1].action_data,
            Some(ActionData::JsonActionData(
                "{\"action\": \"test action data 2\"}".into()
            ))
        );
        assert_eq!(
            result[0].secret_data,
            Some(SecretData::JsonSecretData(
                "{\"secret\": \"test secret data 1\"}".into()
            ))
        );
        assert_eq!(
            result[1].secret_data,
            Some(SecretData::JsonSecretData(
                "{\"secret\": \"test secret data 2\"}".into()
            ))
        );

        Ok(())
    }
}
