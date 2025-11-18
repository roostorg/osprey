mod backoff_utils;
mod cached_futures;
mod coordinator_metrics;
mod discovery;
mod etcd;
mod etcd_config;
mod etcd_watcherd;
mod future_utils;
mod gcloud;
mod hashring;
mod metrics;
mod osprey_bidirectional_stream;
mod pigeon;
mod priority_queue;
mod proto;
mod pub_sub_streaming_pull;
mod pubsub;
mod shutdown_handler;
mod signals;
mod snowflake_client;
mod sync_action_rpc;
mod tokio_utils;
#[cfg(test)]
mod tonic_mock;
use anyhow::Result;
use clap::Parser;
use proto::osprey_coordinator_sync_action::osprey_coordinator_sync_action_service_server::OspreyCoordinatorSyncActionServiceServer;
use std::sync::Arc;
use std::time::Duration;

use crate::coordinator_metrics::OspreyCoordinatorMetrics;
use crate::snowflake_client::SnowflakeClient;

use crate::metrics::emit_worker::SpawnEmitWorker;
use crate::metrics::new_client;

use priority_queue::{create_ackable_action_priority_queue, spawn_priority_queue_metrics_worker};
use pubsub::start_pubsub_subscriber;
use tokio::join;

use crate::osprey_bidirectional_stream::OspreyCoordinatorServer;
use crate::proto::osprey_coordinator_service_server::OspreyCoordinatorServiceServer;

#[derive(Debug, Parser)]
struct CliOptions {
    #[arg(
        short,
        long,
        default_value = "19950",
        env = "OSPREY_COORDINATOR_BIDI_STREAM_PORT"
    )]
    bidi_stream_port: u16,
    #[arg(
        long,
        default_value = "19951",
        env = "OSPREY_COORDINATOR_SYNC_ACTION_PORT"
    )]
    sync_action_port: u16,
    #[arg(
        long,
        default_value = "http://localhost:19952",
        env = "SNOWFLAKE_API_ENDPOINT"
    )]
    snowflake_api_endpoint: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let opts = CliOptions::parse();

    tracing::info!("starting Osprey Coordinator");

    tracing::info!("creating osprey-snowflake client");
    let snowflake_client = Arc::new(SnowflakeClient::new(opts.snowflake_api_endpoint));

    let (priority_queue_sender, priority_queue_receiver) = create_ackable_action_priority_queue();
    let metrics = OspreyCoordinatorMetrics::new();
    tracing::info!("starting grpc metrics worker");
    let _worker_guard = metrics
        .clone()
        .spawn_emit_worker(new_client("osprey_coordinator").unwrap());

    let osprey_coordinator_grpc_bidi_stream_service =
        OspreyCoordinatorServiceServer::new(OspreyCoordinatorServer::new(
            priority_queue_sender.clone(),
            priority_queue_receiver.clone(),
            metrics.clone(),
        ));

    let osprey_coordinator_sync_action_service =
        OspreyCoordinatorSyncActionServiceServer::new(sync_action_rpc::SyncActionServer::new(
            snowflake_client.clone(),
            priority_queue_sender.clone(),
            metrics.clone(),
        ));

    let pubsub_fut = start_pubsub_subscriber(
        snowflake_client,
        priority_queue_sender.clone(),
        metrics.clone(),
    );
    let grpc_bidi_stream_service_fut = pigeon::serve(
        osprey_coordinator_grpc_bidi_stream_service,
        "osprey_coordinator",
        opts.bidi_stream_port,
        Duration::from_secs(30),
    );
    let sync_action_service_fut = pigeon::serve(
        osprey_coordinator_sync_action_service,
        "osprey_coordinator_sync_action",
        opts.sync_action_port,
        Duration::from_secs(60),
    );

    tracing::info!("starting priority queue metrics worker");
    let _drop_guard =
        spawn_priority_queue_metrics_worker(priority_queue_sender.clone(), metrics.clone());

    shutdown_handler::spawn_shutdown_handler(
        priority_queue_sender.clone(),
        priority_queue_receiver.clone(),
    );

    tracing::info!("starting pubsub listener/bidi stream/sync classification rpc");
    let (pubsub_result, grpc_bidi_stream_service_result, sync_action_service_result) = join!(
        pubsub_fut,
        grpc_bidi_stream_service_fut,
        sync_action_service_fut
    );
    tracing::info!({
        pubsub_result=?pubsub_result,
        bidi_stream_result=?grpc_bidi_stream_service_result,
        sync_action_result=?sync_action_service_result},
        "osprey coordinator terminated");

    Ok(())
}
