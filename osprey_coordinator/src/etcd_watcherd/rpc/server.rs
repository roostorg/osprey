use std::pin::Pin;

use futures::Stream;
use tonic::{Response, Status};

use crate::etcd_watcherd::{
    rpc::proto::v1 as proto, KeyWatchEvents, RecursiveKeyWatchEvents, Watcher,
};
pub use proto::etcd_watcherd_service_server::EtcdWatcherdServiceServer;

pub struct EtcdWatcherdServiceImpl {
    watcher_client: Watcher,
}

impl EtcdWatcherdServiceImpl {
    pub fn new(watcher_client: Watcher) -> Self {
        EtcdWatcherdServiceImpl { watcher_client }
    }
}

type BoxedStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;

#[tonic::async_trait]
impl proto::etcd_watcherd_service_server::EtcdWatcherdService for EtcdWatcherdServiceImpl {
    type WatchKeyStream = BoxedStream<proto::WatchKeyResponse>;

    async fn watch_key(
        &self,
        request: tonic::Request<proto::WatchKeyRequest>,
    ) -> Result<tonic::Response<Self::WatchKeyStream>, tonic::Status> {
        let request = request.into_inner();
        if !request.key.starts_with("/") {
            return Err(tonic::Status::invalid_argument(
                "`key` must start with a '/'",
            ));
        }
        if request.key.ends_with("/") {
            return Err(tonic::Status::invalid_argument(
                "`key` must not end with a '/'",
            ));
        }

        let watch_handle = self
            .watcher_client
            .watch_key(request.key)
            .await
            .map_err(|e| Status::internal(format!("etcd watcher error: {:?}", e)))?;

        let mut events = watch_handle.events();
        let output = async_stream::try_stream! {
            loop {
                let KeyWatchEvents::FullSync { value } = events.next().await;
                let value = value.map(|x| x.to_string());
                let response = proto::WatchKeyResponse { value };
                yield response;
            }
        };
        Ok(Response::new(Box::pin(output) as Self::WatchKeyStream))
    }

    type WatchKeyRecursiveStream = BoxedStream<proto::WatchKeyRecursiveResponse>;

    async fn watch_key_recursive(
        &self,
        request: tonic::Request<proto::WatchKeyRecursiveRequest>,
    ) -> Result<Response<Self::WatchKeyRecursiveStream>, Status> {
        let request = request.into_inner();
        if !request.key.starts_with("/") {
            return Err(tonic::Status::invalid_argument(
                "`key` must start with a '/'",
            ));
        }
        if !request.key.ends_with("/") {
            return Err(tonic::Status::invalid_argument("`key` must end with a '/'"));
        }

        let watch_handle = self
            .watcher_client
            .watch_key_recursive(request.key)
            .await
            .map_err(|e| Status::internal(format!("etcd watcher error: {:?}", e)))?;

        let mut events = watch_handle.events();
        let output = async_stream::try_stream! {
            use proto::watch_key_recursive_response::*;
            loop {
                let event = match events.next().await {
                    RecursiveKeyWatchEvents::FullSync { items } => {
                        let items = items
                            .iter()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect();

                        Event::FullSync(FullSync { items })
                    }
                    RecursiveKeyWatchEvents::SyncOne { key, value } => {
                        let (key, value) = (key.to_string(), value.to_string());

                        Event::SyncOne(SyncOne { key, value })
                    }
                    RecursiveKeyWatchEvents::DeleteOne { key, prev_value } => {
                        let (key, prev_value) = (key.to_string(), prev_value.to_string());

                        Event::DeleteOne(DeleteOne { key, prev_value })
                    }
                };
                let response = proto::WatchKeyRecursiveResponse {
                    event: Some(event)
                };
                yield response;
            }
        };

        Ok(Response::new(
            Box::pin(output) as Self::WatchKeyRecursiveStream
        ))
    }
}
