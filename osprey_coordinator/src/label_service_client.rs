use crate::pigeon::grpc_client::GrpcClient;
use crate::pigeon::Connector;

use crate::round_robin;
use anyhow::Result;
use tonic::transport::{Channel, Endpoint, Error as TonicError};

use crate::proto::label_service_client::LabelServiceClient as LabelServiceClientProto;
use crate::proto::{Entity, EntityKey, GetEntityRequest, GetEntityResponse};

use futures::future::try_join_all;
use futures::future::{BoxFuture, FutureExt};

pub struct LabelServiceClient {
    rpc_client: GrpcClient<LabelServiceClientProto<Channel>>,
}

impl LabelServiceClient {
    pub async fn new() -> Result<LabelServiceClient> {
        let rpc_client = GrpcClient::<LabelServiceClientProto<Channel>>::new(
            "label_service_client".into(),
            "discord_smite_labels".into(),
            2,
            Some("smite_coordinator"),
        )
        .await?;
        Ok(LabelServiceClient { rpc_client })
    }

    pub async fn get_entities(&self, entities: Vec<EntityKey>) -> Result<Vec<Entity>> {
        let requests: Vec<_> = entities
            .into_iter()
            .map(|entity_key| GetEntityRequest {
                // the routing key is only important client-side ^^
                routing_key: "meow".to_string(),
                key: Some(entity_key),
            })
            .collect();

        let mut futures: Vec<BoxFuture<Result<GetEntityResponse, tonic::Status>>> =
            Vec::with_capacity(8);

        for request in requests {
            let future: BoxFuture<Result<GetEntityResponse, tonic::Status>> =
                round_robin!(self.rpc_client, get_entity, request).boxed();
            futures.push(future);
        }

        let get_entity_results: Vec<GetEntityResponse> = try_join_all(futures).await?;

        Ok(get_entity_results
            .into_iter()
            .filter_map(|get_entity_response| get_entity_response.entity)
            .collect())
    }
}

#[tonic::async_trait]
impl Connector for LabelServiceClientProto<Channel> {
    async fn connect(dst: Endpoint) -> Result<Self, TonicError> {
        let svc = Self::connect(dst)
            .await?
            .max_decoding_message_size(20 * 1024 * 1024)
            .max_encoding_message_size(20 * 1024 * 1024);

        Ok(svc)
    }
    fn connect_lazy(dst: Endpoint) -> Self {
        let channel = dst.connect_lazy();

        Self::new(channel)
            .max_decoding_message_size(20 * 1024 * 1024)
            .max_encoding_message_size(20 * 1024 * 1024)
    }
}
