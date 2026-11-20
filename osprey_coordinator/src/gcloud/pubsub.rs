use crate::gcloud::{
    auth::AuthorizationHeaderInterceptor, google::pubsub::v1::subscriber_client,
    grpc::connection::Connection,
};
use std::fmt::Display;
use tonic::codegen::InterceptedService;
pub const GOOGLE_PUBSUB_DOMAIN: &str = "pubsub.googleapis.com";
impl Connection {
    /// Creates a subscriber client, which is used to provide a client to `pub_sub_streaming_pull::StreamingPullManager`
    pub fn create_subscriber_client(
        &self,
    ) -> subscriber_client::SubscriberClient<
        InterceptedService<tonic::transport::Channel, AuthorizationHeaderInterceptor>,
    > {
        subscriber_client::SubscriberClient::with_interceptor(
            self.channel.clone(),
            self.authorization_header_interceptor.clone(),
        )
    }
}
/// Represents a pub-sub subscription
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct PubSubSubscription {
    project: String,
    subscription: String,
}

impl PubSubSubscription {
    pub fn new(project: impl Into<String>, subscription: impl Into<String>) -> Self {
        Self {
            project: project.into(),
            subscription: subscription.into(),
        }
    }

    // Utility API copied whole; only part is used.
    #[allow(dead_code)]
    pub fn project(&self) -> &str {
        &self.project
    }

    // Utility API copied whole; only part is used.
    #[allow(dead_code)]
    pub fn subscription(&self) -> &str {
        &self.subscription
    }
}

impl Display for PubSubSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "projects/{}/subscriptions/{}",
            self.project, self.subscription
        )
    }
}
