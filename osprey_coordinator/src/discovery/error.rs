use thiserror::Error;

#[derive(Error, Debug)]
pub enum DiscoveryError {
    #[error("Response has no index")]
    NoIndex,
    #[error("Node has no value")]
    NoNodeValue,
    #[error("Failed to resolve hostname to ip. hostname={}", hostname)]
    FailedToResolveHostname { hostname: String },
    #[error("No ring configured but select_key called.")]
    NoRingConfigured,
    #[error("No services in discovery.")]
    NoServices,
}
