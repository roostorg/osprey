use thiserror::Error;

#[derive(Error, Debug)]
pub enum DiscoveryError {
    #[error("Failed to resolve hostname to ip. hostname={}", hostname)]
    FailedToResolveHostname { hostname: String },
}
