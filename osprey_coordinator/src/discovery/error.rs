use thiserror::Error;

// Utility API copied whole; only part is used.
#[allow(dead_code)]
#[derive(Error, Debug)]
pub enum DiscoveryError {
    #[error("Failed to resolve hostname to ip. hostname={}", hostname)]
    FailedToResolveHostname { hostname: String },
}
