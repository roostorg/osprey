mod announcer;
#[cfg(not(feature = "integration_test"))]
mod directory;
#[cfg(feature = "integration_test")]
pub mod directory;
mod error;
mod limits;
mod ring;
mod service;
mod types;
mod watcher;

pub use announcer::ServiceAnnouncer;
pub use error::DiscoveryError;
pub use limits::{GlobalLimiter, Member, Normal};
pub use ring::{Config as RingConfig, Ring};
pub use service::ServiceRegistration;
pub use watcher::ServiceWatcher;
