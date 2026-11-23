mod client;
mod error;
mod server;
mod watcher;

pub use client::Client;
pub use error::EtcdError;
#[cfg(feature = "test_server")]
pub use server::{Server, ServerBuilder};

pub use etcd::kv::{Action, Node, WatchError};
pub use etcd::Error as EtcdApiError;
