mod client;
mod error;
mod server;
mod watcher;

pub use client::Client;
pub use error::EtcdError;
#[cfg(feature = "test_server")]
pub use server::{Server, ServerBuilder};
pub use watcher::{WatchEvent, Watcher};

pub use etcd::kv::{Action, KeyValueInfo, Node, WatchError};
pub use etcd::Error as EtcdApiError;
