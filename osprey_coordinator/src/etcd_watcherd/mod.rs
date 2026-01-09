mod api;
mod metrics;
mod watcher;
mod watcher_impl;

#[cfg(feature = "rpc")]
pub mod rpc;
mod stats;

pub use self::api::{WatchHandle, Watcher};
pub use self::watcher_impl::{
    key_watcher::KeyWatchEvents, recursive_key_watcher::RecursiveKeyWatchEvents,
};

pub(crate) use self::stats::WatcherStats;
pub(crate) use self::watcher::{
    WatcherInner, WatcherLoop, WatcherSubscription, WatcherSubscriptionId,
};
pub(crate) use self::watcher_impl::{WatchEvents, WatchResult, WatchState, WatcherImpl};
