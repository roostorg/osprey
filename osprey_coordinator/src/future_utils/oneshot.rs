use crate::future_utils::time::timeout_at;
use anyhow::{Error, Result};
use futures::TryFutureExt;
use std::future::Future;
use std::marker::Unpin;
use std::time::Instant;
use tokio::sync::oneshot;

/// Creates a oneshot that will time out at a deadline.
///
/// # Arguments
///
/// * deadline - the time to timeout at
pub fn channel_with_deadline<T: Unpin>(
    deadline: Instant,
) -> (oneshot::Sender<Result<T>>, impl Future<Output = Result<T>>) {
    let (tx, rx) = oneshot::channel();
    let awaitable = async move {
        let rx = rx.map_err(Error::from);
        timeout_at(deadline, rx).await.unwrap_or_else(Err)
    };
    (tx, awaitable)
}
