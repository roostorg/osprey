use std::future::Future;
use std::io::{Error as IoError, ErrorKind};
use std::time::Instant;

/// Utility function for timing out a future without having to double unwrap the result.
/// It collapses the two results into a single result.
pub async fn timeout_at<F, O, E>(deadline: Instant, future: F) -> F::Output
where
    F: Future<Output = Result<O, E>>,
    E: From<IoError>,
{
    tokio::time::timeout_at(deadline.into(), future)
        .await
        .unwrap_or_else(|_e| Err(IoError::new(ErrorKind::TimedOut, "timeout").into()))
}
