use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::task::{JoinError, JoinHandle};

mod unbounded_receiver_chunker;

pub use unbounded_receiver_chunker::{
    ChunkSizeConfig as UnboundedReceiverChunkSizeConfig, UnboundedReceiverChunker,
};

/// A helper struct that will invoke [`JoinHandle::abort`] when dropped.
#[derive(Debug)]
pub struct AbortOnDrop<T = ()> {
    join_handle: Option<JoinHandle<T>>,
}

impl<T> AbortOnDrop<T> {
    pub fn new(join_handle: JoinHandle<T>) -> Self {
        Self {
            join_handle: Some(join_handle),
        }
    }

    /// Disarms the [`AbortOnDrop`], causing it to no longer abort the join handle when dropped.
    pub fn run_forever(mut self) {
        self.join_handle = None;
    }

    pub fn into_inner(mut self) -> JoinHandle<T> {
        self.join_handle
            .take()
            .expect("invariant: the inner join handle should always be present")
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if let Some(join_handle) = self.join_handle.take() {
            join_handle.abort();
        }
    }
}

impl<T> From<JoinHandle<T>> for AbortOnDrop<T> {
    fn from(join_handle: JoinHandle<T>) -> Self {
        Self::new(join_handle)
    }
}

impl<T> Future for AbortOnDrop<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let join_handle = self
            .join_handle
            .as_mut()
            .expect("polling after `AbortOnDrop` already completed");

        Pin::new(join_handle).poll(cx)
    }
}
