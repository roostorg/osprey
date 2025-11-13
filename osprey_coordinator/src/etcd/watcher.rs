use crate::backoff_utils::{AsyncBackoff, Config};
use crate::etcd::{Client, EtcdError};
use etcd::{kv::KeyValueInfo, Response};
use futures::Stream;
use log::error;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::sleep;

type ResponseFuture =
    Pin<Box<dyn Future<Output = Result<Response<KeyValueInfo>, EtcdError>> + Send>>;
type BackoffFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// A single event returned from watch stream.
pub enum WatchEvent {
    /// The initial information when a watch is started.
    /// `KeyValueInfo` is `None` if the key doesn't exist.
    Get(Option<KeyValueInfo>),
    /// A node update.
    Update(KeyValueInfo),
}

/// The next future that should be polled in the `Stream::poll_next` method.
enum PendingFuture {
    Get(ResponseFuture),
    Watch(ResponseFuture),
    Backoff(BackoffFuture),
}

/// A watcher that will stream watch events until it is dropped. It handles backoffs if etcd
/// goes offline or returns errors.
pub struct Watcher {
    /// Etcd client.
    client: Client,
    /// Key to watch.
    key: String,
    /// Backoff for when errors happen.
    backoff: AsyncBackoff,
    /// The future that should be polled next during `Stream::poll_next`.
    pending_future: Option<PendingFuture>,
}

impl Watcher {
    pub fn new(client: Client, key: String) -> Self {
        let config = Config {
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(15),
            ..Default::default()
        };
        let backoff = AsyncBackoff::new(config);
        Self {
            client,
            key,
            backoff,
            pending_future: None,
        }
    }
}

impl Stream for Watcher {
    type Item = WatchEvent;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context) -> Poll<Option<Self::Item>> {
        let watcher = Pin::get_mut(self);

        // Order of operations.
        // 1. Do a get to retrieve the initial index.
        // 2. Start a watch from the index.
        // 3. Loop on the watch until there is an error.
        //
        // If there is ever an error, start a backoff. Once the backoff is complete, go to step 1.
        let (poll, next_future) = match watcher.pending_future.take() {
            // Poll the current pending future if there isn't one.
            Some(pending_future) => watcher.poll_pending(pending_future, context),
            // There is no pending future. We are just starting the stream. Start by doing a get.
            None => watcher.start_get(context),
        };

        watcher.pending_future = Some(next_future);
        poll
    }
}

impl Watcher {
    /// Unwrap the current `pending_future` and poll it.
    fn poll_pending(
        &mut self,
        pending_future: PendingFuture,
        context: &mut Context,
    ) -> (Poll<Option<WatchEvent>>, PendingFuture) {
        match pending_future {
            PendingFuture::Backoff(backoff_future) => self.poll_backoff(backoff_future, context),
            PendingFuture::Watch(watch_future) => self.poll_watch(watch_future, context),
            PendingFuture::Get(get_future) => self.poll_get(get_future, context),
        }
    }

    /// Poll the backoff future. If the backoff is ready then it returns Ready.
    fn poll_backoff(
        &mut self,
        mut backoff_future: BackoffFuture,
        context: &mut Context,
    ) -> (Poll<Option<WatchEvent>>, PendingFuture) {
        match backoff_future.as_mut().poll(context) {
            Poll::Ready(()) => self.start_get(context),
            Poll::Pending => (Poll::Pending, PendingFuture::Backoff(backoff_future)),
        }
    }

    /// Poll the watch future.
    fn poll_watch(
        &mut self,
        mut watch_future: ResponseFuture,
        context: &mut Context,
    ) -> (Poll<Option<WatchEvent>>, PendingFuture) {
        match watch_future.as_mut().poll(context) {
            Poll::Ready(Err(error)) => self.start_backoff(context, error.into()),
            Poll::Ready(Ok(response)) => {
                let after_index = match response.data.node.modified_index {
                    Some(after_index) => after_index,
                    None => return self.start_backoff(context, EtcdError::NoIndex.into()),
                };
                (
                    Poll::Ready(Some(WatchEvent::Update(response.data))),
                    self.make_watch_future(after_index),
                )
            }
            Poll::Pending => (Poll::Pending, PendingFuture::Watch(watch_future)),
        }
    }

    /// Poll the get future.
    fn poll_get(
        &mut self,
        mut get_future: ResponseFuture,
        context: &mut Context,
    ) -> (Poll<Option<WatchEvent>>, PendingFuture) {
        match get_future.as_mut().poll(context) {
            Poll::Ready(Ok(response)) => {
                let after_index = match response.cluster_info.etcd_index {
                    Some(after_index) => after_index,
                    None => return self.start_backoff(context, EtcdError::NoIndex.into()),
                };

                self.backoff.succeed();
                (
                    Poll::Ready(Some(WatchEvent::Get(Some(response.data)))),
                    self.make_watch_future(after_index),
                )
            }
            Poll::Ready(Err(error)) => match error {
                EtcdError::KeyNotFound {
                    index: after_index,
                    error: _error,
                } => (
                    Poll::Ready(Some(WatchEvent::Get(None))),
                    self.make_watch_future(after_index),
                ),
                other => self.start_backoff(context, other.into()),
            },
            Poll::Pending => (Poll::Pending, PendingFuture::Get(get_future)),
        }
    }

    /// Start the backoff future.
    fn start_backoff(
        &mut self,
        context: &mut Context,
        error: anyhow::Error,
    ) -> (Poll<Option<WatchEvent>>, PendingFuture) {
        let backoff_duration = self.backoff.fail();
        error!(
            "Watch error. error={:?}. Starting backoff {:?}",
            error, backoff_duration
        );

        let backoff_future = Box::pin(async move { sleep(backoff_duration).await });
        self.poll_backoff(backoff_future, context)
    }

    /// Start the get future.
    fn start_get(&mut self, context: &mut Context) -> (Poll<Option<WatchEvent>>, PendingFuture) {
        let client = self.client.clone();
        let key = self.key.clone();
        let get_future = Box::pin(async move { client.get(&*key).await });
        self.poll_get(get_future, context)
    }

    /// Make a watch future.
    fn make_watch_future(&mut self, after_index: u64) -> PendingFuture {
        let client = self.client.clone();
        let key = self.key.clone();
        PendingFuture::Watch(Box::pin(async move {
            client.watch_recursive(&key, after_index).await
        }))
    }
}
