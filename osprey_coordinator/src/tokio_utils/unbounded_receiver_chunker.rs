use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::{sync::broadcast::Receiver, sync::mpsc::UnboundedReceiver, time::Instant};

use crate::tokio_utils::AbortOnDrop;

pub struct UnboundedReceiverChunker<T> {
    chunker: Chunker<T>,
    receiver: UnboundedReceiver<T>,
    chunk_timeout: Arc<RwLock<Duration>>,
    inactive_timeout: Duration,
    timeout_at: Instant,
    _disconfig_update_guard: Option<Arc<AbortOnDrop<()>>>,
}

/// Trait to get chunk timeout configuration
pub trait ChunkSizeConfig: Send + Sync + 'static {
    // Returns the configured value for chunk timeout in seconds
    fn chunk_timeout_secs(&self) -> u64;

    // Returns a receiver to subscribe to updates to the configuration
    fn subscribe(&self) -> Receiver<()>;
}

impl<T> UnboundedReceiverChunker<T> {
    pub fn new(
        receiver: UnboundedReceiver<T>,
        max_chunk_size: usize,
        default_chunk_timeout: Duration,
        inactive_timeout: Duration,
    ) -> Self {
        Self {
            chunker: Chunker::new(max_chunk_size),
            chunk_timeout: Arc::new(default_chunk_timeout.into()),
            inactive_timeout,
            receiver,
            timeout_at: Instant::now() + inactive_timeout,
            _disconfig_update_guard: None,
        }
    }

    pub fn with_configurable_timeout(&mut self, disconfig: Arc<impl ChunkSizeConfig>) {
        let chunk_timeout_secs = disconfig.chunk_timeout_secs();
        if chunk_timeout_secs != 0 {
            *self.chunk_timeout.write().unwrap() = Duration::from_secs(chunk_timeout_secs);
        }

        let mut update_rx = disconfig.subscribe();
        let cloned_chunk_timeout = self.chunk_timeout.clone();

        let join_handle = tokio::spawn(async move {
            while let Ok(_) = update_rx.recv().await {
                let chunk_timeout_secs = disconfig.chunk_timeout_secs();
                if chunk_timeout_secs != 0 {
                    *cloned_chunk_timeout.write().unwrap() =
                        Duration::from_secs(chunk_timeout_secs);
                }
            }
        });
        self._disconfig_update_guard = Some(Arc::new(join_handle.into()));
    }

    pub async fn next_chunk(&mut self) -> Option<Vec<T>> {
        loop {
            match tokio::time::timeout_at(self.timeout_at, self.receiver.recv()).await {
                Ok(Some(item)) => {
                    // We got an item that made us non-empty, adjust the timeout accordingly.
                    if self.chunker.is_empty() {
                        self.timeout_at = Instant::now() + *self.chunk_timeout.read().unwrap();
                    }
                    self.chunker.push(item);
                }
                // The channel will no longer receive any more messages. Flush any chunk we might have,
                // then the next time this function is called, we'll go ahead and return None.
                Ok(None) => {
                    return (!self.chunker.is_empty()).then(|| self.chunker.take_current_chunk())
                }
                // We timed out, and we have an empty chunker, which means we're going to shut down due to inactivity.
                Err(_) if self.chunker.is_empty() => self.receiver.close(),
                Err(_) => {
                    // Our next timeout will cause us to go inactive, unless we receive a message between now and then.
                    self.timeout_at = Instant::now() + self.inactive_timeout;
                    return Some(self.chunker.take_current_chunk());
                }
            }

            // Check if a chunk has filled, and return it.
            if let Some(full_chunk) = self.chunker.take_full_chunk() {
                return Some(full_chunk);
            }
        }
    }
}

struct Chunker<T> {
    current_chunk: Vec<T>,
    max_chunk_size: usize,
    full_chunk: Option<Vec<T>>,
}

impl<T> Chunker<T> {
    fn new(max_chunk_size: usize) -> Self {
        assert!(max_chunk_size > 0);

        Self {
            current_chunk: Vec::with_capacity(max_chunk_size),
            full_chunk: None,
            max_chunk_size,
        }
    }

    fn is_empty(&self) -> bool {
        self.current_chunk.is_empty() && self.full_chunk.is_none()
    }

    fn take_full_chunk(&mut self) -> Option<Vec<T>> {
        self.full_chunk.take()
    }

    fn push(&mut self, item: T) {
        self.current_chunk.push(item);

        if self.current_chunk.len() == self.max_chunk_size {
            let chunk = self.take_current_chunk();
            self.full_chunk = Some(chunk);
        }
    }

    fn take_current_chunk(&mut self) -> Vec<T> {
        std::mem::replace(
            &mut self.current_chunk,
            Vec::with_capacity(self.max_chunk_size),
        )
    }
}
