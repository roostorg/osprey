use crate::backoff_utils::Config;
use backoff::{backoff::Backoff, exponential::ExponentialBackoff};
use std::time::Duration;
use tokio::time::sleep;

/// A futures safe backoff.
pub struct AsyncBackoff {
    config: Config,
    inner: ExponentialBackoff<backoff::SystemClock>,
}

impl AsyncBackoff {
    pub fn new(config: Config) -> Self {
        Self {
            config: config.clone(),
            inner: config.into(),
        }
    }

    /// Sleeps the current future.
    pub async fn sleep(&mut self) {
        let backoff_duration = self.fail();
        sleep(backoff_duration).await
    }

    /// Returns the duration of the next backoff. Consumes the next backoff, and increases the `Duration`
    /// that is returned the next time this function is called.
    pub fn fail(&mut self) -> Duration {
        self.inner.next_backoff().unwrap_or(self.config.max_delay)
    }

    /// Resets the backoff.
    pub fn succeed(&mut self) {
        self.inner.reset()
    }
}
