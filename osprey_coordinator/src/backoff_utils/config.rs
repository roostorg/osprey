use backoff::exponential::ExponentialBackoff;
use std::time::Duration;

#[derive(Clone)]
pub struct Config {
    /// The initial retry interval.
    pub min_delay: Duration,
    /// The maximum value of the back off period. Once the retry interval reaches this
    /// value it stops increasing.
    pub max_delay: Duration,
    /// The value to multiply the current interval with for each retry attempt.
    pub multiplier: f64,
}

impl Config {
    /// `Default` isn't `const` so we need this workaround, see:
    /// https://stackoverflow.com/a/72467679
    pub const fn new_const_default() -> Self {
        Self {
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60 * 5),
            multiplier: 2.0,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new_const_default()
    }
}

impl From<Config> for ExponentialBackoff<backoff::SystemClock> {
    fn from(val: Config) -> Self {
        ExponentialBackoff {
            initial_interval: val.min_delay,
            multiplier: val.multiplier,
            max_interval: val.max_delay,
            // XXX: Setting this to Some(duration) will result in the backoff eventually
            // failing. i.e. inner.next_backoff() will return None!
            // Do not change this!
            max_elapsed_time: None,
            ..Default::default()
        }
    }
}
