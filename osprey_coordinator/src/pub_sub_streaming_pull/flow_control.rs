/// The minimum and default value for [`FlowControl::min_duration_per_please_extension`]
const MIN_LEASE_EXTENSION_DURATION_SECS: u32 = 10;

/// The maximum and default value for [`FlowControl::max_duration_per_please_extension`]
const MAX_LEASE_EXTENSION_DURATION_SECS: u32 = 600;

/// Flow control is used to control the buffering of messages for a given [`crate::StreamingPullManager`].
#[derive(Clone, Debug)]
pub struct FlowControl {
    pub(crate) max_bytes: usize,
    pub(crate) max_messages: usize,
    pub(crate) max_processing_messages: usize,
    pub(crate) min_duration_per_lease_extension: u32,
    pub(crate) max_duration_per_lease_extension: u32,
}

impl Default for FlowControl {
    fn default() -> Self {
        Self::new()
    }
}

impl FlowControl {
    pub const fn new() -> Self {
        Self {
            max_bytes: 100 * 1024 * 1024,
            max_messages: 1000,
            max_processing_messages: 1000,
            min_duration_per_lease_extension: MIN_LEASE_EXTENSION_DURATION_SECS,
            max_duration_per_lease_extension: MAX_LEASE_EXTENSION_DURATION_SECS,
        }
    }

    /// The maximum total size of receiveed - but not yet processed - messages before
    /// pausing the message stream.
    ///
    /// Default: 100MiB.
    pub const fn set_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// The maximum number of messages that can be received without being acked/nacked before pausing the message
    /// stream.
    ///
    /// Default: 1000
    pub const fn set_max_messages(mut self, max_messages: usize) -> Self {
        self.max_messages = max_messages;
        self
    }

    /// The maximum number of messages that will be sent to the message handler, and be outstanding at a given time.
    ///
    /// This is essentially a limit to how many messages will be concurrently handled by the message handler.
    ///
    /// Default: 1000
    pub const fn set_max_processing_messages(mut self, max_processing_messages: usize) -> Self {
        self.max_processing_messages = max_processing_messages;
        self
    }

    /// Sets minimum and maximum number of seconds that we will bump any lease forward.
    ///
    /// The minimum must be >= 10.
    /// The maximum must be <= 600.
    /// The maximum must be >= the minimum.
    ///
    /// Default: (min = 10 seconds, max = 600 seconds)
    ///
    /// # Panics
    ///
    /// Panics if any of the invariants above are not met.
    pub fn set_duration_per_lease_extension(mut self, min: u32, max: u32) -> Self {
        if min < MIN_LEASE_EXTENSION_DURATION_SECS {
            panic!(
                "Minimum of {} is under acceptable value of {}",
                min, MIN_LEASE_EXTENSION_DURATION_SECS
            );
        }

        if max > MAX_LEASE_EXTENSION_DURATION_SECS {
            panic!(
                "Maximum of {} is above acceptable value of {}",
                max, MAX_LEASE_EXTENSION_DURATION_SECS
            );
        }

        if min > max {
            panic!("Minimum ({}) is greater than Maximum ({})", min, max);
        }

        self.min_duration_per_lease_extension = min;
        self.max_duration_per_lease_extension = max;
        self
    }
}
