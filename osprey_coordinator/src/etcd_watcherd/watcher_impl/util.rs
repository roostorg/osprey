use std::time::Duration;

use rand::{thread_rng, Rng};

/// Checks a given etcd error to see if it contains the special error code "event index cleared"
/// which signifies that the watcher should be reset.
pub(super) fn err_should_reset_watcher(err: &crate::etcd::EtcdError) -> bool {
    const ETCD_ERROR_CODE_EVENT_INDEX_CLEARED: u64 = 401;

    match err {
        crate::etcd::EtcdError::API {
            error: crate::etcd::EtcdApiError::Api(api_error),
            ..
        } if api_error.error_code == ETCD_ERROR_CODE_EVENT_INDEX_CLEARED => true,
        _ => false,
    }
}

/// Checks to see if the error is a watch timeout.
pub(super) fn err_is_watch_timeout(err: &crate::etcd::EtcdError) -> bool {
    match err {
        crate::etcd::EtcdError::Watch {
            error: crate::etcd::WatchError::Timeout,
        } => true,
        _ => false,
    }
}

/// Returns a timeout, with jitter for how long we should watch the key, before timing out and trying again.
pub(super) fn jittered_watch_timeout() -> Duration {
    // These timeouts are specifically chosen to be high, because the etcd client uses tcp_keepalive, and we can detect
    // zombied connections using that. So we'll use a higher timeout here so we can hopefully reduce watcher churn.
    const WATCH_TIMEOUT_MIN_MS: u64 = 600_000;
    const WATCH_TIMEOUT_MAX_MS: u64 = 1_200_000;

    Duration::from_millis(thread_rng().gen_range(WATCH_TIMEOUT_MIN_MS..=WATCH_TIMEOUT_MAX_MS))
}
