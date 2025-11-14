use std::sync::Arc;

use crate::etcd::Client;
use crate::etcd_watcherd::{RecursiveKeyWatchEvents, Watcher};
use anyhow::Result;

use base64::{engine::general_purpose::STANDARD as BASE64_ENGINE, Engine};
pub use etcd_config_derive::{Disconfig, FeatureFlags};

use prost::Message;

/// Knows how to handle updates from etcd config changes.
pub trait KeyHandler {
    /// Invoked anytime a configuration change to `key` has been detected.
    /// `value` is the raw string contents stored at `key`.
    fn handle_key_updated(&self, key: &str, value: Option<&str>);
}

/// Validate a disconfig after the value from etcd event update has been successfully
/// decoded into a Protobuf message object. The default implementation returns the decoded protobuf object.
/// The type placehoder `T` is the Protobuf message object that the disconfig will be decoded into.
/// The `validate` method can be overridden to perform customized validation on the decoded Protobuf object.
pub trait HandleDisconfigUpdated {
    type Disconfig: Message + Default;
    type Error;

    /// If customizing the validation, return `None` if the decoded Protobuf object is invalid.
    fn validate(proto: Self::Disconfig) -> Result<Self::Disconfig, Self::Error> {
        Ok(proto)
    }

    /// Invoked after the in-memory ArcSwap wrapped Disconfig has been updated.
    fn after_update(&self) {}
}

/// Creates a (non-secure) Etcd client to watch for any value changes recursively under the `config_key_root` path.
/// This watcher is kept alive in the background.
/// Updates to the values under the `config_key_root` are sent to the [KeyHandler] to handle.
pub async fn run_etcd_watcher<T: KeyHandler + Send + Sync + 'static>(
    config_key_root: impl Into<String>,
    key_handler: Arc<T>,
) -> Result<()> {
    let etcd_client = Client::from_etcd_peers()?;
    // let etcd_client = Arc::new(etcd_client);
    let watcher = Watcher::new(etcd_client);
    let response = watcher.watch_key_recursive(config_key_root).await?;

    let mut events = response.events();
    // Handle full sync event before resolving the future.
    if let RecursiveKeyWatchEvents::FullSync { items } = events.initial_event() {
        for (key, value) in items.iter() {
            key_handler.handle_key_updated(key, Some(value));
        }
    }

    // Create background task to monitor for changes
    tokio::spawn(async move {
        loop {
            let event = events.next().await;
            match event {
                RecursiveKeyWatchEvents::FullSync { items } => {
                    for (key, value) in items.iter() {
                        key_handler.handle_key_updated(key, Some(value));
                    }
                }
                RecursiveKeyWatchEvents::SyncOne { key, value } => {
                    key_handler.handle_key_updated(&key, Some(&value));
                }
                RecursiveKeyWatchEvents::DeleteOne { key, prev_value: _ } => {
                    key_handler.handle_key_updated(&key, None);
                }
            }
        }
    });

    Ok(())
}

const PATTERN: &[char] = &['"', ' '];

pub fn convert_to_bool(value: Option<&str>) -> bool {
    value.map_or(false, |v| {
        v.trim_matches(PATTERN).parse::<f32>().unwrap_or_default() > 0.0
    })
}

pub fn convert_to_float(value: Option<&str>) -> f32 {
    value.map_or(0.0, |v| {
        v.trim_matches(PATTERN).parse::<f32>().unwrap_or_default()
    })
}

pub fn convert_to_usize(value: Option<&str>) -> usize {
    value.map_or(0, |v| {
        v.trim_matches(PATTERN).parse::<usize>().unwrap_or_default()
    })
}

pub fn convert_to_u64(value: Option<&str>) -> u64 {
    value.map_or(0, |v| {
        v.trim_matches(PATTERN).parse::<u64>().unwrap_or_default()
    })
}

pub fn convert_to_u32(value: Option<&str>) -> u32 {
    value.map_or(0, |v| {
        v.trim_matches(PATTERN).parse::<u32>().unwrap_or_default()
    })
}

pub fn base64_to_proto<T: Message + Default>(value: Option<&str>) -> Result<Option<T>> {
    let parsed_str = match value {
        Some(value) => match BASE64_ENGINE.decode(value.as_bytes()) {
            Ok(binary) => binary,
            Err(e) => return Err(anyhow::anyhow!("Failed to decode base64 value: {:?}", e)),
        },
        None => return Ok(None),
    };

    let proto = T::decode(parsed_str.as_slice())
        .map_err(|e| anyhow::anyhow!("Failed to decode protobuf: {:?}", e))?;
    Ok(Some(proto))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_convert_to_float() {
        assert_eq!(0.0, convert_to_float(None));
        assert_eq!(0.0, convert_to_float(Some("")));
        assert_eq!(0.0, convert_to_float(Some(" ")));

        assert_eq!(0.0, convert_to_float(Some("0")));
        assert_eq!(0.0, convert_to_float(Some("0.0")));
        assert_eq!(0.0, convert_to_float(Some(" 0.0 ")));
        assert_eq!(0.1234, convert_to_float(Some("0.1234")));
        assert_eq!(0.1234, convert_to_float(Some("\" 0.1234 \"")));

        assert_eq!(1.0, convert_to_float(Some("1.0")));
        assert_eq!(1.0, convert_to_float(Some(" 1.0 ")));
    }

    #[test]
    fn test_convert_to_bool() {
        assert!(!convert_to_bool(None));
        assert!(!convert_to_bool(Some("")));
        assert!(!convert_to_bool(Some(" ")));

        assert!(!convert_to_bool(Some("0")));
        assert!(!convert_to_bool(Some("0.0")));
        assert!(!convert_to_bool(Some(" 0.0 ")));
        assert!(convert_to_bool(Some("0.1234")));
        assert!(convert_to_bool(Some("\" 0.1234 \"")));

        assert!(convert_to_bool(Some("1.0")));
        assert!(convert_to_bool(Some(" 1.0 ")));
    }

    #[test]
    fn test_convert_to_usize() {
        assert_eq!(0, convert_to_usize(None));
        assert_eq!(0, convert_to_usize(Some("")));
        assert_eq!(0, convert_to_usize(Some(" ")));

        assert_eq!(0, convert_to_usize(Some("0")));
        assert_eq!(0, convert_to_usize(Some("0.0")));
        assert_eq!(0, convert_to_usize(Some(" 0.0 ")));
        assert_eq!(0, convert_to_usize(Some("1.0")));
        assert_eq!(0, convert_to_usize(Some("-1")));
        assert_eq!(1234, convert_to_usize(Some("1234")));
        assert_eq!(1234, convert_to_usize(Some("\" 1234 \"")));

        assert_eq!(1, convert_to_usize(Some("1")));
        assert_eq!(1, convert_to_usize(Some(" 1 ")));
    }

    #[test]
    fn test_convert_to_u64() {
        assert_eq!(0, convert_to_u64(None));
        assert_eq!(0, convert_to_u64(Some("")));
        assert_eq!(0, convert_to_u64(Some(" ")));

        assert_eq!(0, convert_to_u64(Some("0")));
        assert_eq!(0, convert_to_u64(Some("0.0")));
        assert_eq!(0, convert_to_u64(Some(" 0.0 ")));
        assert_eq!(0, convert_to_u64(Some("1.0")));
        assert_eq!(0, convert_to_u64(Some("-1")));
        assert_eq!(1234, convert_to_u64(Some("1234")));
        assert_eq!(1234, convert_to_u64(Some("\" 1234 \"")));
        assert_eq!(1234, convert_to_u64(Some("1234")));
        assert_eq!(1234, convert_to_u64(Some("1234")));
        assert_eq!(
            1227039953469571133,
            convert_to_u64(Some("1227039953469571133"))
        );
    }
}
