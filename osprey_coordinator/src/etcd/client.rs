use crate::future_utils::time::timeout_at;
use anyhow::{Context, Result};
use etcd::{kv, ClientBuilder, Response};
use futures::future::TryFutureExt;
use serde::Serialize;
use std::env;
use std::time::{Duration, Instant};

use crate::etcd::EtcdError;

#[derive(Debug, Clone)]
pub struct Config {
    request_timeout: Duration,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            request_timeout: Duration::from_secs(15),
        }
    }
}

fn base_client_builder(key: &str) -> Result<ClientBuilder> {
    let endpoints_str =
        env::var(key).with_context(|| format!("{} environment variable not set.", key))?;
    let endpoints = endpoints_str.split(',').collect::<Vec<&str>>();
    Ok(etcd::ClientBuilder::new(&endpoints)
        .with_tcp_keepalive(Duration::from_secs(30))
        .with_request_timeout(Duration::from_secs(5)))
}

/// Http client for Etcd keys api.
#[derive(Debug, Clone)]
pub struct Client {
    inner: etcd::Client,
    config: Config,
}

impl Client {
    /// Create a new client from an environment variable with comma-separated addresses.
    fn from_environment(key: &str) -> Result<Self> {
        let builder = base_client_builder(key)?;
        let config = Config::default();
        Ok(Self {
            inner: builder.build(),
            config,
        })
    }
    /// Create a new client from the ETCD_PEERS environment variable
    pub fn from_etcd_peers() -> Result<Self> {
        Self::from_environment("ETCD_PEERS")
    }

    /// Create a new client from the ETCD_LOCK environment variable
    pub fn from_etcd_lock() -> Result<Self> {
        Self::from_environment("ETCD_LOCK")
    }

    /// Create a new client from the ETCD_SECURE_PEERS environment variable
    pub fn from_etcd_secure_peers() -> Result<Self> {
        let builder = base_client_builder("ETCD_SECURE_PEERS")?;
        let username =
            env::var("ETCD_USERNAME").context("ETCD_USERNAME environment variable not set.")?;
        let password =
            env::var("ETCD_PASSWORD").context("ETCD_PASSWORD environment variable not set.")?;
        let inner = builder.with_basic_auth(username, password).build();
        let config = Config::default();
        Ok(Self { inner, config })
    }

    /// Create a new client.
    ///
    /// # Arguments
    ///
    /// * `endpoints` - list of etcd endpoints. Called in order until one succeeds.
    pub fn new<T: AsRef<str>>(endpoints: &[T]) -> Result<Self> {
        let config = Config::default();
        Ok(Self {
            inner: etcd::Client::new(&endpoints.iter().map(|x| x.as_ref()).collect::<Vec<_>>()),
            config,
        })
    }

    /// Create a new client with config.
    ///
    /// # Arguments
    ///
    /// * `endpoints` - list of etcd endpoints. Called in order until one succeeds.
    /// * `config` - configuration options
    pub fn new_with_config(endpoints: &[&str], config: Config) -> Result<Self> {
        Ok(Self {
            inner: etcd::Client::new(endpoints),
            config,
        })
    }

    /// Create the value of a key.
    /// Fails if the key already exists.
    pub async fn create<'a, K: Into<&'a str>, O: Serialize + 'a>(
        &'a self,
        key: K,
        object: O,
        ttl: Option<u64>,
    ) -> Result<Response<kv::KeyValueInfo>, EtcdError> {
        let key: &str = key.into();
        let deadline = Instant::now() + self.config.request_timeout;
        let value = serde_json::to_string(&object)
            .map_err(|err| EtcdError::Serialize { error: err.into() })?;
        let fut = kv::create(&self.inner, key, &value, ttl).map_err(EtcdError::from);
        timeout_at(deadline, fut).await
    }

    /// Get the value of a key.
    pub async fn get<'a, K: Into<&'a str>>(
        &'a self,
        key: K,
    ) -> Result<Response<kv::KeyValueInfo>, EtcdError> {
        let key: &str = key.into();
        let deadline = Instant::now() + self.config.request_timeout;
        let fut = kv::get(&self.inner, key, kv::GetOptions::default()).map_err(EtcdError::from);
        timeout_at(deadline, fut).await
    }

    /// Get the value of a key.
    pub async fn get_recursive<'a, K: Into<&'a str>>(
        &'a self,
        key: K,
    ) -> Result<Response<kv::KeyValueInfo>, EtcdError> {
        let key: &str = key.into();
        let deadline = Instant::now() + self.config.request_timeout;
        let fut = kv::get(
            &self.inner,
            key,
            kv::GetOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .map_err(EtcdError::from);
        timeout_at(deadline, fut).await
    }

    /// Set the value of a key.
    pub async fn set<'a, K: Into<&'a str>, O: Serialize + 'a>(
        &'a self,
        key: K,
        object: O,
        ttl: Option<u64>,
    ) -> Result<Response<kv::KeyValueInfo>, EtcdError> {
        let key: &str = key.into();
        let deadline = Instant::now() + self.config.request_timeout;
        let value = serde_json::to_string(&object)
            .map_err(|err| EtcdError::Serialize { error: err.into() })?;
        let fut = kv::set(&self.inner, key, &value, ttl).map_err(EtcdError::from);
        timeout_at(deadline, fut).await
    }

    /// Refreshes the ttl of a key without triggering watchers.
    pub async fn refresh<'a, K: Into<&'a str>>(
        &'a self,
        key: K,
        ttl: u64,
    ) -> Result<Response<kv::KeyValueInfo>, EtcdError> {
        let key: &str = key.into();
        let deadline = Instant::now() + self.config.request_timeout;
        let fut = kv::refresh(&self.inner, key, ttl).map_err(EtcdError::from);
        timeout_at(deadline, fut).await
    }

    /// Deletes a node only if the given current value and/or current modified index match.
    pub async fn compare_and_delete<'a, K: Into<&'a str>>(
        &'a self,
        key: K,
        current_value: Option<&str>,
        current_modified_index: Option<u64>,
    ) -> Result<(), EtcdError> {
        let key: &str = key.into();
        let deadline = Instant::now() + self.config.request_timeout;
        let fut = kv::compare_and_delete(&self.inner, key, current_value, current_modified_index)
            .map_err(EtcdError::from);
        timeout_at(deadline, fut).await.map(|_response| ())
    }

    /// Deletes a key.
    pub async fn delete<'a, K: Into<&'a str>>(&'a self, key: K) -> Result<(), EtcdError> {
        let key: &str = key.into();
        let deadline = Instant::now() + self.config.request_timeout;
        let fut = kv::delete(&self.inner, key, false).map_err(EtcdError::from);
        timeout_at(deadline, fut).await.map(|_response| ())
    }

    /// Watches a key.
    ///
    /// # Arguments
    ///
    /// * `key` - key to watch
    /// * `after_index` - return events only after `after_index`
    pub async fn watch_recursive<'a>(
        &'a self,
        key: &'a str,
        after_index: u64,
    ) -> Result<Response<kv::KeyValueInfo>, EtcdError> {
        let watch_options = kv::WatchOptions {
            index: Some(after_index + 1),
            recursive: true,
            timeout: None,
        };
        kv::watch(&self.inner, key, watch_options)
            .map_err(EtcdError::from)
            .await
    }

    /// Watches a key.
    ///
    /// # Arguments
    ///
    /// * `key` - key to watch
    /// * `after_index` - return events only after `after_index`
    pub async fn watch<'a>(
        &'a self,
        key: &'a str,
        after_index: u64,
    ) -> Result<Response<kv::KeyValueInfo>, EtcdError> {
        let watch_options = kv::WatchOptions {
            index: Some(after_index + 1),
            recursive: false,
            timeout: None,
        };
        kv::watch(&self.inner, key, watch_options)
            .map_err(EtcdError::from)
            .await
    }

    /// Watches a key, allowing various options to be provided
    ///
    /// # Arguments
    /// * `key` - key to watch
    /// * `after_index` - return events only after `after_index`
    /// * `recursive` - whether the watch should be recursive
    /// * `timeout` - when the watch should time out.
    pub async fn watch_opt(
        &self,
        key: impl AsRef<str>,
        after_index: u64,
        recursive: bool,
        timeout: Duration,
    ) -> Result<Response<kv::KeyValueInfo>, EtcdError> {
        let watch_options = kv::WatchOptions {
            index: Some(after_index + 1),
            recursive,
            timeout: Some(timeout),
        };
        kv::watch(&self.inner, key, watch_options)
            .map_err(EtcdError::from)
            .await
    }
}
