use crate::discovery::service::ServiceRegistration;
use crate::etcd::{Client, EtcdError};
use anyhow::Result;
use log::trace;
use std::time::Duration;
use tokio::time::error::Elapsed;

const WAIT_FOR_FREE_KEY_RETRY_INTERVAL: Duration = Duration::from_secs(2);

/// Waits for a given key to become free from service discovery.
/// An upper timeout is required since this method runs indefinitely otherwise.
///
/// # Arguments
///
/// * `client` - the etcd client to use
/// * `key` - the service key to wait for to become free
/// * `timeout` - the `Duration` after which the method will return an error if the key is not freed
pub async fn wait_for_free_key(
    client: &Client,
    key: &str,
    timeout: Duration,
) -> Result<(), Elapsed> {
    tokio::time::timeout(timeout, async move {
        loop {
            let response = client.get(key).await;
            match response {
                Ok(response) => {
                    if response.data.node.value.is_none() {
                        // key is *basically* free („• ᴗ •„)
                        return;
                    }
                }
                Err(err) => match err {
                    EtcdError::KeyNotFound { index: _, error: _ } => {
                        return;
                    }
                    _ => {}
                },
            }
            tokio::time::sleep(WAIT_FOR_FREE_KEY_RETRY_INTERVAL).await;
        }
    })
    .await
}

/// Registers a service in service discovery.
///
/// # Arguments
///
/// * `client` - the etcd client to use
/// * `service` - the service to register
/// * `ttl` - the `Duration` for the service to stay in discovery until it automatically falls
///           falls out.
pub async fn register(client: &Client, service: &ServiceRegistration, ttl: Duration) -> Result<()> {
    trace!("Registering {:?}", service);
    let key = service.key();
    Ok(client
        .set(&*key, service, Some(ttl.as_secs()))
        .await
        .map(|_response| ())?)
}

/// Refreshes a service registration in discovery.
///
/// # Arguments
///
/// * `client` - the etcd client to use
/// * `service` - the service to register
/// * `ttl` - the `Duration` for the service to stay in discovery until it automatically falls
///           falls out.
pub async fn refresh(client: &Client, service: &ServiceRegistration, ttl: Duration) -> Result<()> {
    trace!("Refreshing {:?}", service);
    let key = service.key();
    Ok(client
        .refresh(&*key, ttl.as_secs())
        .await
        .map(|_response| ())?)
}

/// Unregister a service from discovery.
///
/// # Arguments
///
/// * `client` - the etcd client to use
/// * `service` - the service to unregister
pub async fn unregister(client: &Client, service: &ServiceRegistration) -> Result<()> {
    trace!("Unregistering {:?}", service);
    let key = &*service.key();
    Ok(client.delete(key).await?)
}
