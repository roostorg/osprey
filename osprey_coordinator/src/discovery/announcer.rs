use crate::backoff_utils::{AsyncBackoff, Config as BackoffConfig};
use crate::discovery::directory;
use crate::discovery::service::ServiceRegistration;
use crate::etcd::Client;
use anyhow::Result;
use futures::future::FutureExt;
use log::{debug, error, info, warn};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::{spawn, JoinHandle};

const DEFAULT_TTL: Duration = Duration::from_secs(30);
const SHUTDOWN_RETRY_ATTEMPTS: usize = 3;

#[derive(Clone, Debug)]
struct Config {
    /// Registration that should be announced.
    registration: ServiceRegistration,
    /// Duration the service should stay announced before automatically falling out of service
    /// discovery.
    ttl: Duration,
    /// Etcd client.
    client: Arc<Client>,
    /// Whether or not to wait for the key to be free before registering. Defaults to `false` if the `Option` is `None`
    wait_for_free_key_timeout: Option<Duration>,
}

/// Builder for a [`ServiceAnnouncer`].
#[derive(Clone, Debug)]
pub struct Builder {
    config: Config,
}

impl Builder {
    fn new(registration: ServiceRegistration, client: Arc<Client>) -> Self {
        Builder {
            config: Config {
                registration,
                ttl: DEFAULT_TTL,
                client,
                wait_for_free_key_timeout: None,
            },
        }
    }

    pub fn wait_for_free_key(mut self, timeout: Option<Duration>) -> Self {
        self.config.wait_for_free_key_timeout = timeout;
        self
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.config.ttl = ttl;
        self
    }

    /// Start the announcer.
    /// It will continue announcing until [`ServiceAnnouncer.stop`] is called.
    ///
    /// [`ServiceAnnouncer.stop`]: struct.ServiceAnnouncer.html#stop
    pub fn start(self) -> ServiceAnnouncer {
        info!("Starting announcer.");
        let (stop_tx, stop_rx) = oneshot::channel();
        let (drain_tx, drain_rx) = oneshot::channel();

        let join_handle = spawn(async move {
            announce(self.config, stop_rx, drain_rx).await;
        });

        ServiceAnnouncer {
            state: Some(State {
                stop_tx,
                drain_tx: Some(drain_tx),
                join_handle,
            }),
        }
    }
}

/// A service announcer used to automatically register a service in discovery at an interval.
/// Dropping the `ServiceAnnouncer` will send a shutdown signal to the announcement task,
/// but in most cases you should call [`stop`].
///
/// [`stop`]: ServiceAnnouncer::stop()
#[derive(Debug)]
pub struct ServiceAnnouncer {
    state: Option<State>,
}

#[derive(Debug)]
struct State {
    /// Channel for sending a stop message to the announcer.
    stop_tx: oneshot::Sender<()>,
    /// Channel for sending a drain message to the announcer.
    drain_tx: Option<oneshot::Sender<()>>,

    join_handle: JoinHandle<()>,
}

impl ServiceAnnouncer {
    #[inline]
    pub fn builder<C: Into<Arc<Client>>>(registration: ServiceRegistration, client: C) -> Builder {
        Builder::new(registration, client.into())
    }

    /// Start a new `Announcer` for a service with defaults.
    /// Errors if `ETCD_PEERS` is not set in the environment.
    pub fn start(registration: ServiceRegistration) -> Result<ServiceAnnouncer> {
        Ok(Self::builder(registration, Arc::new(Client::from_etcd_peers()?)).start())
    }

    /// Send a drain signal to the announcement task
    pub fn mark_as_draining(&mut self) {
        if let Some(state) = &mut self.state {
            if let Some(drain_tx) = state.drain_tx.take() {
                info!("Notifying task to set drain state to true.");
                drain_tx.send(()).ok();
            }
        }
    }

    /// Send a shutdown signal to the announcement task,
    /// then returns a future of the task's completion.
    pub fn stop(mut self) -> impl Future<Output = ()> {
        // We specifically don't want this to be an async function
        // because we want the interrupt to happen immediately.
        // An async function would defer the interrupt to the first poll.

        info!("Stopping announcer.");
        let state = self.state.take().unwrap();
        // If this fails then the announcer is already stopped.
        state.stop_tx.send(()).ok();
        state.join_handle.map(|_| ())
    }
}

impl Drop for ServiceAnnouncer {
    fn drop(&mut self) {
        if let Some(State { stop_tx, .. }) = self.state.take() {
            stop_tx.send(()).ok();
        }
    }
}

/// Core loop function that continually announces.
///
/// # Arguments
///
/// * `config` - Announce configuration.
/// * `stop_rx` - receiver that listens for stop messages.
/// * `drain_rx` - receiver that listens for drain messages.
async fn announce(
    mut config: Config,
    stop_rx: oneshot::Receiver<()>,
    drain_rx: oneshot::Receiver<()>,
) {
    let client = &config.client;
    let registration = &mut config.registration;
    let ttl = config.ttl;

    debug!("Starting announce service: {:?}", registration);

    let mut backoff = AsyncBackoff::new(BackoffConfig {
        min_delay: Duration::from_secs(1),
        max_delay: ttl / 2,
        ..Default::default()
    });

    let mut refresh = false;

    let mut stop_rx = stop_rx.fuse();
    let mut drain_rx = drain_rx.fuse();
    loop {
        if refresh {
            match directory::refresh(client, registration, ttl).await {
                Ok(()) => {
                    backoff.succeed();
                }
                // If the refresh request fails, we should retry the register, setting
                // refresh to false.
                Err(e) => {
                    warn!("Failure to refresh. error={}", e);
                    // If we are dealing with a timeout error, we can still try and refresh, we'll just backoff a bit, then attempt to refresh!
                    // We *only* need to re-register if we encounter a "key not found" error.
                    if let Ok(err) = e.downcast::<crate::etcd::EtcdError>() {
                        if matches!(err, crate::etcd::EtcdError::IoError { .. }) {
                            backoff.sleep().await;
                            continue;
                        }
                    }

                    refresh = false;
                    continue;
                }
            }
        } else {
            if !registration.draining {
                if let Some(timeout) = config.wait_for_free_key_timeout {
                    debug!(
                        "Waiting for free key to announce service: {:?}",
                        registration
                    );
                    match directory::wait_for_free_key(client, &registration.key(), timeout).await {
                        Ok(()) => {
                            debug!("Free key found for service: {:?}", registration);
                        }
                        Err(_) => {
                            warn!(
                                "Free key not found within the allotted timeout for service: {:?}",
                                registration
                            );
                            backoff.sleep().await;
                            continue;
                        }
                    }
                }
            }
            debug!("Announcing service: {:?}", registration);
            match directory::register(client, registration, ttl).await {
                Ok(()) => {
                    info!("Announced service: {:?}", registration);
                    refresh = true;
                    backoff.succeed();
                }
                Err(e) => {
                    warn!("Failure to set. error={}", e);
                    backoff.sleep().await;
                }
            }
        }

        let delay = tokio::time::sleep((ttl / 10) * 4).fuse();
        tokio::select! {
            _ = delay => {},
            _ = &mut stop_rx => {
                debug!("Stopping announce service: {:?}", registration);
                for attempt in 0..SHUTDOWN_RETRY_ATTEMPTS {
                    if let Err(e) = directory::unregister(client, registration).await {
                        error!("Uannounce attempt {}: Failed to unregister. {}", attempt, e);
                        backoff.sleep().await;
                    } else {
                        info!("Unannounced service: {:?}", registration);
                        return;
                    }
                }
            }
            _ = &mut drain_rx => {
                registration.draining = true;
                refresh = false;
                debug!("Reanouncing service as draining: {:?}", registration);
            }
        };
    }
}
