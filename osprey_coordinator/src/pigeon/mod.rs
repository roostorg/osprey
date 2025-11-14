use crate::discovery::{ServiceAnnouncer, ServiceRegistration};
use crate::etcd::Client as EtcdClient;
use crate::metrics::v2::{MetricsSender, SendMetricsError};
use crate::pigeon::health::{HealthChecker, HealthServer};
use crate::pigeon::pigeon_metrics::MetricsLayer;
use crate::signals::{self, exit_signal};
use anyhow::Result;
use futures::future::{MapErr, Shared};
use futures::stream::{FuturesUnordered, StreamExt};
use futures::{self, FutureExt, TryFutureExt};
use http::{Request, Response};
use hyper::server::conn::AddrIncoming;
use hyper::Body;
use instrumented_connection::InstrumentedIncoming;
use std::future::{self, Future};
use std::iter;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::pin::Pin;
use std::{
    borrow::{Borrow, Cow},
    sync::{atomic::AtomicUsize, Arc},
};
use std::{collections::HashMap, sync::atomic::Ordering};
use std::{env, time::Duration};
use tokio::task::{spawn, JoinError};
use tokio::time::sleep;
use tokio::{sync::oneshot, time::Instant};
use tonic::body::BoxBody;
use tonic_health::pb::health_server::HealthServer as HealthProtoServer;
use tower::util::BoxCloneService;
use tower::{Layer, Service as TowerService, ServiceExt};
use tracing::{debug, info, warn};

pub mod grpc_client;
mod grpc_timeout;
mod health;
mod instrumented_connection;
mod middleware;
mod option_pin;
mod pigeon_metrics;
mod recover_error;
mod router;
mod status;
mod utils;

use self::utils::Error;
use router::Router;

pub use self::utils::Connector;
pub use tonic::server::NamedService;

pub async fn serve<GS>(
    grpc_service: GS,
    service_name: &'static str,
    service_port: u16,
    announce_delay: Duration,
) -> Result<()>
where
    GS: GrpcService + NamedService + Clone + Send + 'static,
    GS::Future: Send + 'static,
    GS::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    Server::new(grpc_service, service_name, service_port)
        .with_standard_registration(
            service_name,
            env::var("POD_IP").expect("`POD_IP needs to be set").into(),
        )
        .with_encoded_file_descriptor_set(crate::proto::PB_DESCRIPTOR_BYTES)
        .with_announce_delay(Some(announce_delay))
        .with_shutdown(signals::exit_signal())
        .serve()
        .await
}

/// The TCP keep-alive ping interval for the GRPC servers that pigeon runs.
const TCP_KEEPALIVE_DURATION: Duration = Duration::from_secs(300);

/// Copy of
/// [`tonic_reflection::proto::server_reflection_server::ServerReflectionServer::NAME`]
/// for visibility reasons.
const REFLECTION_SERVICE_NAME: &str = "grpc.reflection.v1alpha.ServerReflection";

pub trait GrpcService
where
    Self: TowerService<Request<Body>, Response = Response<BoxBody>>,
    Self::Future: Send + 'static,
    Self::Error: Into<Error> + Send,
{
}

impl<TS> GrpcService for TS
where
    TS: TowerService<Request<Body>, Response = Response<BoxBody>>,
    TS::Future: Send + 'static,
    TS::Error: Into<Error> + Send,
{
}

/// Defines the configuration of how the server will shut down after receiving an exit signal.
/// Set using [`Server::with_shutdown_config`].
pub enum ShutdownConfig {
    /// If requests have not stopped in `max_delay`, then the server will shut down anyways.
    WaitForRequestsToStop {
        /// The maximum amount of time to wait for requests to stop.
        max_delay: Duration,
        /// The amount of time to wait for no requests to be received before shutting down.
        acquiesce_duration: Duration,
    },
}

struct DiscoveryConfig {
    /// The delay to start announcing after the GRPC service is started.
    announce_delay: Option<Duration>,
    /// The delay to deregister from service discovery after receiving an exit signal.
    deregister_delay: Option<Duration>,
    /// The optional etcd-client struct for handling registration
    etcd_client: Option<EtcdClient>,
    /// Whether to wait for an existing key to be freed. Defaults to `false`. Timeout is required.
    wait_for_free_key_timeout: Option<Duration>,
    /// Service information for registering in discovery.
    registrations: Vec<ServiceRegistration>,
}

/// A Server that handles the common logic needed to play well with
/// other services in our ecosystem.
pub struct Server {
    address: SocketAddr,
    /// If true, will not execute announce_delay or shutdown_config. Defaults to false.
    debug: bool,

    /// We gate all of our etcd discovery based config behind this structure to make the code cleaner
    discovery_config: DiscoveryConfig,

    /// Configuration defining how to shut down the server after receiving an exit signal. Ignored if debug is true.
    shutdown_config: ShutdownConfig,
    /// Metrics prefix.
    metrics_prefix: String,

    /// gRPC services that do business logic.
    grpc_services: HashMap<&'static str, (BoxGrpcService, Vec<Box<dyn HealthChecker>>)>,
    /// Protobuf reflection data.
    /// If empty, don't serve reflection.
    encoded_file_descriptor_sets: Vec<Cow<'static, [u8]>>,

    /// A signal to shutdown on.
    shutdown: Pin<Box<dyn Future<Output = ()> + Send>>,

    /// A callback to be called synchronously after the server has received
    /// an interrupt signal but BEFORE being deregistered from service discovery
    pre_unannounce_callback:
        Option<Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>>,

    allow_http1_requests: bool,

    /// Configures the maximum number of pending reset streams allowed before a GOAWAY will be sent.
    /// This is currently being used for testing and is not recommended to be set in production.
    /// See https://github.com/hyperium/h2/pull/668 for more details.
    http2_max_pending_accept_reset_streams: Option<usize>,
}

impl Server {
    /// Create a server for the given gRPC server stub.
    /// The returned `Server` will not announce to etcd
    /// unless [`with_registration`] or [`with_standard_registration`] are called.
    pub fn new<GS>(service: GS, metrics_prefix: impl Into<String>, port: u16) -> Server
    where
        GS: GrpcService + NamedService + Clone + Send + 'static,
        GS::Future: Send + 'static,
        GS::Error: Into<Error> + Send,
    {
        let address = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), port);

        let mut grpc_services = HashMap::new();
        grpc_services.insert(GS::NAME, (box_service(service), Vec::new()));

        let discovery_config = DiscoveryConfig {
            announce_delay: Some(Duration::from_secs(5)),
            deregister_delay: None,
            etcd_client: None,
            registrations: Vec::new(),
            wait_for_free_key_timeout: None,
        };

        Self {
            address,
            debug: false,
            shutdown_config: ShutdownConfig::WaitForRequestsToStop {
                max_delay: Duration::from_secs(300),
                acquiesce_duration: Duration::from_secs(15),
            },
            discovery_config,
            metrics_prefix: metrics_prefix.into(),
            grpc_services,
            encoded_file_descriptor_sets: Vec::new(),
            shutdown: Box::pin(future::pending()),
            pre_unannounce_callback: None,
            allow_http1_requests: false,
            http2_max_pending_accept_reset_streams: None,
        }
    }

    /// Adds an standard etcd service registration to announce while serving.
    pub fn with_standard_registration(
        self,
        registration_name: impl Into<String>,
        hostname: Option<impl AsRef<str>>,
    ) -> Self {
        let hostname = match &hostname {
            Some(hostname) => hostname.as_ref().trim().to_string(),
            None => hostname::get()
                .map(|os| os.to_string_lossy().into_owned())
                .unwrap_or_else(|_| "localhost".to_string()),
        };

        let registration =
            ServiceRegistration::builder(registration_name.into(), self.address.port())
                .with_grpc_port(self.address.port())
                .with_address_override(hostname)
                .build();

        self.with_registration(registration)
    }

    /// Adds an etcd service registration to announce while serving.
    pub fn with_registration(mut self, service: ServiceRegistration) -> Self {
        self.discovery_config.registrations.push(service);
        self
    }

    /// Sets a signal that the service should shutdown on. Defaults to `None`.
    pub fn with_shutdown<NS>(mut self, shutdown: NS) -> Self
    where
        NS: Future<Output = ()> + Send + 'static,
    {
        self.shutdown = Box::pin(shutdown);
        self
    }

    /// Sets the amount of time between grpc service start up and when the announcer
    /// starts. Use this setting if your service has a warm up.
    /// Defaults to 5 seconds.
    pub fn with_announce_delay(mut self, announce_delay: Option<Duration>) -> Self {
        self.discovery_config.announce_delay = announce_delay;
        self
    }

    /// Adds a Protocol Buffer file descriptor set (reflection information) to be served.
    /// Calling this at least once will enable [gRPC Server Reflection].
    ///
    /// [gRPC Server Reflection]: https://github.com/grpc/grpc/blob/master/doc/server-reflection.md
    pub fn with_encoded_file_descriptor_set(
        mut self,
        fd_set: impl Into<Cow<'static, [u8]>>,
    ) -> Self {
        self.encoded_file_descriptor_sets.push(fd_set.into());
        self
    }

    /// Starts the server and runs until the service stops or an interrupt
    /// signal is received.
    ///
    /// This will do the following:
    /// 1. Handle registering/deregistring the service with Discovery.
    /// 2. Register metrics for the GRPC service.
    /// 3. Start the GRPC service.
    /// 4. Listen for a stop signal, including ctrl+c and other system interrupts.
    /// 5. Wait for the GRPC service to gracefully stop
    pub async fn serve(self) -> Result<()> {
        self.serve_inner().await
    }

    async fn serve_inner(self) -> Result<()> {
        let Server {
            address,
            debug,
            shutdown_config,
            metrics_prefix,
            discovery_config,
            grpc_services,
            shutdown,
            pre_unannounce_callback,
            encoded_file_descriptor_sets,
            allow_http1_requests,
            http2_max_pending_accept_reset_streams,
        } = self;
        info!("Starting service on {}", address);

        // Start sending metrics if the caller has not started doing so.
        // The thread will be shut down when the future is resolved or dropped.
        let sender_result = MetricsSender::builder().start();

        let request_counter = Arc::new(AtomicUsize::new(0));

        let _sender = match sender_result {
            Ok(sender) => {
                debug!("Started sending statsd metrics");
                Some(sender)
            }
            Err(SendMetricsError::AlreadySending) => None,
            Err(err) => return Err(err.into()),
        };
        let (grpc_services, health_checks): (HashMap<_, _>, HashMap<_, _>) = grpc_services
            .into_iter()
            .map(|(k, (srv, checks))| ((k, srv), (Cow::Borrowed(k), checks)))
            .unzip();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let (health_server, health_shutdown_tx) = HealthServer::new(health_checks);

        let request_counter_for_server = request_counter.clone();
        let error_nullifier: ErrorNullifier = |_| ();
        let join_handle = spawn(async move {
            let result = listen_and_serve(
                &address,
                &metrics_prefix,
                grpc_services,
                HealthProtoServer::new(health_server),
                encoded_file_descriptor_sets,
                shutdown_rx,
                allow_http1_requests,
                http2_max_pending_accept_reset_streams,
                request_counter_for_server,
            )
            .await;
            warn!("GRPC server ({:?}) stopped serving. {:?}", address, result);
        })
        .map_err(error_nullifier)
        .shared(); // Make join_handle sharable, as we will try to await multiple times.
        let grpc_handle = GrpcHandle::builder(
            shutdown_tx,
            join_handle.clone(),
            request_counter,
            shutdown_config,
        );

        let announcers: Vec<ServiceAnnouncer> = {
            if !debug {
                if let Some(announce_delay) = discovery_config.announce_delay {
                    // Give the service time to warm up before announcing.
                    sleep(announce_delay).await;
                }
            }
            if discovery_config.registrations.is_empty() {
                Vec::new()
            } else {
                let etcd_client = discovery_config
                    .etcd_client
                    .map(Result::Ok)
                    .unwrap_or_else(|| EtcdClient::from_etcd_peers())?;
                discovery_config
                    .registrations
                    .into_iter()
                    .map(|service| {
                        ServiceAnnouncer::builder(service, etcd_client.clone())
                            .wait_for_free_key(discovery_config.wait_for_free_key_timeout)
                            .start()
                    })
                    .collect()
            }
        };

        let exit_signal = exit_signal();

        tokio::select! {
            _ = shutdown => warn!("Shutdown received."),
            _ = join_handle.clone() => warn!("Down received."),
            _ = exit_signal => warn!("Graceful exit signal received."),
        };

        if !debug {
            if let Some(deregister_delay) = discovery_config.deregister_delay {
                // Give service time to drain while still being discoverable.
                sleep(deregister_delay).await;
            }
        }

        info!("Stopping services.");
        health_shutdown_tx.notify();

        // Execute pre_unannounce_callback if it exists before the service is de-registered (or marked as draining)
        // from service discovery.
        if let Some(callback) = pre_unannounce_callback {
            (callback)().await;
        }
        let announcer_stops: FuturesUnordered<_> =
            announcers.into_iter().map(|a| a.stop()).collect();
        announcer_stops.for_each(|_| future::ready(())).await;
        if !debug {
            grpc_handle.shutdown().await?;
        } else {
            grpc_handle.stop().await?;
        }
        info!("Services stopped.");
        Ok(())
    }
}

async fn listen_and_serve<'a, THealthServer>(
    address: &'a SocketAddr,
    metrics_prefix: &'a str,
    grpc_services: HashMap<&'static str, BoxGrpcService>,
    health_server: THealthServer,
    encoded_file_descriptor_sets: Vec<Cow<'static, [u8]>>,
    shutdown_rx: oneshot::Receiver<()>,
    allow_http1_requests: bool,
    http2_max_pending_accept_reset_streams: Option<usize>,
    request_counter: Arc<AtomicUsize>,
) -> Result<()>
where
    THealthServer: GrpcService + NamedService + Clone + Send + 'static,
    THealthServer::Future: Send + 'static,
    THealthServer::Error: Into<Error> + Send,
{
    let reflection_service = if encoded_file_descriptor_sets.is_empty() {
        None
    } else {
        let mut reflection_builder = tonic_reflection::server::Builder::configure();
        for encoded_fd_set in &encoded_file_descriptor_sets {
            reflection_builder =
                reflection_builder.register_encoded_file_descriptor_set(encoded_fd_set.borrow());
        }
        Some(reflection_builder.build()?)
    };

    let service_map: HashMap<&'static str, BoxGrpcService> = grpc_services
        .into_iter()
        .map(|(name, grpc_service)| {
            (
                name,
                box_service(
                    MetricsLayer::server(metrics_prefix)
                        .with_request_counter(request_counter.clone())
                        .layer(grpc_service)
                        .map_response(|resp| resp.map(BoxBody::new)),
                ),
            )
        })
        .chain(iter::once((
            THealthServer::NAME,
            box_service(
                MetricsLayer::server(metrics_prefix)
                    .layer(health_server)
                    .map_response(|resp| resp.map(BoxBody::new)),
            ),
        )))
        .chain(
            reflection_service
                .map(|s| (REFLECTION_SERVICE_NAME, box_service(s)))
                .into_iter(),
        )
        .collect();

    let mut incoming = AddrIncoming::bind(address)?;
    incoming
        .set_keepalive(Some(TCP_KEEPALIVE_DURATION))
        .set_nodelay(true);
    let server = hyper::Server::builder(InstrumentedIncoming::new(incoming))
        .http2_only(!allow_http1_requests)
        .http2_max_pending_accept_reset_streams(http2_max_pending_accept_reset_streams);

    server
        .serve(middleware::ServiceFactory {
            router: Router {
                services: service_map,
            },
        })
        .with_graceful_shutdown(shutdown_rx.map(|_| ()))
        .await?;

    Ok(())
}

type BoxGrpcService = BoxCloneService<Request<Body>, Response<BoxBody>, Error>;

fn box_service<S, E>(inner: S) -> BoxGrpcService
where
    S: TowerService<Request<Body>, Response = Response<BoxBody>, Error = E>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    E: Into<Error> + 'static,
{
    BoxCloneService::new(inner.map_err(Into::into))
}

type ErrorNullifier = fn(JoinError) -> ();
type SharedErrorNullifiedJoinHandle = Shared<MapErr<tokio::task::JoinHandle<()>, ErrorNullifier>>;

struct GrpcHandleInner {
    shutdown_tx: oneshot::Sender<()>,
    join_handle: SharedErrorNullifiedJoinHandle,
}

/// A [`GrpcHandle`] can stop a GRPC service when dropped or when [`GrpcHandle::stop`] is called.
///
/// It can also see the number of requests that have been received by the GRPC service during it's
/// lifetime by calling [`GrpcHandle::get_request_count`].
pub struct GrpcHandle {
    inner: Option<GrpcHandleInner>,
    request_counter: Arc<AtomicUsize>,
    shutdown_config: ShutdownConfig,
}

impl GrpcHandle {
    pub fn builder(
        shutdown_tx: oneshot::Sender<()>,
        join_handle: SharedErrorNullifiedJoinHandle,
        request_counter: Arc<AtomicUsize>,
        shutdown_config: ShutdownConfig,
    ) -> GrpcHandle {
        Self {
            inner: Some(GrpcHandleInner {
                shutdown_tx,
                join_handle,
            }),
            request_counter,
            shutdown_config,
        }
    }

    /// Gets the number of requests that have been received by the GRPC service.
    pub fn request_count(&self) -> usize {
        self.request_counter.load(Ordering::Relaxed)
    }

    /// Stops the GRPC service in accordance to the [`ShutdownConfig`] supplied to the [`Server`] builder.
    pub async fn shutdown(self) -> Result<()> {
        match self.shutdown_config {
            ShutdownConfig::WaitForRequestsToStop {
                max_delay,
                acquiesce_duration,
            } => {
                // Future that waits for requests to stop, returning the time it took for requests to stop.
                let stopped_future = async {
                    let mut prev_request_count = self.request_count();
                    let start_time = Instant::now();

                    // This tracks the time at when we last observed an incoming request.
                    let mut acequiesce_start = start_time;

                    // Poll the request counter, detecting a period where there are no requests
                    // for at least `acquiesce_duration`.
                    loop {
                        sleep(Duration::from_millis(100)).await;

                        let current_request_count = self.request_count();
                        if prev_request_count != current_request_count {
                            prev_request_count = current_request_count;
                            acequiesce_start = Instant::now();
                        } else {
                            if acequiesce_start.elapsed() > acquiesce_duration {
                                return start_time.elapsed();
                            }
                        }
                    }
                };

                info!("Waiting for requests to stop, will wait for no requests to be received for {:?}, or at most {:?} before stopping.", acquiesce_duration, max_delay);
                tokio::select! {
                    elapsed = stopped_future => {
                        info!("Requests have stopped in {:?}. Shutting down.", elapsed);
                    },
                    _ = sleep(max_delay) => {
                        info!("Requests have not acquiesced in {:?}. Shutting down anyways.", max_delay);
                    },
                }
            }
        };
        self.stop().await
    }

    /// Immediately stops the GRPC service and waits for it to finish. This function ignores the [`ShutdownConfig`].
    pub async fn stop(mut self) -> Result<()> {
        let inner = self.inner.take().expect("invariant: missing inner");
        inner.shutdown_tx.send(()).ok();
        let _ = inner.join_handle.await;
        Ok(())
    }
}

impl Drop for GrpcHandle {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.shutdown_tx.send(()).ok();
        }
    }
}
