use crate::metrics::string_interner::{InternResult, StringInterner, Transformed};
use crate::metrics::v2::{BaseMetric, DurationResolution};
use crate::metrics::{MetricsEmitter, TagKey};
use futures::ready;
use heck::SnakeCase;
use http::{HeaderMap, Request, Response};
use http_body::Body;
use lazy_static::lazy_static;
use pin_project::pin_project;
use std::collections::{hash_map::Entry, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;
use std::{borrow::Cow, sync::atomic::AtomicUsize};
use tonic::{Code, Status};
use tower::{Layer, Service};
use tracing::error;

use super::utils::parse_path;

pub(crate) const SERVICE_METADATA_KEY: &str = "service";

const ERROR_TYPE_CLIENT: &str = "client";
const ERROR_TYPE_SERVER: &str = "server";

/// Metrics that are prefixed by the running service's name.
#[derive(Debug)]
pub(crate) struct LegacyPigeonMetrics {
    server_rpc_response_time_name: String,
    server_rpc_response_time: crate::metrics::v2::DurationHistogram<LegacyServerRpcResponseTimeKey>,
}

impl LegacyPigeonMetrics {
    fn new(prefix: impl AsRef<str>) -> Self {
        LegacyPigeonMetrics {
            server_rpc_response_time_name: format!("{}.rpc.response_time", prefix.as_ref()),
            server_rpc_response_time: crate::metrics::v2::DurationHistogram::default(),
        }
    }
}

impl crate::metrics::v2::MetricsEmitter for LegacyPigeonMetrics {
    fn emit_metrics(
        &self,
        client: &crate::metrics::v2::macro_support::StatsdClient,
        base_tags: &mut dyn Iterator<Item = (&str, &str)>,
    ) -> Result<(), crate::metrics::v2::macro_support::MetricError> {
        self.server_rpc_response_time.send_metric(
            client,
            &self.server_rpc_response_time_name,
            base_tags,
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, TagKey, PartialEq, Eq, Hash)]
struct LegacyServerRpcResponseTimeKey {
    service: &'static str,
    method: InternResult,
    #[tag(name = "peer.service")]
    peer_service: InternResult,
}

#[derive(Debug, MetricsEmitter)]
pub(crate) struct PigeonMetrics {
    #[metric(name = "pigeon.rpc.count")]
    server_rpc_count: crate::metrics::v2::Counter<RpcTags<InternResult>>,
    #[metric(name = "pigeon.rpc.error_count")]
    server_rpc_error_count: crate::metrics::v2::Counter<RpcErrorTags<InternResult>>,
    #[metric(name = "pigeon.rpc.trailer_response_time")]
    server_rpc_trailer_response_time: crate::metrics::v2::DurationHistogram<RpcTags<InternResult>>,
    #[metric(name = "pigeon.rpc.response_time")]
    server_rpc_response_time: crate::metrics::v2::DurationHistogram<RpcTags<InternResult>>,
    /// The number of currently active server requests
    #[metric(name = "pigeon.rpc.active")]
    server_rpc_active: crate::metrics::v2::Gauge<RpcTags<InternResult>, f64>,

    #[metric(name = "pigeon.client_rpc.count")]
    client_rpc_count: crate::metrics::v2::Counter<RpcTags<&'static str>>,
    #[metric(name = "pigeon.client_rpc.error_count")]
    client_rpc_error_count: crate::metrics::v2::Counter<RpcErrorTags<&'static str>>,
    #[metric(name = "pigeon.client_rpc.response_time")]
    client_rpc_response_time: crate::metrics::v2::DurationHistogram<RpcTags<&'static str>>,
    /// The number of currently active client requests
    #[metric(name = "pigeon.rpc.active")]
    client_rpc_active: crate::metrics::v2::Gauge<RpcTags<&'static str>, f64>,
}

impl Default for PigeonMetrics {
    fn default() -> Self {
        PigeonMetrics {
            server_rpc_count: Default::default(),
            server_rpc_error_count: Default::default(),
            server_rpc_response_time: crate::metrics::v2::DurationHistogram::new(
                DurationResolution::Microseconds,
            ),
            server_rpc_trailer_response_time: crate::metrics::v2::DurationHistogram::new(
                DurationResolution::Microseconds,
            ),
            server_rpc_active: Default::default(),
            client_rpc_count: Default::default(),
            client_rpc_error_count: Default::default(),
            client_rpc_response_time: crate::metrics::v2::DurationHistogram::new(
                DurationResolution::Microseconds,
            ),
            client_rpc_active: Default::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, TagKey)]
pub(crate) struct RpcErrorTags<PeerService> {
    #[tag(name = "grpc.service")]
    service: &'static str,
    #[tag(name = "grpc.method")]
    method: &'static str,
    #[tag(name = "grpc.code")]
    code: Code,
    #[tag(name = "peer.service")]
    peer_service: PeerService,
    #[tag(name = "type")]
    error_type: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, TagKey)]
pub(crate) struct RpcTags<PeerService> {
    #[tag(name = "grpc.service")]
    service: &'static str,
    #[tag(name = "grpc.method")]
    method: &'static str,
    #[tag(name = "peer.service")]
    peer_service: PeerService,
}

lazy_static! {
    static ref METRICS: Arc<PigeonMetrics> = {
        let m = Arc::new(PigeonMetrics::default());
        crate::metrics::v2::register_emitter(m.clone()).emit_forever();
        m
    };
    static ref LEGACY_METRICS_MAP: Arc<Mutex<HashMap<String, Arc<LegacyPigeonMetrics>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    static ref PEER_INTERNER: StringInterner = StringInterner::new(512);
    static ref SERVICE_INTERNER: StringInterner = StringInterner::new(1024);
    static ref METHOD_INTERNER: StringInterner<Transformed> =
        StringInterner::with_transformer(1024, SnakeCase::to_snake_case);
}

/// Gets or creates the metrics with the given prefix.
/// Will also register the metrics with the MetricsSender
/// the first time metrics with a particular prefix are requested.
pub(crate) fn get_legacy_metrics_and_ensure_registered<'a>(
    prefix: impl Into<Cow<'a, str>>,
) -> anyhow::Result<Arc<LegacyPigeonMetrics>> {
    let prefix = prefix.into();

    let mut map = LEGACY_METRICS_MAP.lock().unwrap();
    match map.entry(prefix.into_owned()) {
        Entry::Vacant(ent) => {
            // First thread to create metrics registers it.
            let pigeon_metrics = Arc::new(LegacyPigeonMetrics::new(ent.key()));
            crate::metrics::v2::register_emitter(pigeon_metrics.clone()).emit_forever();
            ent.insert(pigeon_metrics.clone());
            Ok(pigeon_metrics)
        }
        // Another thread created the metrics first.
        Entry::Occupied(ent) => Ok(ent.get().clone()),
    }
}

/// A [`tower::Layer`] that decorates a gRPC server or client channel
/// by instrumenting it with metrics.
#[derive(Clone, Debug)]
pub struct MetricsLayer {
    v1_enabled: bool,
    metrics: Arc<PigeonMetrics>,
    role: MetricsServiceRole,
    request_counter: Option<Arc<AtomicUsize>>,
}

impl MetricsLayer {
    /// Creates a new server layer with the given metrics prefix.
    /// This is typically the name of the running service (e.g. `"osprey_foo"`).
    pub fn server<'a>(prefix: impl Into<Cow<'a, str>>) -> Self {
        let prefix: &'static str = Box::leak(prefix.into().into_owned().into_boxed_str());
        MetricsLayer {
            v1_enabled: true,
            metrics: METRICS.clone(),
            role: MetricsServiceRole::Server {
                legacy_metrics: get_legacy_metrics_and_ensure_registered(prefix).unwrap(),
            },
            request_counter: None,
        }
    }

    /// Creates a new client layer
    /// with the name of the running service (e.g. `"osprey_foo"`).
    pub fn client<'a>(peer_service: impl Into<&'static str>) -> Self {
        MetricsLayer {
            v1_enabled: true,
            metrics: METRICS.clone(),
            role: MetricsServiceRole::Client {
                peer_service: peer_service.into(),
            },
            request_counter: None,
        }
    }

    /// Stop emitting v1 prefixed metrics.
    pub fn disable_metrics_v1(mut self) -> Self {
        self.v1_enabled = false;
        self
    }

    /// Attaches a request counter to the metrics layer, which will be incremented each time
    /// a request is served.
    pub fn with_request_counter(self, request_counter: Arc<AtomicUsize>) -> Self {
        MetricsLayer {
            request_counter: Some(request_counter),
            ..self
        }
    }
}

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            v1_enabled: self.v1_enabled,
            metrics: self.metrics.clone(),
            role: self.role.clone(),
            request_counter: self.request_counter.clone(),
        }
    }
}

/// Instruments an underlying service with metrics.
#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
    v1_enabled: bool,
    metrics: Arc<PigeonMetrics>,
    role: MetricsServiceRole,
    request_counter: Option<Arc<AtomicUsize>>,
}

impl<S, ReqBody, RespBody> Service<Request<ReqBody>> for MetricsService<S>
where
    S: Service<Request<ReqBody>, Response = Response<RespBody>>,
    S::Future: Send + 'static,
    RespBody: Body,
{
    type Response = Response<MetricsServiceBody<RespBody>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let start = Instant::now();
        let peer_service = request
            .headers()
            .get(SERVICE_METADATA_KEY)
            .and_then(|v| v.to_str().ok())
            .map(|s| (*PEER_INTERNER).intern(s))
            .unwrap_or_else(|| InternResult::Interned("n/a"));
        let parsed_path = parse_path(request.uri().path()).and_then(intern_path);
        if let Some((service, method)) = parsed_path {
            match self.role {
                MetricsServiceRole::Server { .. } => {
                    self.metrics.server_rpc_count.incr(RpcTags {
                        service,
                        method,
                        peer_service,
                    });
                    self.metrics.server_rpc_active.incr(
                        RpcTags {
                            service,
                            method,
                            peer_service,
                        },
                        1.0,
                    );
                }
                MetricsServiceRole::Client { peer_service } => {
                    self.metrics.client_rpc_count.incr(RpcTags {
                        service,
                        method,
                        peer_service,
                    });
                    self.metrics.client_rpc_active.incr(
                        RpcTags {
                            service,
                            method,
                            peer_service,
                        },
                        1.0,
                    );
                }
            }
        }
        let metrics = self.metrics.clone();
        if let Some(request_counter) = &self.request_counter {
            request_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        let future = self.inner.call(request);
        let role = self.role.clone();
        let v1_enabled = self.v1_enabled;
        Box::pin(async move {
            let result = future.await;
            if let Some((service, method)) = parsed_path {
                // When the future completes, that indicates the server handler has finished.
                match role {
                    // On server-side, we want to record this as the response time,
                    // since the subsequent body will be dependent on the client and network conditions.
                    MetricsServiceRole::Server { .. } => {
                        metrics.server_rpc_response_time.record(
                            RpcTags {
                                service,
                                method,
                                peer_service,
                            },
                            Instant::now().duration_since(start),
                        );
                        metrics.server_rpc_active.decr(
                            RpcTags {
                                service,
                                method,
                                peer_service,
                            },
                            1.0,
                        );
                    }
                    // On the client-side, we're not terribly interested in this timing.
                    // We won't return control flow to the application
                    // until we receive the trailers with the status,
                    // so the end of trailers may as well be the total response time.
                    // We will decrement the active gauge, though.
                    MetricsServiceRole::Client { peer_service } => {
                        metrics.client_rpc_active.decr(
                            RpcTags {
                                service,
                                method,
                                peer_service,
                            },
                            1.0,
                        );
                    }
                }
            }

            match result {
                Ok(response) => {
                    let header_status_code = Status::from_header_map(response.headers())
                        .as_ref()
                        .map(Status::code);
                    // gRPC status for responses with a body is in the Trailers,
                    // so we wrap the inner service's returned body
                    // to increment the metrics after the trailers have been read.
                    Ok(response.map(|inner| MetricsServiceBody {
                        inner,
                        state: MetricsBodyState {
                            header_status_code,
                            pending_metrics: parsed_path.map(|(service, method)| PendingMetrics {
                                v1_enabled,
                                start,
                                service,
                                method,
                                peer_service,
                                metrics,
                                role,
                            }),
                        },
                    }))
                }
                Err(err) => {
                    if let Some((service, method)) = parsed_path {
                        match role {
                            MetricsServiceRole::Server { .. } => {
                                metrics.server_rpc_error_count.incr(RpcErrorTags {
                                    service,
                                    method,
                                    code: Code::Internal,
                                    error_type: ERROR_TYPE_SERVER,
                                    peer_service,
                                });
                            }
                            MetricsServiceRole::Client { peer_service } => {
                                // Network error prevented reading the headers.
                                // gRPC canonically treats this as UNAVAILABLE.
                                metrics.client_rpc_error_count.incr(RpcErrorTags {
                                    service,
                                    method,
                                    code: Code::Unavailable,
                                    error_type: ERROR_TYPE_SERVER,
                                    peer_service,
                                });
                            }
                        }
                    }
                    Err(err)
                }
            }
        })
    }
}

fn intern_path((service, method): (&str, &str)) -> Option<(&'static str, &'static str)> {
    let service = match (*SERVICE_INTERNER).intern(service) {
        InternResult::Interned(s) => s,
        InternResult::AtCapacity => {
            error!(
                "Could not add metric for /{}/{}: service intern table at capacity",
                service, method,
            );
            return None;
        }
    };
    let method = match (*METHOD_INTERNER).intern(method) {
        InternResult::Interned(s) => s,
        InternResult::AtCapacity => {
            error!(
                "Could not add metric for /{}/{}: method intern table at capacity",
                service, method,
            );
            return None;
        }
    };
    Some((service, method))
}

#[derive(Debug)]
struct MetricsBodyState {
    header_status_code: Option<Code>,
    pending_metrics: Option<PendingMetrics>,
}

impl MetricsBodyState {
    fn record(&mut self, status_code: Code, end: Instant) {
        if let Some(metrics) = self.pending_metrics.take() {
            metrics.record(status_code, end);
        }
    }
}

impl Drop for MetricsBodyState {
    /// Ensures that the metrics are recorded,
    /// using the time that the state is dropped as the RPC end time.
    ///
    /// gRPC error responses do not contain a body
    /// so Tonic never bothers to poll the body.
    /// Implementing `Drop` guarantees that the metrics will be recorded.
    fn drop(&mut self) {
        if let Some(metrics) = self.pending_metrics.take() {
            let code = self.header_status_code.unwrap_or(Code::Unknown);
            metrics.record(code, Instant::now());
        }
    }
}

#[derive(Clone, Debug)]
enum MetricsServiceRole {
    Server {
        legacy_metrics: Arc<LegacyPigeonMetrics>,
    },
    Client {
        peer_service: &'static str,
    },
}

#[derive(Debug)]
struct PendingMetrics {
    start: Instant,
    service: &'static str,
    method: &'static str,
    peer_service: InternResult,
    v1_enabled: bool,
    metrics: Arc<PigeonMetrics>,
    role: MetricsServiceRole,
}

impl PendingMetrics {
    fn record(self, status_code: Code, trailer_end: Instant) {
        match &self.role {
            // Disable legacy metrics if asked
            MetricsServiceRole::Server { .. } if !self.v1_enabled => {}
            MetricsServiceRole::Server { legacy_metrics } => {
                let trailer_response_time = trailer_end.duration_since(self.start);

                legacy_metrics.server_rpc_response_time.record(
                    LegacyServerRpcResponseTimeKey {
                        service: self.service,
                        method: (*METHOD_INTERNER).intern(self.method),
                        peer_service: self.peer_service,
                    },
                    trailer_response_time,
                );

                self.metrics.server_rpc_trailer_response_time.record(
                    RpcTags {
                        service: self.service,
                        method: self.method,
                        peer_service: self.peer_service,
                    },
                    trailer_response_time,
                );
            }
            MetricsServiceRole::Client { peer_service } => {
                self.metrics.client_rpc_response_time.record(
                    RpcTags {
                        service: self.service,
                        method: self.method,
                        peer_service,
                    },
                    trailer_end.duration_since(self.start),
                );
            }
        }
        match status_code {
            Code::Ok => {}
            Code::Cancelled
            | Code::InvalidArgument
            | Code::NotFound
            | Code::AlreadyExists
            | Code::PermissionDenied
            | Code::FailedPrecondition
            | Code::Aborted
            | Code::OutOfRange
            | Code::Unauthenticated => match self.role {
                MetricsServiceRole::Server { .. } => {
                    self.metrics.server_rpc_error_count.incr(RpcErrorTags {
                        service: self.service,
                        method: self.method,
                        code: status_code,
                        error_type: ERROR_TYPE_CLIENT,
                        peer_service: self.peer_service,
                    });
                }
                MetricsServiceRole::Client { peer_service } => {
                    self.metrics.client_rpc_error_count.incr(RpcErrorTags {
                        service: self.service,
                        method: self.method,
                        code: status_code,
                        error_type: ERROR_TYPE_CLIENT,
                        peer_service,
                    });
                }
            },
            _ => match &self.role {
                MetricsServiceRole::Server { .. } => {
                    self.metrics.server_rpc_error_count.incr(RpcErrorTags {
                        service: self.service,
                        method: self.method,
                        code: status_code,
                        error_type: ERROR_TYPE_SERVER,
                        peer_service: self.peer_service,
                    });
                }
                MetricsServiceRole::Client { peer_service } => {
                    self.metrics.client_rpc_error_count.incr(RpcErrorTags {
                        service: self.service,
                        method: self.method,
                        code: status_code,
                        error_type: ERROR_TYPE_SERVER,
                        peer_service,
                    });
                }
            },
        }
    }
}

/// Body of a [`ClientService`] response.
#[pin_project]
pub struct MetricsServiceBody<B> {
    #[pin]
    inner: B,
    state: MetricsBodyState,
}

impl<B: Body> Body for MetricsServiceBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_data(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        let this = self.project();
        let result = ready!(this.inner.poll_data(cx));
        if let Some(Err(_)) = &result {
            this.state.record(Code::Unavailable, Instant::now());
        }
        Poll::Ready(result)
    }

    fn poll_trailers(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<HeaderMap>, Self::Error>> {
        let this = self.project();
        let result = ready!(this.inner.poll_trailers(cx));
        let end = Instant::now();
        let status_code = match &result {
            Ok(Some(trailer)) => Status::from_header_map(trailer)
                .as_ref()
                .map(Status::code)
                .or(this.state.header_status_code),
            Ok(None) => this.state.header_status_code,
            Err(_) => Some(Code::Unavailable),
        }
        .unwrap_or(Code::Unknown);
        this.state.record(status_code, end);
        Poll::Ready(result)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}
