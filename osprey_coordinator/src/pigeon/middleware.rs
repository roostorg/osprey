//! This file is mostly a copy of
//! https://github.com/hyperium/tonic/blob/v0.6.2/tonic/src/transport/server/mod.rs#L757-L902

use anyhow::Result;
use http::{Request, Response};
use hyper::Body;
use opentelemetry::trace::TraceContextExt;
use opentelemetry_http::HeaderExtractor;
use pin_project::pin_project;
use std::future::{self, Future};
use std::pin::Pin;
use tonic::body::BoxBody;
use tower::util::BoxService;
use tower::{Service as TowerService, ServiceBuilder};
use tracing::{debug_span, field, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use super::grpc_timeout::GrpcTimeout;
use super::instrumented_connection::InstrumentedConnection;
use super::recover_error::RecoverError;
use super::router::Router;
use super::utils::Error;

// NOTE: When a field is added here, the value must be pre-set also in the the `info_span!(...)`
// declaration within `extract_span`.
static HEADERS_TO_RECORD_AS_SPAN_FIELDS: &[(&str, &str)] = &[
    ("service", "peer_service"),
    ("service-version", "peer_service_version"),
];

// Health gRPC Service Name and Package so we can set them as debug spans
const GRPC_HEALTH_PACKAGE: &str = "grpc.health.v1";
const GRPC_HEALTH_SERVICE: &str = "Health";

/// Factory for [`TracedService`].
pub(crate) struct ServiceFactory<S> {
    pub(crate) router: Router<S>,
}

impl<'a, S> TowerService<&'a InstrumentedConnection> for ServiceFactory<S>
where
    S: TowerService<Request<Body>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Error>,
{
    type Response = BoxService<Request<Body>, Response<BoxErrorBody>, Error>;
    type Error = Error;
    type Future = future::Ready<std::result::Result<Self::Response, Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _socket: &'a InstrumentedConnection) -> Self::Future {
        let svc = self.router.clone();
        let svc = ServiceBuilder::new()
            .layer_fn(RecoverError::new)
            .layer_fn(|s| GrpcTimeout::new(s, None))
            .service(svc);
        let svc = ServiceBuilder::new()
            .layer(BoxService::layer())
            .service(TracedService { inner: svc });
        future::ready(Ok(svc))
    }
}

type BoxErrorBody = http_body::combinators::UnsyncBoxBody<hyper::body::Bytes, Error>;

/// Service that starts a trace span.
#[derive(Clone)]
struct TracedService<S> {
    inner: S,
}

impl<S, ResBody> TowerService<Request<Body>> for TracedService<S>
where
    S: TowerService<Request<Body>, Response = Response<ResBody>>,
    S::Error: Into<Error>,
    ResBody: http_body::Body<Data = hyper::body::Bytes> + Send + 'static,
    ResBody::Error: Into<Error>,
{
    type Response = Response<BoxErrorBody>;
    type Error = Error;
    type Future = TracedServiceFuture<S::Future>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let span = extract_span(&req);

        TracedServiceFuture {
            inner: self.inner.call(req),
            span,
        }
    }
}

#[pin_project]
struct TracedServiceFuture<F> {
    #[pin]
    inner: F,
    span: tracing::Span,
}

impl<F, E, ResBody> Future for TracedServiceFuture<F>
where
    F: Future<Output = Result<Response<ResBody>, E>>,
    E: Into<Error>,
    ResBody: http_body::Body<Data = hyper::body::Bytes> + Send + 'static,
    ResBody::Error: Into<Error>,
{
    type Output = Result<Response<BoxErrorBody>, Error>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use http_body::Body;
        let this = self.project();
        let _guard = this.span.enter();

        let response: Response<ResBody> =
            futures::ready!(this.inner.poll(cx)).map_err(Into::into)?;
        let response = response.map(|body| body.map_err(Into::into).boxed_unsync());
        std::task::Poll::Ready(Ok(response))
    }
}

fn extract_span<B>(request: &Request<B>) -> tracing::Span {
    let headers = request.headers();
    let extractor = HeaderExtractor(headers);
    let parent_cx = opentelemetry::global::get_text_map_propagator(|prop| prop.extract(&extractor));

    let (pkg, svc, method) = parse_path(request.uri().path());

    // `otel.name` overrides the static span name ("grpc.request") we pass below.
    // this is good for otel, but for datadog we still need the primary operation name,
    // which we pass as `dd.operation = "grpc.request"`.
    let otel_name = match (svc, method) {
        (Some(svc), Some(method)) => Some(format!("{svc}/{method}")),
        _ => None,
    };

    let tracing_span = match (pkg, svc) {
        // Set gRPC Health requests to debug level
        (Some(p), Some(s)) if p == GRPC_HEALTH_PACKAGE && s == GRPC_HEALTH_SERVICE => {
            debug_span!(
                "grpc.request",
                otel.name = field::Empty,
                dd.operation = field::Empty,
                peer_service = field::Empty,
                peer_service_version = field::Empty,
                rpc.grpc.path = request.uri().path(),
                rpc.grpc.package = p,
                rpc.service = s,
                rpc.method = method,
            )
        }
        _ => info_span!(
            "grpc.request",
            otel.name = field::Empty,
            dd.operation = field::Empty,
            peer_service = field::Empty,
            peer_service_version = field::Empty,
            rpc.grpc.path = request.uri().path(),
            rpc.grpc.package = pkg,
            rpc.service = svc,
            rpc.method = method,
        ),
    };

    // Note: The use_dd_operation feature was in the tracing_utils crate
    // For now, we'll always record these fields
    tracing_span.record("otel.name", otel_name);
    tracing_span.record("dd.operation", "grpc.request");

    // Only set a parent span if it is "valid", else treat this span as the root.
    // A parent span is "valid" if it contains a trace id and span id.
    // This check fixes an issue where requests directly to a service without span headers were creating
    // "invalid" parent spans, causing the child span to be dropped.
    if parent_cx.span().span_context().is_valid() {
        tracing_span.set_parent(parent_cx);
    }

    for (header_key, field_key) in HEADERS_TO_RECORD_AS_SPAN_FIELDS {
        if let Some(header_value) = headers.get(*header_key) {
            if let Ok(header_value_str) = header_value.to_str() {
                tracing_span.record(*field_key, &header_value_str);
            }
        }
    }

    tracing_span
}

fn str_is_empty(s: &str) -> Option<&str> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

// Parses the gRPC Path and returns the (package, service, method).
fn parse_path(path: &str) -> (Option<&str>, Option<&str>, Option<&str>) {
    let tail = match path.strip_prefix('/') {
        Some(tail) => tail,
        None => return (None, None, None),
    };

    let (service_with_pkg, method) = match tail.split_once('/') {
        Some((a, b)) => (str_is_empty(a), str_is_empty(b)),
        None => return (None, None, None),
    };

    let (pkg, svc) = match service_with_pkg {
        Some(svc_pkg) => match svc_pkg.rsplit_once(".") {
            Some((a, b)) => (str_is_empty(a), str_is_empty(b)),
            // Assume pkg is optional, and just treat entire left side as svc
            None => (None, Some(svc_pkg)),
        },
        None => (None, None),
    };

    (pkg, svc, method)
}
