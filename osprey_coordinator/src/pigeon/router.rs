use super::status::GRPC_STATUS_HEADER_CODE;
use super::utils::{parse_path, Error};
use futures::{future::BoxFuture, TryFutureExt};
use http::{Request, Response, StatusCode};
use hyper::Body;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::future;
use tonic::body::BoxBody;
use tower::Service as TowerService;

/// Dispatcher for the [`Server`](super::Server).
#[derive(Clone)]
pub(crate) struct Router<S> {
    pub(crate) services: HashMap<&'static str, S>,
}

impl<S> TowerService<Request<Body>> for Router<S>
where
    S: TowerService<Request<Body>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send + 'static,
{
    type Response = Response<BoxBody>;
    type Error = Error;
    type Future = BoxFuture<'static, std::result::Result<Response<BoxBody>, Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let maybe_service =
            parse_path(req.uri().path()).and_then(|(name, _)| self.services.get(name));
        match maybe_service {
            // TODO: Technically, this should call poll_ready before calling the service,
            Some(service) => Box::pin(service.clone().call(req).map_err(Into::into)),
            None => Box::pin(future::ready(Ok(unimplemented_response()))),
        }
    }
}

impl<S> Debug for Router<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("Router { services: {")?;
        let mut keys: Vec<_> = self.services.keys().copied().collect();
        keys.sort_unstable();
        for (i, key) in keys.into_iter().enumerate() {
            write!(f, "{} {}: Service", if i == 0 { "" } else { "," }, key)?;
        }
        f.write_str("} }")?;
        Ok(())
    }
}

fn unimplemented_response() -> Response<BoxBody> {
    // Copied from tonic's Unimplemented
    // See https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#responses
    http::Response::builder()
        .status(StatusCode::OK)
        .header(GRPC_STATUS_HEADER_CODE, tonic::Code::Unimplemented as i32)
        .header("content-type", "application/grpc")
        .body(tonic::body::empty_body())
        .unwrap()
}
