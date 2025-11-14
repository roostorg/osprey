//! Copy of
//! https://github.com/hyperium/tonic/blob/v0.6.2/tonic/src/transport/server/recover_error.rs

use futures::ready;
use http::Response;
use pin_project::pin_project;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tower::Service;

use super::option_pin::{OptionPin, OptionPinProj};
use super::status::{add_status_header, try_status_from_error};

/// Middleware that attempts to recover from service errors by turning them into a response built
/// from the `Status`.
#[derive(Debug, Clone)]
pub(crate) struct RecoverError<S> {
    inner: S,
}

impl<S> RecoverError<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, R, ResBody> Service<R> for RecoverError<S>
where
    S: Service<R, Response = Response<ResBody>>,
    S::Error: Into<super::utils::Error>,
{
    type Response = Response<MaybeEmptyBody<ResBody>>;
    type Error = super::utils::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: R) -> Self::Future {
        ResponseFuture {
            inner: self.inner.call(req),
        }
    }
}

#[pin_project]
pub(crate) struct ResponseFuture<F> {
    #[pin]
    inner: F,
}

impl<F, E, ResBody> Future for ResponseFuture<F>
where
    F: Future<Output = Result<Response<ResBody>, E>>,
    E: Into<super::utils::Error>,
{
    type Output = Result<Response<MaybeEmptyBody<ResBody>>, super::utils::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result: Result<Response<_>, super::utils::Error> =
            ready!(self.project().inner.poll(cx)).map_err(Into::into);

        match result {
            Ok(response) => {
                let response = response.map(MaybeEmptyBody::full);
                Poll::Ready(Ok(response))
            }
            Err(err) => match try_status_from_error(err) {
                Ok(status) => {
                    let mut res = Response::new(MaybeEmptyBody::empty());
                    add_status_header(&status, res.headers_mut()).unwrap();
                    Poll::Ready(Ok(res))
                }
                Err(err) => Poll::Ready(Err(err)),
            },
        }
    }
}

#[pin_project]
pub(crate) struct MaybeEmptyBody<B> {
    #[pin]
    inner: OptionPin<B>,
}

impl<B> MaybeEmptyBody<B> {
    fn full(inner: B) -> Self {
        Self {
            inner: OptionPin::Some { inner },
        }
    }

    fn empty() -> Self {
        Self {
            inner: OptionPin::None,
        }
    }
}

impl<B> http_body::Body for MaybeEmptyBody<B>
where
    B: http_body::Body + Send,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_data(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        match self.project().inner.project() {
            OptionPinProj::Some { inner: b } => b.poll_data(cx),
            OptionPinProj::None => Poll::Ready(None),
        }
    }

    fn poll_trailers(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<http::HeaderMap>, Self::Error>> {
        match self.project().inner.project() {
            OptionPinProj::Some { inner: b } => b.poll_trailers(cx),
            OptionPinProj::None => Poll::Ready(Ok(None)),
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.inner {
            OptionPin::Some { inner: b } => b.is_end_stream(),
            OptionPin::None => true,
        }
    }
}
