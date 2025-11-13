use tonic::transport::{Endpoint, Error as TonicError};

pub(crate) type Error = Box<dyn std::error::Error + Send + Sync>;

/// Splits off the first component of a URL path.
/// As per the [gRPC spec], this corresponds to the gRPC service name and method name.
///
/// [gRPC spec]: https://github.com/grpc/grpc/blob/99a7f2995e66f9d6f5c3403dc7178238251fb2b5/doc/PROTOCOL-HTTP2.md#requests
pub(crate) fn parse_path(path: &str) -> Option<(&str, &str)> {
    let tail = match path.strip_prefix('/') {
        Some(tail) => tail,
        None => return None,
    };
    let (service, method) = match tail.split_once('/') {
        Some(split) => split,
        None => return None,
    };
    if service.is_empty() || method.is_empty() {
        None
    } else {
        Some((service, method))
    }
}

#[macro_export]
macro_rules! round_robin {
    ( $client:expr, $method:ident, $request:expr ) => {
        $client.round_robin_unary_unary($request, |mut c, r| async move { c.$method(r).await })
    };
}

pub trait ChunkedRequest {
    type Key: Clone + ToString;

    // FIXME: Move this to use a GAT once that's stabilized.
    fn iter_keys<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Self::Key> + 'a>;
    fn with_keys(&self, keys: Vec<Self::Key>) -> Self;
}

#[tonic::async_trait]
pub trait Connector: Sized + Send {
    async fn connect(dst: Endpoint) -> Result<Self, TonicError>;
    fn connect_lazy(dst: Endpoint) -> Self;
}

pub trait ScalarRequest {
    type Key: Clone + ToString;
    fn get_key(&self) -> &Self::Key;
}
