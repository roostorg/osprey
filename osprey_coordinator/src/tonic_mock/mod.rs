//! This crate is a vendored fork of https://github.com/rnw159/tonic-mock which is not maintained anymore.
//! We keep this fork so we can push our version of tonic forward
use prost::Message;
use tonic::{Request, Streaming};

mod mock;

pub use mock::{MockBody, ProstDecoder};

/// Generate streaming request for GRPC
///
/// When testing streaming RPC implemented with tonic, it is pretty clumsy
/// to build the streaming request, this function extracted test code and prost
/// decoder from tonic source code and wrap it with a nice interface. With it,
/// testing your streaming RPC implementation is much easier.
///
/// Usage:
/// ```
/// use bytes::Bytes;
/// use prost::Message;
/// use tonic_mock::streaming_request;
///
/// // normally this should be generated from protos with prost
/// #[derive(Clone, PartialEq, Message)]
/// pub struct Event {
///     #[prost(bytes = "bytes", tag = "1")]
///     pub id: Bytes,
///     #[prost(bytes = "bytes", tag = "2")]
///     pub data: Bytes,
/// }
///
/// let event = Event { id: Bytes::from("1"), data: Bytes::from("a".repeat(10)) };
/// let mut events = vec![event.clone(), event.clone(), event];
/// let stream = tonic_mock::streaming_request(events);
///
pub fn streaming_request<T>(messages: Vec<T>) -> Request<Streaming<T>>
where
    T: Message + Default + 'static,
{
    let body = MockBody::new(messages);
    let decoder: ProstDecoder<T> = ProstDecoder::new();
    let stream = Streaming::new_request(decoder, body, None, None);

    Request::new(stream)
}
