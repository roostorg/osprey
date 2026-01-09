//! This crate is a vendored fork of https://github.com/rnw159/tonic-mock which is not maintained anymore.
//! We keep this fork so we can push our version of tonic forward
use futures::{Stream, StreamExt};
use prost::Message;
use std::pin::Pin;
use tonic::{Request, Response, Status, Streaming};

mod mock;

pub use mock::{MockBody, ProstDecoder};

pub type StreamResponseInner<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync>>;
pub type StreamResponse<T> = Response<StreamResponseInner<T>>;

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

/// a simple wrapper to process and validate streaming response
///
/// Usage:
/// ```
/// use tonic::{Response, Status};
/// use futures::Stream;
/// use std::pin::Pin;
///
/// #[derive(Clone, PartialEq, ::prost::Message)]
/// pub struct ResponsePush {
///     #[prost(int32, tag = "1")]
///     pub code: i32,
/// }
///
/// // below code is to mimic a stream response from a GRPC service
/// let output = async_stream::try_stream! {
///     yield ResponsePush { code: 0 };
///     yield ResponsePush { code: 1 };
///     yield ResponsePush { code: 2 };
/// };
/// let response = Response::new(Box::pin(output) as tonic_mock::StreamResponseInner<ResponsePush>);
/// let rt = tokio::runtime::Runtime::new().unwrap();
///
/// // now we process the events
/// rt.block_on(async {
///     tonic_mock::process_streaming_response(response, |msg, i| {
///         assert!(msg.is_ok());
///         assert_eq!(msg.as_ref().unwrap().code, i as i32);
///     }).await;
/// });
/// ```

pub async fn process_streaming_response<T, F>(response: StreamResponse<T>, f: F)
where
    T: Message + Default + 'static,
    F: Fn(Result<T, Status>, usize),
{
    let mut i: usize = 0;
    let mut messages = response.into_inner();
    while let Some(v) = messages.next().await {
        f(v, i);
        i += 1;
    }
}

/// convert a streaming response to a Vec for simplified testing
///
/// Usage:
/// ```
/// use tonic::{Response, Status};
/// use futures::Stream;
/// use std::pin::Pin;
///
/// #[derive(Clone, PartialEq, ::prost::Message)]
/// pub struct ResponsePush {
///     #[prost(int32, tag = "1")]
///     pub code: i32,
/// }
///
/// // below code is to mimic a stream response from a GRPC service
/// let output = async_stream::try_stream! {
///     yield ResponsePush { code: 0 };
///     yield ResponsePush { code: 1 };
///     yield ResponsePush { code: 2 };
/// };
/// let response = Response::new(Box::pin(output) as tonic_mock::StreamResponseInner<ResponsePush>);
/// let rt = tokio::runtime::Runtime::new().unwrap();
///
/// // now we convert response to vec
/// let result: Vec<Result<ResponsePush, Status>> = rt.block_on(async { tonic_mock::stream_to_vec(response).await });
/// for (i, v) in result.iter().enumerate() {
///     assert!(v.is_ok());
///     assert_eq!(v.as_ref().unwrap().code, i as i32);
/// }
/// ```
pub async fn stream_to_vec<T>(response: StreamResponse<T>) -> Vec<Result<T, Status>>
where
    T: Message + Default + 'static,
{
    let mut result = Vec::new();
    let mut messages = response.into_inner();
    while let Some(v) = messages.next().await {
        result.push(v)
    }
    result
}
