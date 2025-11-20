use bytes::{Buf, BufMut, Bytes, BytesMut};
use http_body::Body;
use prost::Message;
use std::{
    collections::VecDeque,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use tonic::{
    codec::{DecodeBuf, Decoder},
    Status,
};

#[derive(Clone)]
pub struct MockBody {
    data: VecDeque<Bytes>,
}

impl MockBody {
    pub fn new(data: Vec<impl Message>) -> Self {
        let mut queue: VecDeque<Bytes> = VecDeque::with_capacity(16);
        for msg in data {
            let buf = Self::encode(msg);
            queue.push_back(buf);
        }

        MockBody { data: queue }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    // see: https://github.com/hyperium/tonic/blob/1b03ece2a81cb7e8b1922b3c3c1f496bd402d76c/tonic/src/codec/encode.rs#L52
    fn encode(msg: impl Message) -> Bytes {
        let mut buf = BytesMut::with_capacity(256);

        buf.reserve(5);
        unsafe {
            buf.advance_mut(5);
        }
        msg.encode(&mut buf).unwrap();
        {
            let len = buf.len() - 5;
            let mut buf = &mut buf[..5];
            buf.put_u8(0); // byte must be 0, reserve doesn't auto-zero
            buf.put_u32(len as u32);
        }
        buf.freeze()
    }
}

impl Body for MockBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_data(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        if !self.is_empty() {
            let msg = self.data.pop_front().unwrap();
            Poll::Ready(Some(Ok(msg)))
        } else {
            Poll::Ready(None)
        }
    }

    fn poll_trailers(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Result<Option<http::HeaderMap>, Self::Error>> {
        Poll::Ready(Ok(None))
    }
}
/// A [`Decoder`] that knows how to decode `U`.
#[derive(Debug, Clone, Default)]
pub struct ProstDecoder<U>(PhantomData<U>);

impl<U> ProstDecoder<U> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<U: Message + Default> Decoder for ProstDecoder<U> {
    type Item = U;
    type Error = Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let item = Message::decode(buf.chunk())
            .map(Option::Some)
            .map_err(|e| Status::internal(e.to_string()))?;

        buf.advance(buf.chunk().len());
        Ok(item)
    }
}
