use crate::metrics::string_interner::{InternResult, StringInterner};
use crate::metrics::{MetricsEmitter, TagKey};
use hyper::server::accept::Accept;
use hyper::server::conn::{AddrIncoming, AddrStream};
use lazy_static::lazy_static;
use pin_project::{pin_project, pinned_drop};
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::metrics::v2::{Counter, Gauge};

/// Define the tags for our connection-accept counter.
/// We currently only track the status and the address we are bound to
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, TagKey)]
struct AcceptRateKey {
    #[tag(name = "status")]
    status: &'static str,
    #[tag(name = "local_address")]
    local_address: InternResult,
}

/// Define the tags for our connection gauge.
/// We only track the local bind address
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, TagKey)]
struct ConnectionKey {
    #[tag(name = "local_address")]
    local_address: InternResult,
}

/// A structure defining all of the metrics we export from our [`InstrumentedConnection`]
/// and [`InstrumentedIncoming`]. These metrics are at a level below grpc, and even hyper.
/// This is purely a way to see what the underlying tcp is doing.
#[derive(Debug, MetricsEmitter)]
struct ConnectionMetrics {
    #[metric(name = "pigeon.server.connection_count")]
    connection_count: Gauge<ConnectionKey, u64>,
    #[metric(name = "pigeon.server.connection_rate")]
    connection_rate: Counter<AcceptRateKey>,
}

lazy_static! {
    static ref CONNECTION_METRICS: Arc<ConnectionMetrics> = {
        let metrics = Arc::new(ConnectionMetrics {
            connection_count: Default::default(),
            connection_rate: Default::default(),
        });
        crate::metrics::v2::register_emitter(metrics.clone()).emit_forever();
        metrics
    };
    // NOTE: load bearing assertion that we won't bind on more than 16 addresses
    static ref LOCAL_ADDR_INTERNER: StringInterner = StringInterner::new(16);
}

/// An InstrumentedConnection is a very simple wrapper around hyper's [`AddrStream`]
/// All it adds on top is a [`Gauge`] tracking the number of connections from a given
/// listening socket.
#[pin_project(PinnedDrop)]
pub struct InstrumentedConnection {
    metrics_key: ConnectionKey,
    #[pin]
    inner: AddrStream,
}

impl InstrumentedConnection {
    pub fn new(inner: AddrStream, local_addr: InternResult) -> Self {
        let metrics_key = ConnectionKey {
            local_address: local_addr,
        };
        CONNECTION_METRICS.connection_count.incr(metrics_key, 1_u64);
        Self { metrics_key, inner }
    }
}

#[pinned_drop]
impl PinnedDrop for InstrumentedConnection {
    fn drop(self: Pin<&mut Self>) {
        CONNECTION_METRICS
            .connection_count
            .decr(self.metrics_key, 1_u64);
    }
}

impl AsRawFd for InstrumentedConnection {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.inner.as_raw_fd()
    }
}

impl AsyncRead for InstrumentedConnection {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.project().inner.poll_read(cx, buf)
    }
}

impl AsyncWrite for InstrumentedConnection {
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        self.project().inner.poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        self.project().inner.poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        self.project().inner.poll_write_vectored(cx, bufs)
    }
}

/// We provider a wrapper around hyper's [`AddrIncoming`] to instrument the rate of accepts
/// and to wrap all resulting [`AddrStream`] instances with [`InstrumentedConnection`]
#[pin_project]
pub struct InstrumentedIncoming {
    local_addr: InternResult,
    #[pin]
    inner: AddrIncoming,
}

impl InstrumentedIncoming {
    pub fn new(inner: AddrIncoming) -> Self {
        let local_addr = LOCAL_ADDR_INTERNER.intern(inner.local_addr().to_string());
        Self { local_addr, inner }
    }
}

impl Accept for InstrumentedIncoming {
    type Conn = InstrumentedConnection;
    type Error = std::io::Error;
    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Self::Conn, Self::Error>>> {
        let laddr = self.local_addr;
        match self.project().inner.poll_accept(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(conn))) => {
                CONNECTION_METRICS.connection_rate.incr(AcceptRateKey {
                    local_address: laddr,
                    status: "ok",
                });
                Poll::Ready(Some(Ok(InstrumentedConnection::new(conn, laddr))))
            }
            Poll::Ready(Some(Err(e))) => {
                CONNECTION_METRICS.connection_rate.incr(AcceptRateKey {
                    local_address: laddr,
                    status: "error",
                });
                Poll::Ready(Some(Err(e)))
            }
        }
    }
}
