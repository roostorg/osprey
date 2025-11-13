use std::env;
use std::io::Write;
use std::io::{self, BufWriter};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cadence::{ErrorKind, MetricError, MetricResult, MetricSink};

const DEFAULT_BUFFER_SIZE: usize = 512;
const SOCKET_RECYCLE_INTERVAL: Duration = Duration::from_secs(15);

fn get_buffer_size_from_environ() -> usize {
    env::var("CADENCE_DEFAULT_BUFFER_SIZE")
        .ok()
        .and_then(|x| x.parse().ok())
        .unwrap_or(DEFAULT_BUFFER_SIZE)
}

fn get_addr<A: ToSocketAddrs>(addr: A) -> MetricResult<SocketAddr> {
    match addr.to_socket_addrs()?.next() {
        Some(addr) => Ok(addr),
        None => Err(MetricError::from((
            ErrorKind::InvalidInput,
            "No socket addresses yielded",
        ))),
    }
}

#[derive(Debug)]
pub struct UdpMetricSink {
    addr: SocketAddr,
    socket: UdpSocket,
    last_socket_created: Instant,
}

impl UdpMetricSink {
    pub fn from<A>(to_addr: A) -> MetricResult<UdpMetricSink>
    where
        A: ToSocketAddrs,
    {
        let addr = get_addr(to_addr)?;
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.set_nonblocking(true)?;

        Ok(UdpMetricSink {
            addr,
            socket,
            last_socket_created: Instant::now(),
        })
    }
}

impl Write for UdpMetricSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.last_socket_created.elapsed() > SOCKET_RECYCLE_INTERVAL {
            let socket = UdpSocket::bind("0.0.0.0:0")?;
            socket.set_nonblocking(true)?;
            self.socket = socket;
            self.last_socket_created = Instant::now();
        }
        self.socket.send_to(buf, &self.addr)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct UdpWriteAdapter {
    sink: UdpMetricSink,
}

impl UdpWriteAdapter {
    pub(crate) fn new(sink: UdpMetricSink) -> UdpWriteAdapter {
        Self { sink }
    }
}

impl Write for UdpWriteAdapter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.sink.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.sink.flush()
    }
}

/// This [`MetricSink`] implementation is buffered. It will hold on top data in its internal buffer until it has enough
/// to send out. This means that if you require timely delivery of metrics in low-throughput scenarios you will need to
/// periodically call [`BufferedUdpMetricSink::flush`]
#[derive(Clone, Debug)]
pub struct BufferedUdpMetricSink {
    buffer: Arc<Mutex<MultiLineWriter<UdpWriteAdapter>>>,
}

impl BufferedUdpMetricSink {
    pub fn from<A>(sink_addr: A) -> MetricResult<BufferedUdpMetricSink>
    where
        A: ToSocketAddrs,
    {
        Self::with_capacity(sink_addr, get_buffer_size_from_environ())
    }

    pub fn with_capacity<A>(sink_addr: A, cap: usize) -> MetricResult<BufferedUdpMetricSink>
    where
        A: ToSocketAddrs,
    {
        let sink = UdpMetricSink::from(sink_addr)?;
        Ok(BufferedUdpMetricSink {
            buffer: Arc::new(Mutex::new(MultiLineWriter::new(
                UdpWriteAdapter::new(sink),
                cap,
            ))),
        })
    }
}

impl MetricSink for BufferedUdpMetricSink {
    fn emit(&self, metric: &str) -> io::Result<usize> {
        let mut writer = self.buffer.lock().unwrap();
        writer.write(metric.as_bytes())
    }

    fn flush(&self) -> io::Result<()> {
        let mut writer = self.buffer.lock().unwrap();
        writer.flush()
    }
}

#[derive(Debug, Default)]
struct WriterMetrics {
    inner_write: u64,
    buf_write: u64,
    flushed: u64,
}

#[derive(Debug)]
pub(crate) struct MultiLineWriter<T>
where
    T: Write,
{
    written: usize,
    capacity: usize,
    metrics: WriterMetrics,
    inner: BufWriter<T>,
    line_ending: Vec<u8>,
}

impl<T> MultiLineWriter<T>
where
    T: Write,
{
    pub(crate) fn new(inner: T, cap: usize) -> MultiLineWriter<T> {
        Self::with_ending(inner, cap, "\n")
    }

    pub(crate) fn with_ending(inner: T, cap: usize, end: &str) -> MultiLineWriter<T> {
        MultiLineWriter {
            written: 0,
            capacity: cap,
            metrics: WriterMetrics::default(),
            inner: BufWriter::with_capacity(cap, inner),
            line_ending: Vec::from(end.as_bytes()),
        }
    }

    #[allow(dead_code)]
    fn get_ref(&self) -> &T {
        self.inner.get_ref()
    }

    #[allow(dead_code)]
    fn get_metrics(&self) -> &WriterMetrics {
        &self.metrics
    }
}

impl<T> Write for MultiLineWriter<T>
where
    T: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let left = self.capacity - self.written;
        let required = buf.len() + self.line_ending.len();

        if required > self.capacity {
            self.metrics.inner_write += 1;
            Ok(self.inner.get_mut().write(buf)?)
        } else {
            if left < required {
                self.flush()?;
            }
            self.metrics.buf_write += 1;
            let write1 = self.inner.write(buf)?;
            self.written += write1;

            let write2 = self.inner.write(&self.line_ending)?;
            self.written += write2;

            Ok(write1)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.written > 0 {
            self.metrics.flushed += 1;
            self.inner.flush()?;
            self.written = 0;
        }
        Ok(())
    }
}
