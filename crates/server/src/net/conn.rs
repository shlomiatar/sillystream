//! One socket's life: TLS, then HTTP requests, perhaps ending in a WebSocket upgrade.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

use super::Shared;

pub(super) type Tls = tokio_rustls::server::TlsStream<TcpStream>;

/// How long a client gets to finish TLS.
const HANDSHAKE_TIMEOUT_MS: u64 = 10_000;
/// A read asks for this much of the slab, and needs at least the smaller amount left in it.
pub(super) const READ_CHUNK_BYTES: usize = 64 * 1024;
pub(super) const READ_CHUNK_MIN_BYTES: usize = 16 * 1024;

/// Keeps a count right whichever way a task ends.
pub(super) struct Counted<'a>(pub &'a std::sync::atomic::AtomicU64);

impl<'a> Counted<'a> {
    pub(super) fn new(counter: &'a std::sync::atomic::AtomicU64) -> Counted<'a> {
        counter.fetch_add(1, Ordering::Relaxed);
        Counted(counter)
    }
}

impl Drop for Counted<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(super) async fn serve(shared: Arc<Shared>, home: usize, socket: std::net::TcpStream) {
    let counters = &shared.counters[home];
    let _open = Counted::new(&counters.open_connection_count);
    // On the core's thread, so that it registers with the core's reactor.
    let Ok(socket) = TcpStream::from_std(socket) else {
        return;
    };
    let handshake = tokio::time::timeout(
        Duration::from_millis(HANDSHAKE_TIMEOUT_MS),
        shared.acceptor.accept(socket),
    )
    .await;
    let Ok(Ok(tls)) = handshake else {
        counters
            .handshake_failed_count
            .fetch_add(1, Ordering::Relaxed);
        return;
    };
    let upgraded = super::http::HttpConn::new(&shared, home, tls).run().await;
    if let Some((tls, leftover)) = upgraded {
        super::wsconn::run(&shared, home, tls, leftover).await;
    }
}

/// One attempt to read, which never waits. `None` means nothing is here. The waker is thrown
/// away, because the caller goes back to `readable()`, which has its own.
pub(super) fn try_read(tls: &mut Tls, buffer: &mut [u8]) -> Option<io::Result<usize>> {
    let mut context = Context::from_waker(Waker::noop());
    let mut read_buffer = ReadBuf::new(buffer);
    match Pin::new(tls).poll_read(&mut context, &mut read_buffer) {
        Poll::Ready(Ok(())) => Some(Ok(read_buffer.filled().len())),
        Poll::Ready(Err(error)) => Some(Err(error)),
        Poll::Pending => None,
    }
}

/// As `try_read`. `None` means the socket has no room.
pub(super) fn try_write_vectored(
    tls: &mut Tls,
    slices: &[io::IoSlice<'_>],
) -> Option<io::Result<usize>> {
    let mut context = Context::from_waker(Waker::noop());
    match Pin::new(tls).poll_write_vectored(&mut context, slices) {
        Poll::Ready(result) => Some(result),
        Poll::Pending => None,
    }
}

/// `None` means what rustls holds doesn't fit the socket yet.
pub(super) fn try_flush(tls: &mut Tls) -> Option<io::Result<()>> {
    let mut context = Context::from_waker(Waker::noop());
    match Pin::new(tls).poll_flush(&mut context) {
        Poll::Ready(result) => Some(result),
        Poll::Pending => None,
    }
}

/// A frame still arriving. It lives in its own buffer, sized by what has really come, and moves
/// to a slab once whole: a header only claims a length, and a claim must not count against the
/// memory cap (D121).
pub(super) struct PartRead {
    bytes: Vec<u8>,
    have_bytes: usize,
    payload_bytes: usize,
}

impl PartRead {
    pub(super) fn new(payload_bytes: usize, first: &[u8]) -> PartRead {
        PartRead {
            bytes: first.to_vec(),
            have_bytes: first.len(),
            payload_bytes,
        }
    }

    /// Where the next bytes go: more than has come so far, up to what the frame claims.
    pub(super) fn room(&mut self) -> &mut [u8] {
        if self.have_bytes == self.bytes.len() {
            let wanted_bytes = (self.have_bytes * 2).max(READ_CHUNK_BYTES);
            self.bytes.resize(wanted_bytes.min(self.payload_bytes), 0);
        }
        &mut self.bytes[self.have_bytes..]
    }

    pub(super) fn filled(&mut self, read_bytes: usize) {
        self.have_bytes += read_bytes;
    }

    /// Takes what it still needs, and says how much that was.
    pub(super) fn add(&mut self, rest: &[u8]) -> usize {
        let copy_bytes = (self.payload_bytes - self.have_bytes).min(rest.len());
        self.bytes.truncate(self.have_bytes);
        self.bytes.extend_from_slice(&rest[..copy_bytes]);
        self.have_bytes += copy_bytes;
        copy_bytes
    }

    pub(super) fn is_whole(&self) -> bool {
        self.have_bytes == self.payload_bytes
    }

    pub(super) fn payload(&self) -> &[u8] {
        &self.bytes[..self.have_bytes]
    }
}
