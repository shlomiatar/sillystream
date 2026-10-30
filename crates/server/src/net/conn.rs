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
use crate::slab::SlabRegion;

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

/// A region and how far it has been read: the unfinished read of a frame or a body.
pub(super) struct PartRead {
    pub region: SlabRegion,
    pub have_bytes: usize,
}
