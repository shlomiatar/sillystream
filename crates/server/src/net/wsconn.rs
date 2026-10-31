//! A WebSocket connection: frames off the socket into slabs, frames from the node onto the
//! socket, and auth on every operation (sections 6.5, 11.1, 12).
//!
//! One task does both directions, and nothing in it waits except the one `select!`, so a
//! socket that won't take bytes doesn't stop it hearing the node, or the reverse.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use proto::{
    ClientFrame, DATA_PREFIX_BYTES, ErrorCode, PROTOCOL_VERSION, ServerFrame, data_prefix,
    decode_client,
};
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;

use super::conn::{
    PartRead, READ_CHUNK_BYTES, READ_CHUNK_MIN_BYTES, Tls, try_flush, try_read, try_write_vectored,
};
use super::ws::{
    FrameHeader, HeaderParse, MAX_HEADER_BYTES, OP_BINARY, OP_CLOSE, OP_CONTINUATION, OP_PING,
    OP_PONG, OP_TEXT, encode_header, header_array, parse_header, unmask,
};
use super::{Shared, with_core_io};
use crate::connection::{Connection, DataPart, Event, Subscription};
use crate::delivery::cursor_name_is_allowed;
use crate::message::{Reply, StateChange};
use crate::node::{Client, ClientError, OwnedStateOp, RegionPublish};
use crate::slab::SlabRegion;
use crate::state::StateOp;

/// A frame before `HELLO` can only be a token, and an unknown client doesn't get to make the
/// server allocate `max_frame`.
const PRE_AUTH_MAX_FRAME_BYTES: u64 = 128 * 1024;
/// How long a client has to send `HELLO`.
const HELLO_TIMEOUT_MS: u64 = 10_000;
/// How long a closing socket gets to take what is left.
const CLOSE_TIMEOUT_MS: u64 = 1_000;
/// More frames from the node wait when this many `DATA` bytes wait for the socket. So the
/// connection's window sees a slow client (D91).
const OUT_DATA_HIGH_BYTES: u64 = 256 * 1024;
/// Commands stop being read when this many reply bytes wait for the socket, which bounds
/// what a client can make the server hold by never reading (D102).
const OUT_REPLY_HIGH_BYTES: u64 = 1024 * 1024;
/// Reads per wake, so that a fast sender leaves the socket's other work a turn.
const READS_PER_WAKE_COUNT: usize = 16;
/// How often a write the node can't take yet is tried again (D62).
const RETRY_FULL_MS: u64 = 1;
const OPEN_HANDLES_MAX_COUNT: usize = 65_536;
const IOV_MAX_COUNT: usize = 64;
const REVOKED_MESSAGE: &str = "the renewed token doesn't grant this stream";

/// What a connection writes, in order.
enum Chunk {
    Owned {
        bytes: Vec<u8>,
        at: usize,
        reply: bool,
    },
    Part {
        part: DataPart,
        at: usize,
    },
}

#[derive(Default)]
struct OutQueue {
    chunks: VecDeque<Chunk>,
    reply_bytes: u64,
    data_bytes: u64,
}

impl OutQueue {
    fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Small replies share an allocation and go out as one write.
    fn push_reply(&mut self, frame_bytes: &[&[u8]]) {
        let len: usize = frame_bytes.iter().map(|part| part.len()).sum();
        self.reply_bytes += len as u64;
        if let Some(Chunk::Owned {
            bytes, reply: true, ..
        }) = self.chunks.back_mut()
            && bytes.len() + len <= 32 * 1024
        {
            for part in frame_bytes {
                bytes.extend_from_slice(part);
            }
            return;
        }
        let mut bytes = Vec::with_capacity(len.max(256));
        for part in frame_bytes {
            bytes.extend_from_slice(part);
        }
        self.chunks.push_back(Chunk::Owned {
            bytes,
            at: 0,
            reply: true,
        });
    }

    fn push_data(&mut self, head: Vec<u8>, parts: &[DataPart]) {
        self.data_bytes += head.len() as u64;
        self.chunks.push_back(Chunk::Owned {
            bytes: head,
            at: 0,
            reply: false,
        });
        for part in parts {
            self.data_bytes += part.as_bytes().len() as u64;
            self.chunks.push_back(Chunk::Part {
                part: part.clone(),
                at: 0,
            });
        }
    }

    fn advance(&mut self, mut written_bytes: usize) {
        while written_bytes > 0 {
            let Some(front) = self.chunks.front_mut() else {
                return;
            };
            let (chunk_bytes, at, is_reply) = match front {
                Chunk::Owned { bytes, at, reply } => (bytes.len(), at, *reply),
                Chunk::Part { part, at } => (part.as_bytes().len(), at, false),
            };
            let taken_bytes = (chunk_bytes - *at).min(written_bytes);
            *at += taken_bytes;
            written_bytes -= taken_bytes;
            if is_reply {
                self.reply_bytes -= taken_bytes as u64;
            } else {
                self.data_bytes -= taken_bytes as u64;
            }
            if *at == chunk_bytes {
                self.chunks.pop_front();
            }
        }
        if self.chunks.is_empty() && self.chunks.capacity() > 16 {
            self.chunks = VecDeque::new();
        }
    }

    /// Writes until the socket has no room. Rustls copies what it encrypts, so this is the one
    /// copy of a record on its way out (section 5.2 has no way round TLS).
    fn pump(&mut self, tls: &mut Tls) -> io::Result<()> {
        while !self.chunks.is_empty() {
            let mut slices = [io::IoSlice::new(&[]); IOV_MAX_COUNT];
            let mut used = 0;
            for (slot, chunk) in slices.iter_mut().zip(&self.chunks) {
                *slot = io::IoSlice::new(match chunk {
                    Chunk::Owned { bytes, at, .. } => &bytes[*at..],
                    Chunk::Part { part, at } => &part.as_bytes()[*at..],
                });
                used += 1;
            }
            match try_write_vectored(tls, &slices[..used]) {
                None => return Ok(()),
                Some(Ok(0)) => return Err(io::ErrorKind::WriteZero.into()),
                Some(Ok(written_bytes)) => self.advance(written_bytes),
                Some(Err(error)) => return Err(error),
            }
        }
        match try_flush(tls) {
            None | Some(Ok(())) => Ok(()),
            Some(Err(error)) => Err(error),
        }
    }
}

/// A write waiting for room in the node, in order.
enum PendingOp {
    Publish(RegionPublish),
    State(OwnedStateOp),
}

/// A connection after a good `HELLO`.
struct Session {
    token: auth::Token,
    client: Client,
    connection: Connection,
    handles: HashMap<u32, Arc<str>>,
    subs: HashMap<u32, Arc<str>>,
    /// Subscriptions a renewed token ended, so that a `COMMIT` on one still hears why.
    revoked_subs: HashSet<u32>,
}

struct WsConn<'a> {
    shared: &'a Shared,
    home: usize,
    tls: Tls,
    out: OutQueue,
    session: Option<Box<Session>>,
    /// A header split across two reads.
    head: [u8; MAX_HEADER_BYTES],
    head_have_bytes: usize,
    /// A frame still arriving (D121).
    part: Option<(FrameHeader, PartRead)>,
    /// Payload to throw away, of a frame over the limit.
    /// rustls may hold plaintext: the last read did not come back empty.
    read_again: bool,
    discard_bytes: u64,
    /// A fragmented message so far. It is the slow path, so it is copied together.
    fragments: Option<Vec<u8>>,
    /// Fragments of a message already refused, up to its last frame.
    dropping_fragments: bool,
    /// What couldn't be looked at until `pending` empties.
    backlog: Option<SlabRegion>,
    pending: VecDeque<PendingOp>,
    /// When `pending` is next tried. It stays put across wakes, or a busy connection would keep
    /// pushing it back (D122).
    retry_at: Option<Instant>,
    /// When a frame or fragmented message still arriving runs out of time.
    partial_deadline: Option<Instant>,
    closing: bool,
    hello_deadline: Instant,
    /// The token's expiry timer ended the subscriptions, so it needn't fire again.
    expiry_seen: bool,
}

enum Wake {
    Writable(io::Result<()>),
    Reply(Option<Reply>),
    Event(Option<Event>),
    Retry,
    Readable(io::Result<()>),
    Expiry,
    HelloTimeout,
    PartialTimeout,
}

async fn recv_reply(client: Option<&mut Client>) -> Option<Reply> {
    match client {
        Some(client) => client.recv().await,
        None => std::future::pending().await,
    }
}

async fn recv_event(connection: Option<&mut Connection>) -> Option<Event> {
    match connection {
        Some(connection) => connection.recv().await,
        None => std::future::pending().await,
    }
}

/// Never finishes without a time to finish at, for a timer that may not be running.
async fn sleep_until_some(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Runs the connection until the socket ends. `leftover` is what arrived behind the upgrade.
pub(super) async fn run(shared: &Shared, home: usize, tls: Tls, leftover: Vec<u8>) {
    let mut conn = WsConn {
        shared,
        home,
        tls,
        out: OutQueue::default(),
        session: None,
        head: [0; MAX_HEADER_BYTES],
        head_have_bytes: 0,
        part: None,
        read_again: true,
        discard_bytes: 0,
        fragments: None,
        dropping_fragments: false,
        backlog: None,
        pending: VecDeque::new(),
        retry_at: None,
        partial_deadline: None,
        closing: false,
        hello_deadline: Instant::now() + Duration::from_millis(HELLO_TIMEOUT_MS),
        expiry_seen: false,
    };
    if !leftover.is_empty() {
        let region = with_core_io(shared, home, |io| io.pool.copy_in(&leftover));
        drop(leftover);
        if conn.feed(region).await.is_err() {
            return;
        }
    }
    let _ = conn.run().await;
    // Whatever was held went with the connection.
    conn.settle_held();
}

impl WsConn<'_> {
    fn counters(&self) -> &super::IoCounters {
        &self.shared.counters[self.home]
    }

    fn settle_held(&mut self) {
        if self.part.take().is_some() {
            self.counters()
                .held_buffer_count
                .fetch_sub(1, Ordering::Relaxed);
        }
        if self.backlog.take().is_some() {
            self.counters()
                .held_buffer_count
                .fetch_sub(1, Ordering::Relaxed);
        }
    }

    async fn run(&mut self) -> io::Result<()> {
        loop {
            self.out.pump(&mut self.tls)?;
            let want_write = !self.out.is_empty() || self.tls.get_ref().1.wants_write();
            if self.closing && !want_write {
                let _ = tokio::time::timeout(
                    Duration::from_millis(CLOSE_TIMEOUT_MS),
                    self.tls.shutdown(),
                )
                .await;
                return Ok(());
            }
            let may_read = !self.closing
                && self.pending.is_empty()
                && self.backlog.is_none()
                && self.out.reply_bytes < OUT_REPLY_HIGH_BYTES;
            let want_events = !self.closing && self.out_has_event_room();
            if self.pending.is_empty() {
                self.retry_at = None;
            } else if self.retry_at.is_none() {
                self.retry_at = Some(Instant::now() + Duration::from_millis(RETRY_FULL_MS));
            }
            let retry_at = self.retry_at;
            let partial_deadline = self.update_partial_deadline();
            let no_session = self.session.is_none();
            let hello_deadline = self.hello_deadline;
            let expiry = self.expiry_wait();
            let close_wait = self.closing;
            let read_again = self.read_again;
            let tcp = self.tls.get_ref().0;
            let (client, connection) = match &mut self.session {
                Some(session) => (Some(&mut session.client), Some(&mut session.connection)),
                None => (None, None),
            };
            let wake = tokio::select! {
                biased;
                // Timers first: a steady flow below is always ready, and must not starve them.
                _ = sleep_until_some(retry_at) => Wake::Retry,
                _ = sleep_until_some(partial_deadline) => Wake::PartialTimeout,
                _ = tokio::time::sleep_until(hello_deadline), if no_session => Wake::HelloTimeout,
                _ = tokio::time::sleep(expiry.unwrap_or_default()), if expiry.is_some() => Wake::Expiry,
                ready = tcp.writable(), if want_write => Wake::Writable(ready),
                reply = recv_reply(client) => Wake::Reply(reply),
                event = recv_event(connection), if want_events => Wake::Event(event),
                // rustls may hold plaintext the socket no longer says anything about.
                _ = std::future::ready(()), if read_again && may_read => Wake::Readable(Ok(())),
                ready = tcp.readable(), if may_read => Wake::Readable(ready),
                _ = tokio::time::sleep(Duration::from_millis(CLOSE_TIMEOUT_MS)), if close_wait => {
                    return Ok(());
                }
            };
            match wake {
                Wake::Writable(ready) => ready?,
                Wake::Reply(reply) => {
                    if let Some(reply) = reply {
                        self.on_reply(reply);
                    }
                    // Together they make one TLS record, not one each.
                    while self.out.reply_bytes < OUT_REPLY_HIGH_BYTES {
                        let Some(reply) = self
                            .session
                            .as_mut()
                            .and_then(|session| session.client.try_recv())
                        else {
                            break;
                        };
                        self.on_reply(reply);
                    }
                }
                Wake::Event(event) => {
                    let Some(event) = event else {
                        // The node has stopped.
                        return Ok(());
                    };
                    self.on_event(event);
                    while self.out_has_event_room() {
                        let Some(event) = self
                            .session
                            .as_mut()
                            .and_then(|session| session.connection.try_recv())
                        else {
                            break;
                        };
                        self.on_event(event);
                    }
                }
                Wake::Retry => {
                    self.retry_at = None;
                    self.retry_pending().await?;
                }
                Wake::Readable(ready) => {
                    ready?;
                    self.on_readable().await?;
                }
                Wake::Expiry => self.on_expiry(),
                Wake::HelloTimeout => {
                    self.send_error(0, ErrorCode::Unauthorized, "no HELLO in time");
                    self.close(1008);
                }
                Wake::PartialTimeout => {
                    self.send_error(0, ErrorCode::Malformed, "a frame didn't finish in time");
                    self.settle_held();
                    self.fragments = None;
                    self.close(1008);
                }
            }
        }
    }

    /// Events wait when either kind of frame has backed up: `STATE` and `SUBSCRIBED` go out as
    /// replies, and leaving them in the connection is what closes its window (D123).
    fn out_has_event_room(&self) -> bool {
        self.out.data_bytes < OUT_DATA_HIGH_BYTES && self.out.reply_bytes < OUT_REPLY_HIGH_BYTES
    }

    /// Starts the clock when a frame or message begins, and stops it when none is under way.
    fn update_partial_deadline(&mut self) -> Option<Instant> {
        if self.closing || (self.part.is_none() && self.fragments.is_none()) {
            self.partial_deadline = None;
        } else if self.partial_deadline.is_none() {
            let timeout = Duration::from_millis(self.shared.partial_frame_timeout_ms);
            self.partial_deadline = Some(Instant::now() + timeout);
        }
        self.partial_deadline
    }

    /// Reads while there is something to read, for a bounded number of reads. A frame still
    /// arriving reads straight into its region. Otherwise part of the core's slab is borrowed
    /// for the read alone, which doesn't wait, so nothing is held between reads.
    async fn on_readable(&mut self) -> io::Result<()> {
        // Only a read that found nothing clears this.
        self.read_again = true;
        for _ in 0..READS_PER_WAKE_COUNT {
            if self.closing || !self.pending.is_empty() || self.backlog.is_some() {
                return Ok(());
            }
            if let Some((_, part)) = &mut self.part {
                let read = try_read(&mut self.tls, part.room());
                match read {
                    None => {
                        self.read_again = false;
                        return Ok(());
                    }
                    Some(Err(error)) => return Err(error),
                    Some(Ok(0)) => return Err(io::ErrorKind::UnexpectedEof.into()),
                    Some(Ok(read_bytes)) => {
                        part.filled(read_bytes);
                        if part.is_whole() {
                            self.finish_part().await?;
                        }
                    }
                }
                continue;
            }
            let counters = &self.shared.counters[self.home];
            let tls = &mut self.tls;
            let read = with_core_io(self.shared, self.home, |io| {
                let mut chunk = io.pool.borrow_chunk(READ_CHUNK_MIN_BYTES, READ_CHUNK_BYTES);
                counters.read_borrow_count.fetch_add(1, Ordering::Relaxed);
                let result = try_read(tls, &mut chunk);
                if let Some(Ok(read_bytes)) = result
                    && read_bytes > 0
                {
                    let tail = chunk.split_off(read_bytes);
                    io.pool.give_back(tail);
                    return Ok(Some(chunk));
                }
                io.pool.give_back(chunk);
                Err(result)
            });
            match read {
                Ok(Some(chunk)) => self.feed(chunk).await?,
                Ok(None) | Err(None) => {
                    self.read_again = false;
                    return Ok(());
                }
                Err(Some(Err(error))) => return Err(error),
                Err(Some(Ok(_))) => return Err(io::ErrorKind::UnexpectedEof.into()),
            }
        }
        Ok(())
    }

    /// Takes frames out of what was read. Whatever is whole is handled where it lies.
    async fn feed(&mut self, mut rest: SlabRegion) -> io::Result<()> {
        while !rest.is_empty() {
            if !self.pending.is_empty() || self.closing {
                if !self.closing {
                    self.counters()
                        .held_buffer_count
                        .fetch_add(1, Ordering::Relaxed);
                    self.backlog = Some(rest);
                }
                return Ok(());
            }
            if self.discard_bytes > 0 {
                let drop_bytes = self.discard_bytes.min(rest.len() as u64) as usize;
                let _ = rest.split_to(drop_bytes);
                self.discard_bytes -= drop_bytes as u64;
                continue;
            }
            if let Some((_, part)) = &mut self.part {
                let copy_bytes = part.add(&rest);
                let _ = rest.split_to(copy_bytes);
                if part.is_whole() {
                    self.finish_part().await?;
                }
                continue;
            }
            let Some(header) = self.next_header(&mut rest) else {
                continue;
            };
            let limit_bytes = self.frame_limit_bytes();
            if header.mask.is_none() {
                self.protocol_error("client frames must be masked");
                return Ok(());
            }
            if header.payload_bytes > limit_bytes {
                if self.session.is_none() {
                    self.send_error(0, ErrorCode::FrameTooLarge, "frame over the limit");
                    self.close(1009);
                    return Ok(());
                }
                self.send_error(0, ErrorCode::FrameTooLarge, "frame over max_frame");
                self.discard_bytes = header.payload_bytes;
                self.fragments = None;
                self.dropping_fragments = !header.fin;
                continue;
            }
            let payload_bytes = header.payload_bytes as usize;
            if rest.len() >= payload_bytes {
                let payload = rest.split_to(payload_bytes);
                self.on_frame(header, payload).await?;
            } else {
                let part = PartRead::new(payload_bytes, &rest);
                let all_bytes = rest.len();
                let _ = rest.split_to(all_bytes);
                self.counters()
                    .held_buffer_count
                    .fetch_add(1, Ordering::Relaxed);
                self.part = Some((header, part));
            }
        }
        Ok(())
    }

    fn frame_limit_bytes(&self) -> u64 {
        if self.session.is_some() {
            self.shared.max_frame_bytes
        } else {
            PRE_AUTH_MAX_FRAME_BYTES
        }
    }

    /// The next header, taking its bytes from `rest`. `None` if more are needed, or if the
    /// connection is closing.
    fn next_header(&mut self, rest: &mut SlabRegion) -> Option<FrameHeader> {
        let parsed = if self.head_have_bytes == 0 {
            parse_header(rest)
        } else {
            let copy_bytes = (MAX_HEADER_BYTES - self.head_have_bytes).min(rest.len());
            self.head[self.head_have_bytes..self.head_have_bytes + copy_bytes]
                .copy_from_slice(&rest[..copy_bytes]);
            parse_header(&self.head[..self.head_have_bytes + copy_bytes])
        };
        match parsed {
            HeaderParse::NeedMore => {
                let copy_bytes = rest.len();
                let start = self.head_have_bytes;
                self.head[start..start + copy_bytes].copy_from_slice(rest);
                self.head_have_bytes += copy_bytes;
                let _ = rest.split_to(copy_bytes);
                None
            }
            HeaderParse::Invalid(reason) => {
                self.protocol_error(reason);
                let all_bytes = rest.len();
                let _ = rest.split_to(all_bytes);
                None
            }
            HeaderParse::Header(header) => {
                let _ = rest.split_to(header.header_bytes - self.head_have_bytes);
                self.head_have_bytes = 0;
                Some(header)
            }
        }
    }

    async fn finish_part(&mut self) -> io::Result<()> {
        if let Some((header, part)) = self.part.take() {
            self.counters()
                .held_buffer_count
                .fetch_sub(1, Ordering::Relaxed);
            let region = with_core_io(self.shared, self.home, |io| io.pool.copy_in(part.payload()));
            drop(part);
            self.on_frame(header, region).await?;
        }
        Ok(())
    }

    async fn on_frame(&mut self, header: FrameHeader, mut payload: SlabRegion) -> io::Result<()> {
        if let Some(mask) = header.mask {
            unmask(&mut payload, mask);
        }
        match header.opcode {
            OP_PING => self.push_ws_frame(OP_PONG, &payload[..]),
            OP_PONG => {}
            OP_CLOSE => {
                let code = match payload.first_chunk::<2>() {
                    Some(code) => u16::from_be_bytes(*code),
                    None => 1000,
                };
                self.close(code);
            }
            OP_TEXT => {
                self.send_error(0, ErrorCode::Malformed, "frames are binary");
                self.dropping_fragments = !header.fin;
            }
            OP_BINARY => {
                if self.fragments.is_some() || self.dropping_fragments {
                    self.protocol_error("a new message inside a fragmented one");
                } else if header.fin {
                    self.on_message(payload).await?;
                } else {
                    self.fragments = Some(payload.to_vec());
                }
            }
            OP_CONTINUATION => self.on_continuation(header, payload).await?,
            _ => self.protocol_error("unknown opcode"),
        }
        Ok(())
    }

    async fn on_continuation(
        &mut self,
        header: FrameHeader,
        payload: SlabRegion,
    ) -> io::Result<()> {
        if self.dropping_fragments {
            self.dropping_fragments = !header.fin;
            return Ok(());
        }
        let limit_bytes = self.frame_limit_bytes() as usize;
        let Some(fragments) = &mut self.fragments else {
            self.protocol_error("a continuation with nothing to continue");
            return Ok(());
        };
        if fragments.len() + payload.len() > limit_bytes {
            self.fragments = None;
            self.dropping_fragments = !header.fin;
            self.send_error(0, ErrorCode::FrameTooLarge, "message over max_frame");
            return Ok(());
        }
        fragments.extend_from_slice(&payload);
        if header.fin {
            let whole = self.fragments.take().unwrap_or_default();
            let region = with_core_io(self.shared, self.home, |io| io.pool.copy_in(&whole));
            self.on_message(region).await?;
        }
        Ok(())
    }

    /// One whole WebSocket message, which is one frame of section 11.1.
    async fn on_message(&mut self, mut message: SlabRegion) -> io::Result<()> {
        let max_frame_bytes = self.shared.max_frame_bytes as usize;
        let frame = match decode_client(&message, max_frame_bytes) {
            Ok(frame) => frame,
            Err(error) => {
                self.send_error(0, error.code(), &error.to_string());
                return Ok(());
            }
        };
        let now_ms = self.shared.clock.now_ms();
        if self.session.is_none() {
            let ClientFrame::Hello { ver, token } = frame else {
                self.send_error(0, ErrorCode::Unauthorized, "HELLO comes first");
                self.close(1008);
                return Ok(());
            };
            let token = token.to_owned();
            return self.on_hello(ver, &token, now_ms).await;
        }
        match frame {
            ClientFrame::Hello { ver, token } => {
                let token = token.to_owned();
                self.on_hello(ver, &token, now_ms).await?;
            }
            ClientFrame::Ping { nonce } => self.send_frame(ServerFrame::Pong { nonce }),
            ClientFrame::Open { handle, stream } => {
                let Some(session) = &mut self.session else {
                    return Ok(());
                };
                if session.handles.len() >= OPEN_HANDLES_MAX_COUNT
                    && !session.handles.contains_key(&handle)
                {
                    self.send_error(handle, ErrorCode::Malformed, "too many open handles");
                    return Ok(());
                }
                session.handles.insert(handle, Arc::from(stream));
            }
            ClientFrame::Subscribe(subscribe) => {
                let Some(session) = &mut self.session else {
                    return Ok(());
                };
                if let Err(error) = session
                    .token
                    .check(subscribe.stream, auth::PERM_READ, now_ms)
                {
                    let text = error.to_string();
                    self.send_error(subscribe.sub, ErrorCode::Unauthorized, &text);
                    return Ok(());
                }
                let filter = match subscribe.filter() {
                    Ok(filter) => filter,
                    Err(error) => {
                        self.send_error(subscribe.sub, error.code(), &error.to_string());
                        return Ok(());
                    }
                };
                // What the core would refuse is refused here, because `subs` is written before
                // the core answers and must only ever name what it took (D129).
                if session.subs.contains_key(&subscribe.sub) {
                    let reference = subscribe.sub;
                    self.send_error(reference, ErrorCode::Malformed, "subscription id is in use");
                    return Ok(());
                }
                if subscribe
                    .name
                    .is_some_and(|name| !cursor_name_is_allowed(name))
                {
                    let reference = subscribe.sub;
                    let text = "sink: cursors belong to sinks";
                    self.send_error(reference, ErrorCode::Malformed, text);
                    return Ok(());
                }
                if session.subs.len() >= self.shared.max_subscriptions {
                    self.send_error(
                        subscribe.sub,
                        ErrorCode::TooManySubscriptions,
                        "too many subscriptions",
                    );
                    return Ok(());
                }
                session.revoked_subs.remove(&subscribe.sub);
                session
                    .subs
                    .insert(subscribe.sub, Arc::from(subscribe.stream));
                session.connection.subscribe(Subscription {
                    sub: subscribe.sub,
                    stream: subscribe.stream,
                    from: subscribe.from,
                    linger_ms: subscribe.linger_ms,
                    author: subscribe.author,
                    clauses: filter.clauses(),
                    name: subscribe.name,
                    with_state: subscribe.with_state,
                    state_only: subscribe.state_only,
                    include_system: subscribe.include_system,
                });
            }
            ClientFrame::Unsubscribe { sub } => {
                if let Some(session) = &mut self.session {
                    session.subs.remove(&sub);
                    session.revoked_subs.remove(&sub);
                    session.connection.unsubscribe(sub);
                }
            }
            ClientFrame::Commit { sub, id } => {
                let Some(session) = &mut self.session else {
                    return Ok(());
                };
                if now_ms >= session.token.expiry_ms {
                    self.send_error(sub, ErrorCode::Unauthorized, "token has expired");
                    return Ok(());
                }
                if session.revoked_subs.contains(&sub) {
                    self.send_error(sub, ErrorCode::Unauthorized, REVOKED_MESSAGE);
                    return Ok(());
                }
                // A subscription this connection never had is the handle's error to report.
                let allowed = match session.subs.get(&sub) {
                    Some(stream) => session.token.check(stream, auth::PERM_READ, now_ms),
                    None => Ok(()),
                };
                if let Err(error) = allowed {
                    let text = error.to_string();
                    self.send_error(sub, ErrorCode::Unauthorized, &text);
                    return Ok(());
                }
                session.connection.commit(sub, id);
            }
            ClientFrame::StateOp {
                handle,
                req,
                expect_ver,
                op,
                doc,
            } => {
                let Some(session) = &mut self.session else {
                    return Ok(());
                };
                let Some(stream) = session.handles.get(&handle).cloned() else {
                    self.send_error(req, ErrorCode::Malformed, "handle isn't open");
                    return Ok(());
                };
                if let Err(error) = session.token.check(&stream, auth::PERM_STATE, now_ms) {
                    let text = error.to_string();
                    self.send_error(req, ErrorCode::Unauthorized, &text);
                    return Ok(());
                }
                let change = StateChange {
                    expect_ver,
                    op: state_op_from_wire(op),
                    doc: Bytes::copy_from_slice(doc),
                };
                let author = session.token.author;
                self.submit(PendingOp::State(OwnedStateOp {
                    stream,
                    req,
                    author,
                    change,
                }));
            }
            ClientFrame::Publish(publish) => {
                let Some(session) = &mut self.session else {
                    return Ok(());
                };
                let (handle, producer_seq) = (publish.handle, publish.producer_seq);
                let Some(stream) = session.handles.get(&handle).cloned() else {
                    self.send_nack(
                        handle,
                        producer_seq,
                        ErrorCode::Malformed,
                        "handle isn't open",
                    );
                    return Ok(());
                };
                let mut allowed = session.token.check(&stream, auth::PERM_PUBLISH, now_ms);
                if allowed.is_ok() && publish.state.is_some() {
                    allowed = session.token.check(&stream, auth::PERM_STATE, now_ms);
                }
                if let Err(error) = allowed {
                    let text = error.to_string();
                    self.send_nack(handle, producer_seq, ErrorCode::Unauthorized, &text);
                    return Ok(());
                }
                let author = session.token.author;
                let state = publish.state.map(|state| StateChange {
                    expect_ver: state.expect_ver,
                    op: state_op_from_wire(state.op),
                    doc: Bytes::copy_from_slice(state.doc),
                });
                let (producer_id, ack) = (publish.producer_id, publish.ack as u8);
                // `records` is the frame's last field (proto), so it is its tail.
                let records_start = message.len() - publish.records.len();
                let records = message.split_off(records_start);
                self.submit(PendingOp::Publish(RegionPublish {
                    stream,
                    handle,
                    producer_id,
                    producer_seq,
                    ack_level: Some(ack),
                    author,
                    state,
                    records,
                }));
            }
        }
        Ok(())
    }

    async fn on_hello(&mut self, ver: u16, token: &str, now_ms: u64) -> io::Result<()> {
        if ver != PROTOCOL_VERSION {
            // A client on another version can't be expected to read what follows (D41).
            self.send_error(0, ErrorCode::Malformed, "unknown protocol version");
            self.close(1002);
            return Ok(());
        }
        let token = match auth::verify(&self.shared.secret, token, now_ms) {
            Ok(token) => token,
            Err(error) => {
                self.send_error(0, ErrorCode::Unauthorized, &error.to_string());
                self.close(1008);
                return Ok(());
            }
        };
        self.expiry_seen = false;
        if let Some(session) = &mut self.session {
            // A newer token for the same connection, so that a client can outlive one.
            session.token = token;
        } else {
            let Some(connection) = self.shared.node.connection(self.home).await else {
                self.close(1011);
                return Ok(());
            };
            self.session = Some(Box::new(Session {
                token,
                client: self.shared.node.client(self.home),
                connection,
                handles: HashMap::new(),
                subs: HashMap::new(),
                revoked_subs: HashSet::new(),
            }));
        }
        let number = self.shared.session_count.fetch_add(1, Ordering::Relaxed);
        self.send_frame(ServerFrame::Welcome {
            session: (self.home as u64) << 48 | (number & 0xFFFF_FFFF_FFFF),
            max_frame_bytes: self.shared.max_frame_bytes.min(u32::MAX as u64) as u32,
            server_ms: now_ms,
        });
        self.end_subs_token_doesnt_grant(now_ms);
        Ok(())
    }

    /// A renewal is a new set of grants, and a subscription only lasts under one that covers it.
    /// Section 12 revokes by expiry, so without this a renewal could never take a stream away.
    fn end_subs_token_doesnt_grant(&mut self, now_ms: u64) {
        let max_revoked_count = self.shared.max_subscriptions;
        let Some(session) = &mut self.session else {
            return;
        };
        let mut ended = Vec::new();
        session.subs.retain(|sub, stream| {
            let keep = session.token.check(stream, auth::PERM_READ, now_ms).is_ok();
            if !keep {
                ended.push(*sub);
            }
            keep
        });
        for sub in &ended {
            session.connection.unsubscribe(*sub);
            if session.revoked_subs.len() < max_revoked_count {
                session.revoked_subs.insert(*sub);
            }
        }
        for sub in ended {
            self.send_error(sub, ErrorCode::Unauthorized, REVOKED_MESSAGE);
        }
    }

    /// Hands a write to the node, or lets it wait in order if the node can't take it.
    fn submit(&mut self, op: PendingOp) {
        if !self.pending.is_empty() {
            self.pending.push_back(op);
        } else if let Some(op) = self.try_submit(op) {
            self.pending.push_back(op);
        }
    }

    /// `None` if it went. Back if it has to wait.
    fn try_submit(&mut self, op: PendingOp) -> Option<PendingOp> {
        let session = self.session.as_mut()?;
        match op {
            PendingOp::Publish(publish) => match session.client.try_publish_region(publish) {
                Ok(()) => None,
                Err((ClientError::Full, publish)) => Some(PendingOp::Publish(publish)),
                Err((ClientError::NodeStopped, publish)) => {
                    let (handle, seq) = (publish.handle, publish.producer_seq);
                    self.send_nack(handle, seq, ErrorCode::RateLimited, "the node has stopped");
                    None
                }
            },
            PendingOp::State(op) => match session.client.try_state_op(op) {
                Ok(()) => None,
                Err((ClientError::Full, op)) => Some(PendingOp::State(op)),
                Err((ClientError::NodeStopped, op)) => {
                    let req = op.req;
                    self.send_error(req, ErrorCode::RateLimited, "the node has stopped");
                    None
                }
            },
        }
    }

    async fn retry_pending(&mut self) -> io::Result<()> {
        while let Some(op) = self.pending.pop_front() {
            if let Some(op) = self.try_submit(op) {
                self.pending.push_front(op);
                return Ok(());
            }
        }
        if let Some(backlog) = self.backlog.take() {
            self.counters()
                .held_buffer_count
                .fetch_sub(1, Ordering::Relaxed);
            self.feed(backlog).await?;
        }
        Ok(())
    }

    fn on_reply(&mut self, reply: Reply) {
        match reply {
            Reply::Ack {
                handle,
                producer_seq,
                first_id,
                count,
            } => self.send_frame(ServerFrame::Ack {
                handle,
                producer_seq,
                first_id,
                count,
            }),
            Reply::Nack {
                handle,
                producer_seq,
                code,
                msg,
            } => self.send_nack(handle, producer_seq, code, &msg),
            Reply::StateResult { req, ok, ver } => {
                self.send_frame(ServerFrame::StateResult { req, ok, ver });
            }
            Reply::Error {
                reference,
                code,
                msg,
            } => self.send_error(reference, code, &msg),
            Reply::Moved { .. } | Reply::RouteDone { .. } => {}
        }
    }

    fn on_event(&mut self, event: Event) {
        match event {
            Event::Subscribed {
                sub,
                head,
                state_ver,
                state,
            } => self.send_frame(ServerFrame::Subscribed {
                sub,
                head,
                state_ver,
                state: &state,
            }),
            Event::Data { sub, records } => {
                let records_bytes = records.len_bytes() as usize;
                let Some(prefix) = data_prefix(sub, records_bytes) else {
                    return;
                };
                let mut head = Vec::with_capacity(10 + DATA_PREFIX_BYTES);
                encode_header(
                    &mut head,
                    OP_BINARY,
                    (DATA_PREFIX_BYTES + records_bytes) as u64,
                );
                head.extend_from_slice(&prefix);
                self.out.push_data(head, records.parts());
            }
            Event::State { sub, ver, doc } => {
                self.send_frame(ServerFrame::State {
                    sub,
                    ver,
                    doc: &doc,
                });
            }
            Event::Behind { sub, resume_from } => {
                self.send_frame(ServerFrame::Behind { sub, resume_from });
            }
            Event::Progress { sub, id } => self.send_frame(ServerFrame::Progress { sub, id }),
            Event::Error {
                reference,
                code,
                msg,
            } => {
                if matches!(
                    code,
                    ErrorCode::NotFound
                        | ErrorCode::TooManySubscriptions
                        | ErrorCode::RetentionExpired
                ) && let Some(session) = &mut self.session
                {
                    session.subs.remove(&reference);
                }
                self.send_error(reference, code, &msg);
            }
        }
    }

    /// How long until a token ends this connection's subscriptions. Revocation is by expiry
    /// (section 12), so a subscription can't outlast it.
    fn expiry_wait(&self) -> Option<Duration> {
        let session = self.session.as_ref()?;
        if session.subs.is_empty() || self.expiry_seen || self.closing {
            return None;
        }
        let now_ms = self.shared.clock.now_ms();
        Some(Duration::from_millis(
            session.token.expiry_ms.saturating_sub(now_ms),
        ))
    }

    fn on_expiry(&mut self) {
        let now_ms = self.shared.clock.now_ms();
        let Some(session) = &mut self.session else {
            return;
        };
        if now_ms < session.token.expiry_ms {
            return;
        }
        self.expiry_seen = true;
        let subs: Vec<u32> = session.subs.drain().map(|(sub, _)| sub).collect();
        for sub in subs {
            if let Some(session) = &self.session {
                session.connection.unsubscribe(sub);
            }
            self.send_error(sub, ErrorCode::Unauthorized, "token has expired");
        }
    }

    fn push_ws_frame(&mut self, opcode: u8, payload: &[u8]) {
        let (head, head_bytes) = header_array(opcode, payload.len() as u64);
        self.out.push_reply(&[&head[..head_bytes], payload]);
    }

    fn send_frame(&mut self, frame: ServerFrame<'_>) {
        let (shared, home) = (self.shared, self.home);
        let out = &mut self.out;
        with_core_io(shared, home, |io| {
            io.scratch.clear();
            if frame.encode(&mut io.scratch).is_ok() {
                let (head, head_bytes) = header_array(OP_BINARY, io.scratch.len() as u64);
                out.push_reply(&[&head[..head_bytes], &io.scratch]);
            }
        });
    }

    fn send_error(&mut self, reference: u32, code: ErrorCode, msg: &str) {
        self.send_frame(ServerFrame::Error {
            reference,
            code,
            msg,
        });
    }

    fn send_nack(&mut self, handle: u32, producer_seq: u64, code: ErrorCode, msg: &str) {
        self.send_frame(ServerFrame::Nack {
            handle,
            producer_seq,
            code,
            msg,
        });
    }

    fn protocol_error(&mut self, reason: &str) {
        self.send_error(0, ErrorCode::Malformed, reason);
        self.close(1002);
    }

    /// Says goodbye, and the loop ends once the socket has taken what is left.
    fn close(&mut self, code: u16) {
        if !self.closing {
            self.push_ws_frame(OP_CLOSE, &code.to_be_bytes());
            self.closing = true;
        }
    }
}

fn state_op_from_wire(op: proto::StateOp) -> StateOp {
    match op {
        proto::StateOp::Set => StateOp::Set,
        proto::StateOp::Merge => StateOp::Merge,
        proto::StateOp::Incr => StateOp::Incr,
    }
}
