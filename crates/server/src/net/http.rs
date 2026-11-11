//! The HTTP API (section 11.2, D21, D26), and the upgrade to WebSocket (D22).
//!
//! One request at a time on a connection, in plain async code. Between requests a connection
//! holds nothing but its TLS session (section 6.5). Only `Content-Length` bodies are read:
//! `Transfer-Encoding` is refused, so a proxy and this server can't disagree on where a request
//! ends. Results are small JSON objects, with ids as decimal strings, because an id is over
//! 2^53 and most JSON readers would round it.

use std::collections::VecDeque;
use std::io;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use format::{Clause, Filter, HEADER_BYTES, KeyHash, RecordHeader};
use proto::{DATA_PREFIX_BYTES, ErrorCode, is_valid_stream_id};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::conn::{Counted, PartRead, READ_CHUNK_BYTES, READ_CHUNK_MIN_BYTES, Tls, try_read};
use super::json::{Json, quoted};
use super::ws::accept_key;
use super::{Shared, with_core_io};
use crate::connection::{Connection, Event, Subscription};
use crate::delivery::{CURSOR_NAME_REFUSED, cursor_name_is_allowed};
use crate::keys::Keys;
use crate::message::{
    Reply, SinkOp, SinkSpec, SinkStart, StateChange, StreamConfig, StreamConfigPatch, StreamInfo,
};
use crate::node::{Client, ClientError, OwnedStateOp, RegionPublish};
use crate::sink::SINK_TEXT_MAX_BYTES;
use crate::state::StateOp;

const CONTENT_TYPE_RECORDS: &str = "application/x-sillystream-records";
/// A request's line and headers. A token alone can be 65,534 bytes (D48).
const MAX_HEAD_BYTES: usize = 128 * 1024;
/// A body this small is read and thrown away after a refusal, so the response isn't lost to a
/// reset. A bigger one ends the connection instead.
const DRAIN_BODY_MAX_BYTES: u64 = 64 * 1024;
/// A body other than a publish's: config and state docs, which have smaller limits anyway.
const SMALL_BODY_MAX_BYTES: u64 = 1024 * 1024;
/// All a request gets to arrive, and all a response gets to leave. Not per byte: a client could
/// hold its socket and whatever it has cost for as long as it could send or take one (D238).
const REQUEST_TIMEOUT_MS: u64 = 30_000;
const KEEP_ALIVE_TIMEOUT_MS: u64 = 120_000;
const READ_LIMIT_DEFAULT_COUNT: u64 = 1_000;
/// How long a read waits for records it has been told exist, past `PROGRESS`'s interval.
const READ_GRACE_MS: u64 = 2_000;
const RETRY_FULL_MS: u64 = 1;

/// Retention classes of section 7.4.
const RETENTION_CLASSES: [(&str, u64); 4] = [
    ("1d", 86_400_000),
    ("7d", 7 * 86_400_000),
    ("30d", 30 * 86_400_000),
    ("365d", 365 * 86_400_000),
];

struct Request {
    method: String,
    path: String,
    query: Vec<(String, String)>,
    headers: Vec<(String, String)>,
    content_length: u64,
    /// Body bytes that came in with the head.
    leftover: Vec<u8>,
    close: bool,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    fn query(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

#[derive(Debug)]
struct Response {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
    close: bool,
    /// A read's share of what responses may hold, let go with the body.
    held: Option<HeldResponseBytes>,
}

/// Bytes of read responses waiting on their clients, against a bound shared by every connection
/// (D238). A client's socket decides how long these stay, so they can't be left uncounted.
#[derive(Debug)]
struct HeldResponseBytes {
    total_bytes: Arc<AtomicU64>,
    cap_bytes: u64,
    held_bytes: u64,
}

impl HeldResponseBytes {
    /// False if there's no room. Nothing else held means any size fits, or a record over the
    /// bound could never be read.
    fn try_hold(&mut self, more_bytes: u64) -> bool {
        let cap_bytes = self.cap_bytes;
        let held = self
            .total_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |total_bytes| {
                (total_bytes == 0 || total_bytes.saturating_add(more_bytes) <= cap_bytes)
                    .then_some(total_bytes + more_bytes)
            })
            .is_ok();
        if held {
            self.held_bytes += more_bytes;
        }
        held
    }
}

impl Drop for HeldResponseBytes {
    fn drop(&mut self) {
        self.total_bytes
            .fetch_sub(self.held_bytes, Ordering::Relaxed);
    }
}

impl Response {
    fn json(status: u16, body: String) -> Response {
        Response {
            status,
            headers: vec![("content-type", "application/json".to_owned())],
            body: body.into_bytes(),
            close: false,
            held: None,
        }
    }

    fn binary(content_type: &str, body: Vec<u8>) -> Response {
        Response {
            status: 200,
            headers: vec![("content-type", content_type.to_owned())],
            body,
            close: false,
            held: None,
        }
    }

    fn header(mut self, name: &'static str, value: String) -> Response {
        self.headers.push((name, value));
        self
    }
}

/// An error with the code of section 11.1 in its body.
fn error(code: ErrorCode, msg: &str) -> Response {
    let status = match code {
        ErrorCode::Unauthorized => 401,
        ErrorCode::NotFound => 404,
        ErrorCode::VersionConflict => 409,
        ErrorCode::RateLimited | ErrorCode::TooManySubscriptions => 429,
        ErrorCode::FrameTooLarge => 413,
        ErrorCode::RetentionExpired => 410,
        ErrorCode::Malformed => 400,
    };
    Response::json(
        status,
        format!("{{\"code\":{},\"error\":{}}}", code.to_wire(), quoted(msg)),
    )
}

fn malformed(msg: &str) -> Response {
    error(ErrorCode::Malformed, msg)
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        410 => "Gone",
        413 => "Content Too Large",
        426 => "Upgrade Required",
        429 => "Too Many Requests",
        _ => "Error",
    }
}

/// What a path and method ask for.
#[derive(Debug, PartialEq, Eq, Clone)]
enum Route {
    Info,
    Publish,
    Read,
    StateGet,
    StatePatch,
    Cursor(String),
    Put,
    Delete,
    Core,
    SinkPut(u64),
    SinkDelete(u64),
    SinkPause(u64),
    SinkResume(u64),
    SinkReset(u64),
}

impl Route {
    /// The permission of section 12.
    fn permission(&self) -> u8 {
        match self {
            Route::Info | Route::Read | Route::StateGet | Route::Cursor(_) => auth::PERM_READ,
            Route::Publish => auth::PERM_PUBLISH,
            Route::StatePatch => auth::PERM_STATE,
            _ => auth::PERM_ADMIN,
        }
    }
}

fn percent_decode(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'%' {
            let digits = std::str::from_utf8(bytes.get(at + 1..at + 3)?).ok()?;
            out.push(u8::from_str_radix(digits, 16).ok()?);
            at += 3;
        } else {
            out.push(bytes[at]);
            at += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// `Err` is the response to give.
fn route(method: &str, path: &str) -> Result<(String, Route), Response> {
    let not_found = || error(ErrorCode::NotFound, "no such route");
    let method_wrong = || {
        let mut response = malformed("wrong method for this route");
        response.status = 405;
        response
    };
    let segments: Vec<&str> = path.split('/').collect();
    let ["", "v1", "stream", stream, rest @ ..] = segments.as_slice() else {
        return Err(not_found());
    };
    let stream = percent_decode(stream).ok_or_else(|| malformed("bad percent-encoding"))?;
    if !is_valid_stream_id(&stream) {
        return Err(malformed("stream id is empty or contains '/'"));
    }
    let sink_id = |text: &str| -> Result<u64, Response> {
        text.parse()
            .map_err(|_| malformed("a sink id is a u64 (D27)"))
    };
    let route = match rest {
        [] => match method {
            "GET" => Route::Info,
            "PUT" => Route::Put,
            "DELETE" => Route::Delete,
            _ => return Err(method_wrong()),
        },
        ["core"] => match method {
            "PUT" => Route::Core,
            _ => return Err(method_wrong()),
        },
        ["publish"] => match method {
            "POST" => Route::Publish,
            _ => return Err(method_wrong()),
        },
        ["read"] => match method {
            "GET" => Route::Read,
            _ => return Err(method_wrong()),
        },
        ["state"] => match method {
            "GET" => Route::StateGet,
            "PATCH" => Route::StatePatch,
            _ => return Err(method_wrong()),
        },
        ["cursor", name] => match method {
            "PUT" => Route::Cursor(
                percent_decode(name).ok_or_else(|| malformed("bad percent-encoding"))?,
            ),
            _ => return Err(method_wrong()),
        },
        ["sink", id] => match method {
            "PUT" => Route::SinkPut(sink_id(id)?),
            "DELETE" => Route::SinkDelete(sink_id(id)?),
            _ => return Err(method_wrong()),
        },
        ["sink", id, action] => {
            if method != "POST" {
                return Err(method_wrong());
            }
            let sink = sink_id(id)?;
            match *action {
                "pause" => Route::SinkPause(sink),
                "resume" => Route::SinkResume(sink),
                "reset" => Route::SinkReset(sink),
                _ => return Err(not_found()),
            }
        }
        _ => return Err(not_found()),
    };
    Ok((stream, route))
}

/// A u64 in decimal or `0x` hex.
fn parse_u64(text: &str) -> Option<u64> {
    let text = text.trim();
    match text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
        Some(hex) => u64::from_str_radix(hex, 16).ok(),
        None => text.parse().ok(),
    }
}

/// D26's filter text. `None` if it isn't one.
pub(crate) fn parse_filter(text: &str) -> Option<(Vec<Clause>, Option<u64>)> {
    let mut clauses = Vec::new();
    let mut author = None;
    for part in text.split(',').filter(|part| !part.is_empty()) {
        let fields: Vec<&str> = part.split(':').collect();
        match fields.as_slice() {
            ["eq", mask, value] => clauses.push(Clause::Eq {
                mask: parse_u64(mask)?,
                value: parse_u64(value)?,
            }),
            ["any", mask] => clauses.push(Clause::Any {
                mask: parse_u64(mask)?,
            }),
            ["author", id] => author = Some(parse_u64(id)?),
            _ => return None,
        }
    }
    Filter::new(&clauses).ok()?;
    Some((clauses, author))
}

fn find_head_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn parse_request(head: &[u8], leftover: Vec<u8>) -> Result<Request, Response> {
    let bad = || malformed("the request can't be parsed");
    let text = std::str::from_utf8(head).map_err(|_| bad())?;
    let mut lines = text.split("\r\n");
    // A bare CR or LF would end a line for some other parser, and hide a header from the
    // framing check below.
    if lines.clone().any(|line| line.contains(['\r', '\n'])) {
        return Err(bad());
    }
    let mut request_line = lines.next().ok_or_else(bad)?.split(' ');
    let (Some(method), Some(target), Some(version), None) = (
        request_line.next(),
        request_line.next(),
        request_line.next(),
        request_line.next(),
    ) else {
        return Err(bad());
    };
    if version != "HTTP/1.1" && version != "HTTP/1.0" {
        return Err(bad());
    }
    let (path, query_text) = target.split_once('?').unwrap_or((target, ""));
    let mut query = Vec::new();
    for pair in query_text.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        query.push((
            percent_decode(key).ok_or_else(bad)?,
            percent_decode(value).ok_or_else(bad)?,
        ));
    }
    let mut headers = Vec::new();
    for line in lines {
        let (name, value) = line.split_once(':').ok_or_else(bad)?;
        if name.is_empty() || name.ends_with(' ') || name.starts_with([' ', '\t']) {
            return Err(bad());
        }
        headers.push((name.to_ascii_lowercase(), value.trim().to_owned()));
    }
    if headers.iter().any(|(name, _)| name == "transfer-encoding") {
        return Err(malformed("only Content-Length bodies are read"));
    }
    let mut lengths = headers.iter().filter(|(name, _)| name == "content-length");
    let content_length = match lengths.next() {
        // `u64::from_str` also takes a leading `+`, which RFC 9110 doesn't.
        Some((_, value)) if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) => {
            value.parse().map_err(|_| bad())?
        }
        Some(_) => return Err(bad()),
        None => 0,
    };
    if lengths.next().is_some() {
        return Err(bad());
    }
    let connection = headers
        .iter()
        .find(|(name, _)| name == "connection")
        .map(|(_, value)| value.to_ascii_lowercase())
        .unwrap_or_default();
    let close = connection.contains("close")
        || (version == "HTTP/1.0" && !connection.contains("keep-alive"));
    Ok(Request {
        method: method.to_owned(),
        path: path.to_owned(),
        query,
        headers,
        content_length,
        leftover,
        close,
    })
}

pub(super) struct HttpConn<'a> {
    shared: &'a Shared,
    home: usize,
    tls: Tls,
    /// Made on the first request that needs one.
    client: Option<Client>,
    /// Bytes behind the last request, which belong to the next.
    next_bytes: Vec<u8>,
    next_number: u32,
}

impl<'a> HttpConn<'a> {
    pub(super) fn new(shared: &'a Shared, home: usize, tls: Tls) -> HttpConn<'a> {
        HttpConn {
            shared,
            home,
            tls,
            client: None,
            next_bytes: Vec::new(),
            next_number: 0,
        }
    }

    /// Serves requests until the connection ends, or returns the socket after an upgrade,
    /// with what arrived behind it.
    pub(super) async fn run(mut self) -> Option<(Tls, Vec<u8>)> {
        loop {
            let (head, leftover) = match self.read_head().await {
                Ok(Some(read)) => read,
                Ok(None) | Err(_) => return None,
            };
            let request = match parse_request(&head, leftover) {
                Ok(request) => request,
                Err(mut response) => {
                    response.close = true;
                    let _ = self.write_response(response).await;
                    return None;
                }
            };
            drop(head);
            if request.path == "/v1/ws" {
                return self.upgrade(request).await;
            }
            let close = request.close;
            match self.handle(request).await {
                Ok(response) => {
                    let close = close || response.close;
                    if self.write_response(response).await.is_err() || close {
                        let _ =
                            tokio::time::timeout(Duration::from_millis(1_000), self.tls.shutdown())
                                .await;
                        return None;
                    }
                }
                Err(_) => return None,
            }
        }
    }

    /// The head and whatever came behind it. `None` at a clean end.
    async fn read_head(&mut self) -> io::Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut head = std::mem::take(&mut self.next_bytes);
        let mut held = None;
        // Neither wait starts again when bytes come, nor when part of a TLS record does, which
        // adds nothing to the head.
        let mut request_begun = !head.is_empty();
        let mut deadline = Instant::now()
            + Duration::from_millis(if request_begun {
                REQUEST_TIMEOUT_MS
            } else {
                KEEP_ALIVE_TIMEOUT_MS
            });
        loop {
            if let Some(end) = find_head_end(&head) {
                let leftover = head.split_off(end + 4);
                head.truncate(end);
                return Ok(Some((head, leftover)));
            }
            if head.len() > MAX_HEAD_BYTES {
                let mut response = malformed("the request head is too long");
                response.close = true;
                let _ = self.write_response(response).await;
                return Ok(None);
            }
            loop {
                let (shared, home) = (self.shared, self.home);
                let tls = &mut self.tls;
                let read = with_core_io(shared, home, |io| {
                    let mut chunk = io.pool.borrow_chunk(READ_CHUNK_MIN_BYTES, READ_CHUNK_BYTES);
                    shared.counters[home]
                        .read_borrow_count
                        .fetch_add(1, Ordering::Relaxed);
                    let result = try_read(tls, &mut chunk);
                    if let Some(Ok(read_bytes)) = result {
                        head.extend_from_slice(&chunk[..read_bytes]);
                    }
                    io.pool.give_back(chunk);
                    result
                });
                match read {
                    None => break,
                    Some(Err(_)) | Some(Ok(0)) => return Ok(None),
                    Some(Ok(_)) => {
                        if find_head_end(&head).is_some() || head.len() > MAX_HEAD_BYTES {
                            break;
                        }
                    }
                }
            }
            if find_head_end(&head).is_some() || head.len() > MAX_HEAD_BYTES {
                continue;
            }
            // With nothing of a request here yet, this is an idle connection.
            if !head.is_empty() {
                held.get_or_insert_with(|| {
                    Counted::new(&self.shared.counters[self.home].held_buffer_count)
                });
                if !request_begun {
                    request_begun = true;
                    deadline = Instant::now() + Duration::from_millis(REQUEST_TIMEOUT_MS);
                }
            }
            // Only now: rustls may hold plaintext that came with the bytes it already took, and
            // the socket won't say so.
            let tcp = self.tls.get_ref().0;
            match tokio::time::timeout_at(deadline.into(), tcp.readable()).await {
                Err(_) => return Ok(None),
                Ok(ready) => ready?,
            }
        }
    }

    async fn write_response(&mut self, response: Response) -> io::Result<()> {
        let mut head = format!(
            "HTTP/1.1 {} {}\r\ncontent-length: {}\r\n",
            response.status,
            reason(response.status),
            response.body.len()
        );
        for (name, value) in &response.headers {
            head.push_str(name);
            head.push_str(": ");
            head.push_str(value);
            head.push_str("\r\n");
        }
        if response.close {
            head.push_str("connection: close\r\n");
        }
        head.push_str("\r\n");
        let tls = &mut self.tls;
        write_within_limit(async {
            tls.write_all(head.as_bytes()).await?;
            tls.write_all(&response.body).await?;
            tls.flush().await
        })
        .await
    }

    async fn upgrade(mut self, request: Request) -> Option<(Tls, Vec<u8>)> {
        let upgrade = request
            .header("upgrade")
            .is_some_and(|value| value.eq_ignore_ascii_case("websocket"));
        let connection = request
            .header("connection")
            .is_some_and(|value| value.to_ascii_lowercase().contains("upgrade"));
        let version = request.header("sec-websocket-version") == Some("13");
        let key = request.header("sec-websocket-key");
        let (true, true, true, "GET", Some(key)) =
            (upgrade, connection, version, request.method.as_str(), key)
        else {
            let mut response = malformed("this route is a WebSocket upgrade");
            response.status = 426;
            response.close = true;
            let _ = self.write_response(response).await;
            return None;
        };
        let accepted = format!(
            "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\n\
             sec-websocket-accept: {}\r\n\r\n",
            accept_key(key)
        );
        let tls = &mut self.tls;
        write_within_limit(async {
            tls.write_all(accepted.as_bytes()).await?;
            tls.flush().await
        })
        .await
        .ok()?;
        Some((self.tls, request.leftover))
    }

    fn client(&mut self) -> &mut Client {
        let (shared, home) = (self.shared, self.home);
        self.client.get_or_insert_with(|| shared.node.client(home))
    }

    fn number(&mut self) -> u32 {
        self.next_number = self.next_number.wrapping_add(1);
        self.next_number
    }

    /// Answers one request. An `Err` ends the connection, with no response.
    /// The most records a publish can carry. They leave in a `DATA` frame, and `max_frame` covers
    /// the whole of that (D43).
    fn max_records_bytes(&self) -> u64 {
        self.shared
            .max_frame_bytes
            .saturating_sub(DATA_PREFIX_BYTES as u64)
    }

    async fn handle(&mut self, mut request: Request) -> io::Result<Response> {
        let (stream, route) = match route(&request.method, &request.path) {
            Ok(routed) => routed,
            Err(response) => return self.refuse(&mut request, response).await,
        };
        // Nothing about a stream is said, or read, for a caller who can't use it.
        let now_ms = self.shared.clock.now_ms();
        let token = request
            .header("authorization")
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(|token| auth::verify(&self.shared.secret, token.trim(), now_ms));
        let token = match token {
            None => {
                let response = error(ErrorCode::Unauthorized, "a Bearer token is required");
                return self.refuse(&mut request, response).await;
            }
            Some(Err(failure)) => {
                let response = error(ErrorCode::Unauthorized, &failure.to_string());
                return self.refuse(&mut request, response).await;
            }
            Some(Ok(token)) => token,
        };
        if let Err(failure) = token.check(&stream, route.permission(), now_ms) {
            let mut response = error(ErrorCode::Unauthorized, &failure.to_string());
            response.status = 403;
            return self.refuse(&mut request, response).await;
        }
        // A sink hands the stream's records to its destination, so making one takes what reading
        // them takes (D235). Read and admin may come from different grants.
        if matches!(route, Route::SinkPut(_))
            && let Err(failure) = token.check(&stream, auth::PERM_READ, now_ms)
        {
            let mut response = error(ErrorCode::Unauthorized, &failure.to_string());
            response.status = 403;
            return self.refuse(&mut request, response).await;
        }
        let key = self.shared.keys.hash(&stream);
        let limit_bytes = if route == Route::Publish {
            self.max_records_bytes()
        } else {
            SMALL_BODY_MAX_BYTES
        };
        if request.content_length > limit_bytes {
            let response = error(ErrorCode::FrameTooLarge, "the body is too large");
            return self.refuse(&mut request, response).await;
        }
        self.say_continue(&request).await?;
        let body = if route == Route::Publish {
            Vec::new()
        } else {
            self.read_small_body(&mut request).await?
        };
        let shape = match route {
            Route::Info => {
                let is_admin = token.check(&stream, auth::PERM_ADMIN, now_ms).is_ok();
                self.info(&stream, key, is_admin).await
            }
            Route::Publish => {
                return self.publish(key, &mut request, token.author).await;
            }
            Route::Read => self.read(key, &request).await,
            Route::StateGet => self.state_get(key).await,
            Route::StatePatch => self.state_patch(key, &request, body, token.author).await,
            Route::Cursor(name) => self.cursor(key, &name, &request, &body).await,
            Route::Put => self.stream_put(key, &body).await,
            Route::Core => self.stream_core(key, &body).await,
            Route::Delete => {
                if self.client().delete_stream(key).await {
                    Response::json(200, "{\"deleted\":true}".to_owned())
                } else {
                    error(ErrorCode::NotFound, "no such stream")
                }
            }
            Route::SinkPut(sink_id) => match forward_target(&body, &self.shared.keys) {
                // The sink will write there, as a publish would (section 12).
                Some(target)
                    if let Err(failure) = token.check(&target, auth::PERM_PUBLISH, now_ms) =>
                {
                    let mut response = error(ErrorCode::Unauthorized, &failure.to_string());
                    response.status = 403;
                    response
                }
                _ => self.sink_put(key, sink_id, &body).await,
            },
            Route::SinkDelete(sink_id) => self.sink(key, SinkOp::Delete { sink_id }).await,
            Route::SinkPause(sink_id) => {
                self.sink(
                    key,
                    SinkOp::SetPaused {
                        sink_id,
                        paused: true,
                    },
                )
                .await
            }
            Route::SinkResume(sink_id) => {
                self.sink(
                    key,
                    SinkOp::SetPaused {
                        sink_id,
                        paused: false,
                    },
                )
                .await
            }
            Route::SinkReset(sink_id) => match parse_sink_reset(&body) {
                Ok(start) => self.sink(key, SinkOp::Reset { sink_id, start }).await,
                Err(response) => response,
            },
        };
        Ok(shape)
    }

    /// Answers without having read the body. A small one is read and dropped, so that the
    /// response isn't lost when the socket closes with bytes unread. Otherwise this closes.
    async fn refuse(
        &mut self,
        request: &mut Request,
        mut response: Response,
    ) -> io::Result<Response> {
        let left = request
            .content_length
            .saturating_sub(request.leftover.len() as u64);
        if request.content_length > DRAIN_BODY_MAX_BYTES {
            response.close = true;
            return Ok(response);
        }
        self.say_continue(request).await?;
        self.next_bytes = request
            .leftover
            .split_off((request.content_length as usize).min(request.leftover.len()));
        let mut drop_buffer = vec![0u8; left as usize];
        self.read_exact(&mut drop_buffer).await?;
        Ok(response)
    }

    async fn say_continue(&mut self, request: &Request) -> io::Result<()> {
        let wants = request
            .header("expect")
            .is_some_and(|value| value.eq_ignore_ascii_case("100-continue"));
        if wants && (request.leftover.len() as u64) < request.content_length {
            let tls = &mut self.tls;
            write_within_limit(async {
                tls.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await?;
                tls.flush().await
            })
            .await?;
        }
        Ok(())
    }

    async fn read_exact(&mut self, buffer: &mut [u8]) -> io::Result<()> {
        let read = tokio::time::timeout(
            Duration::from_millis(REQUEST_TIMEOUT_MS),
            self.tls.read_exact(buffer),
        )
        .await;
        match read {
            Ok(result) => result.map(|_| ()),
            Err(_) => Err(io::ErrorKind::TimedOut.into()),
        }
    }

    async fn read_small_body(&mut self, request: &mut Request) -> io::Result<Vec<u8>> {
        let length = request.content_length as usize;
        let mut body = std::mem::take(&mut request.leftover);
        if body.len() > length {
            self.next_bytes = body.split_off(length);
        }
        let have = body.len();
        body.resize(length, 0);
        self.read_exact(&mut body[have..]).await?;
        Ok(body)
    }

    /// Reads a publish's body, then moves it to a slab behind `prefix_bytes` left for a header.
    /// A `Content-Length` only claims a length, so nothing counts against the memory cap until
    /// the bytes have really come (D130).
    async fn read_body_into_slab(
        &mut self,
        request: &mut Request,
        prefix_bytes: usize,
        suffix_bytes: usize,
    ) -> io::Result<crate::slab::SlabRegion> {
        let length = request.content_length as usize;
        let have = request.leftover.len().min(length);
        let mut part = PartRead::new(length, &request.leftover[..have]);
        self.next_bytes = request.leftover.split_off(have);
        let _held = Counted::new(&self.shared.counters[self.home].held_buffer_count);
        let tls = &mut self.tls;
        let read = tokio::time::timeout(Duration::from_millis(REQUEST_TIMEOUT_MS), async {
            while !part.is_whole() {
                match tls.read(part.room()).await? {
                    0 => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
                    read_bytes => part.filled(read_bytes),
                }
            }
            Ok(())
        })
        .await;
        match read {
            Ok(result) => result?,
            Err(_) => return Err(io::ErrorKind::TimedOut.into()),
        }
        let mut region = with_core_io(self.shared, self.home, |io| {
            io.pool.alloc(prefix_bytes + length + suffix_bytes)
        });
        region[prefix_bytes..prefix_bytes + length].copy_from_slice(part.payload());
        // A slab's bytes may have been read into before, and padding has to be zero.
        region[prefix_bytes + length..].fill(0);
        Ok(region)
    }

    async fn publish(
        &mut self,
        stream: KeyHash,
        request: &mut Request,
        author: NonZeroU64,
    ) -> io::Result<Response> {
        let shape = publish_shape(request);
        let shape = match shape {
            Ok(shape) => shape,
            Err(response) => return self.refuse(request, response).await,
        };
        let ack_level = match shape.ack_level {
            // The owner raises it if it has to, and this waits for whatever that brings.
            Some(level @ 1..) => level,
            // Level 0 sends nothing, so waiting needs to know before it starts. A minimum set
            // after this look still raises the commit, but not this answer (D223).
            asked => {
                let config = self
                    .client()
                    .stream_info(stream)
                    .await
                    .map(|info| info.config)
                    .unwrap_or_default();
                asked.map_or_else(
                    || effective_default(&config),
                    |level| level.max(config.min_ack_level).min(3),
                )
            }
        };
        let mut records = if shape.simple {
            let length = request.content_length as usize;
            let padded_bytes = length.next_multiple_of(8);
            let total_bytes = (HEADER_BYTES + padded_bytes) as u64;
            if total_bytes > self.max_records_bytes() {
                let response = error(ErrorCode::FrameTooLarge, "the record is too large");
                return self.refuse(request, response).await;
            }
            let mut region = self
                .read_body_into_slab(request, HEADER_BYTES, padded_bytes - length)
                .await?;
            let header = RecordHeader {
                id: 0,
                options: shape.options,
                author: 0,
                len: length as u32,
                count: 1,
                codec: shape.codec,
            };
            region[..HEADER_BYTES].copy_from_slice(&header.to_bytes());
            region
        } else {
            self.read_body_into_slab(request, 0, 0).await?
        };
        // Nothing will come back to answer with, so what the owner would refuse is refused here,
        // or the caller is told a publish was accepted that commits nothing (D133).
        if ack_level == 0
            && let Err(msg) = crate::core::check_publish(&mut records, None)
        {
            return Ok(error(ErrorCode::Malformed, &msg));
        }
        let handle = self.number();
        let mut publish = RegionPublish {
            stream,
            handle,
            producer_id: shape.producer_id,
            producer_seq: shape.producer_seq,
            ack_level: Some(ack_level),
            author,
            state: None,
            records,
        };
        let client = self.client();
        loop {
            match client.try_publish_region(publish) {
                Ok(()) => break,
                Err((ClientError::Full, back)) => {
                    publish = back;
                    tokio::time::sleep(Duration::from_millis(RETRY_FULL_MS)).await;
                }
                Err((ClientError::NodeStopped, _)) => {
                    return Ok(error(ErrorCode::RateLimited, "the node has stopped"));
                }
            }
        }
        if ack_level == 0 {
            return Ok(Response::json(202, "{\"accepted\":true}".to_owned()));
        }
        loop {
            let Some(reply) = client.recv().await else {
                return Ok(error(ErrorCode::RateLimited, "the node has stopped"));
            };
            match reply {
                Reply::Ack {
                    handle: replied,
                    first_id,
                    count,
                    producer_seq,
                } if replied == handle => {
                    return Ok(Response::json(
                        200,
                        format!(
                            "{{\"first_id\":\"{first_id}\",\"count\":{count},\
                             \"producer_seq\":\"{producer_seq}\"}}"
                        ),
                    ));
                }
                Reply::Nack {
                    handle: replied,
                    code,
                    msg,
                    ..
                } if replied == handle => return Ok(error(code, &msg)),
                Reply::Error {
                    reference,
                    code,
                    msg,
                } if reference == handle => return Ok(error(code, &msg)),
                // What an earlier level 0 publish or request left behind.
                _ => {}
            }
        }
    }

    async fn info(&mut self, name: &str, stream: KeyHash, is_admin: bool) -> Response {
        match self.client().stream_info(stream).await {
            Err(code) => error(code, "no such stream"),
            Ok(info) => Response::json(200, info_json(name, &info, is_admin)),
        }
    }

    async fn state_get(&mut self, stream: KeyHash) -> Response {
        match self.client().snapshot(stream).await {
            Err(code) => error(code, "no such stream"),
            Ok(snapshot) => Response::binary("application/cbor", snapshot.state.to_vec())
                .header("etag", format!("\"{}\"", snapshot.state_ver))
                .header("x-state-ver", snapshot.state_ver.to_string())
                .header("x-head", snapshot.head.to_string()),
        }
    }

    async fn state_patch(
        &mut self,
        stream: KeyHash,
        request: &Request,
        body: Vec<u8>,
        author: NonZeroU64,
    ) -> Response {
        let op = match request.header("x-state-op").unwrap_or("merge") {
            "set" | "1" => StateOp::Set,
            "merge" | "2" => StateOp::Merge,
            "incr" | "3" => StateOp::Incr,
            _ => return malformed("X-State-Op is set, merge or incr"),
        };
        let expect_ver = match request.header("if-match") {
            // `If-None-Match: *` is a create: only if the state was never set (section 10).
            None if request.header("if-none-match") == Some("*") => u64::MAX,
            None | Some("*") => 0,
            Some(value) => match value.trim_matches('"').parse() {
                Ok(version) => version,
                Err(_) => return malformed("If-Match is a state version"),
            },
        };
        let req = self.number();
        let mut state_op = OwnedStateOp {
            stream,
            req,
            author,
            change: StateChange {
                expect_ver,
                op,
                doc: Bytes::from(body),
            },
        };
        let client = self.client();
        loop {
            match client.try_state_op(state_op) {
                Ok(()) => break,
                Err((ClientError::Full, back)) => {
                    state_op = back;
                    tokio::time::sleep(Duration::from_millis(RETRY_FULL_MS)).await;
                }
                Err((ClientError::NodeStopped, _)) => {
                    return error(ErrorCode::RateLimited, "the node has stopped");
                }
            }
        }
        loop {
            let Some(reply) = client.recv().await else {
                return error(ErrorCode::RateLimited, "the node has stopped");
            };
            match reply {
                Reply::StateResult {
                    req: replied,
                    ok,
                    ver,
                } if replied == req => {
                    return if ok {
                        Response::json(200, format!("{{\"ok\":true,\"ver\":\"{ver}\"}}"))
                    } else {
                        let code = ErrorCode::VersionConflict.to_wire();
                        Response::json(
                            409,
                            format!(
                                "{{\"code\":{code},\"error\":\"version conflict\",\
                                 \"ok\":false,\"ver\":\"{ver}\"}}"
                            ),
                        )
                    };
                }
                Reply::Error {
                    reference,
                    code,
                    msg,
                } if reference == req => return error(code, &msg),
                _ => {}
            }
        }
    }

    async fn cursor(
        &mut self,
        stream: KeyHash,
        name: &str,
        request: &Request,
        body: &[u8],
    ) -> Response {
        if !cursor_name_is_allowed(name) {
            return malformed(CURSOR_NAME_REFUSED);
        }
        let text = std::str::from_utf8(body).unwrap_or("").trim();
        let id = match Json::parse(body) {
            Some(Json::Object(fields)) => fields
                .iter()
                .find(|(key, _)| key == "id")
                .and_then(|(_, value)| value.as_u64()),
            _ => text.parse().ok(),
        };
        let Some(id) = id else {
            return malformed("the body is an id, in decimal or as {\"id\": ...}");
        };
        let reset = matches!(request.query("reset"), Some("true") | Some("1"));
        let name = self.shared.keys.hash(name);
        match self.client().put_cursor(stream, name, id, reset).await {
            Ok(at) => Response::json(200, format!("{{\"id\":\"{at}\"}}")),
            Err(ErrorCode::NotFound) => error(ErrorCode::NotFound, "no such stream"),
            Err(ErrorCode::TooManySubscriptions) => error(
                ErrorCode::TooManySubscriptions,
                "this stream has too many named cursors",
            ),
            Err(code) => error(code, "a commit can't be above the stream's head"),
        }
    }

    /// Gives a stream its own shard on a core, or moves the one it has (section 6.1, D249).
    async fn stream_core(&mut self, stream: KeyHash, body: &[u8]) -> Response {
        let fields = match object_fields(body) {
            Ok(fields) => fields,
            Err(response) => return response,
        };
        let core = match fields.as_slice() {
            [(name, value)] if name == "core" => value.as_u64(),
            _ => None,
        };
        let core_count = self.shared.node.core_count();
        let Some(core) = core
            .and_then(|core| usize::try_from(core).ok())
            .filter(|core| *core < core_count)
        else {
            return malformed(&format!(
                "the body is {{\"core\": N}}, with N below {core_count}"
            ));
        };
        if let Err(code) = self.client().stream_info(stream).await {
            return error(code, "no such stream");
        }
        match self.shared.node.set_override(stream, core).await {
            Some(shard) => Response::json(200, format!("{{\"shard\":{shard},\"core\":{core}}}")),
            None => error(ErrorCode::RateLimited, "the override table is full"),
        }
    }

    async fn stream_put(&mut self, stream: KeyHash, body: &[u8]) -> Response {
        let patch = match parse_config(body) {
            Ok(patch) => patch,
            Err(response) => return response,
        };
        match self.client().configure_patch(stream, patch).await {
            Some(config) => Response::json(200, config_json(&config)),
            None => error(ErrorCode::RateLimited, "the node has stopped"),
        }
    }

    async fn sink_put(&mut self, stream: KeyHash, sink_id: u64, body: &[u8]) -> Response {
        match parse_sink(body, &self.shared.keys) {
            Ok((spec, start, _)) => {
                self.sink(
                    stream,
                    SinkOp::Put {
                        sink_id,
                        spec: Box::new(spec),
                        start,
                    },
                )
                .await
            }
            Err(response) => response,
        }
    }

    async fn sink(&mut self, stream: KeyHash, op: SinkOp) -> Response {
        match self.client().sink_op(stream, op).await {
            Ok(()) => Response::json(200, "{\"ok\":true}".to_owned()),
            Err(code) if code == ErrorCode::Malformed => error(
                code,
                "a forward needs a non-zero id and a target other than its source, and a start \
                 can't be above the stream's head",
            ),
            Err(code) if code == ErrorCode::TooManySubscriptions => {
                error(code, "the stream holds as many sinks as it can")
            }
            Err(code) => error(code, "no such stream or sink"),
        }
    }

    /// A read is a subscription that ends: catch-up already goes through memory, the local
    /// disk and the object store, and through the filter (section 5.1).
    async fn read(&mut self, stream: KeyHash, request: &Request) -> Response {
        let from = match request.query("from").map(parse_u64) {
            None => 0,
            Some(Some(from)) => from,
            Some(None) => return malformed("from is a u64"),
        };
        let limit_count = match request.query("limit").map(parse_u64) {
            None => READ_LIMIT_DEFAULT_COUNT,
            Some(Some(limit)) if limit > 0 => limit,
            Some(_) => return malformed("limit is a u64 above 0"),
        };
        let (clauses, author) = match request.query("filter").map(parse_filter) {
            None => (Vec::new(), None),
            Some(Some(filter)) => filter,
            Some(None) => return malformed("filter isn't valid (D26)"),
        };
        let Some(mut connection) = self.shared.node.connection(self.home).await else {
            return error(ErrorCode::RateLimited, "the node has stopped");
        };
        let mut subscription = Subscription::new(1, stream, from);
        subscription.clauses = &clauses;
        subscription.author = author;
        connection.subscribe(subscription);
        let max_bytes = self.shared.max_frame_bytes as usize;
        collect_read(
            &mut connection,
            from,
            limit_count,
            max_bytes,
            self.shared.progress_interval_ms + READ_GRACE_MS,
            HeldResponseBytes {
                total_bytes: Arc::clone(&self.shared.held_response_bytes),
                cap_bytes: self.shared.held_response_cap_bytes,
                held_bytes: 0,
            },
        )
        .await
    }
}

async fn write_within_limit(write: impl Future<Output = io::Result<()>>) -> io::Result<()> {
    match tokio::time::timeout(Duration::from_millis(REQUEST_TIMEOUT_MS), write).await {
        Ok(result) => result,
        Err(_) => Err(io::ErrorKind::TimedOut.into()),
    }
}

fn effective_default(config: &StreamConfig) -> u8 {
    config.default_ack_level.max(config.min_ack_level).min(3)
}

struct PublishShape {
    simple: bool,
    options: u64,
    codec: u16,
    producer_id: u64,
    producer_seq: u64,
    ack_level: Option<u8>,
}

fn publish_shape(request: &Request) -> Result<PublishShape, Response> {
    let content_type = request
        .header("content-type")
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .unwrap_or("");
    let simple = !content_type.eq_ignore_ascii_case(CONTENT_TYPE_RECORDS);
    let number = |name: &str, what: &str| -> Result<u64, Response> {
        match request.header(name) {
            None => Ok(0),
            Some(value) => parse_u64(value).ok_or_else(|| malformed(what)),
        }
    };
    let options = number("x-options", "X-Options is a u64, in decimal or 0x hex")?;
    let codec = number("x-codec", "X-Codec is a u16")?;
    let codec = u16::try_from(codec).map_err(|_| malformed("X-Codec is a u16"))?;
    let producer_id = number("x-producer-id", "X-Producer-Id is a u64")?;
    let producer_seq = number("x-producer-seq", "X-Producer-Seq is a u64")?;
    if (request.header("x-producer-id").is_some()) != (request.header("x-producer-seq").is_some()) {
        return Err(malformed("X-Producer-Id and X-Producer-Seq go together"));
    }
    let ack_level = match request.query("ack") {
        None => None,
        Some(text) => match text.parse::<u8>() {
            Ok(level) if level <= 3 => Some(level),
            _ => return Err(malformed("ack is a level from 0 to 3")),
        },
    };
    Ok(PublishShape {
        simple,
        options,
        codec,
        producer_id,
        producer_seq,
        ack_level,
    })
}

async fn collect_read(
    connection: &mut Connection,
    from: u64,
    limit_count: u64,
    max_bytes: usize,
    wait_ms: u64,
    mut held: HeldResponseBytes,
) -> Response {
    let (head, caught_up) = loop {
        match connection.recv_within(wait_ms).await {
            Some(Event::Subscribed {
                head, caught_up, ..
            }) => break (head, caught_up),
            Some(Event::Error { code, msg, .. }) => return error(code, &msg),
            Some(_) => {}
            None => return error(ErrorCode::RateLimited, "the node isn't answering"),
        }
    };
    let mut out = Vec::new();
    // Where a read from `x-through` carries on: with nothing read, where this one started, since
    // 0 would mean the start of retention (D247).
    let mut through = if from == u64::MAX { head } else { from };
    let mut got_count = 0;
    let mut pending: VecDeque<Vec<u8>> = VecDeque::new();
    if from != u64::MAX && from < head && !caught_up {
        'read: loop {
            match connection.recv_within(wait_ms).await {
                Some(Event::Data { records, .. }) => pending.push_back(records.to_vec()),
                Some(Event::Progress { id, .. }) => {
                    through = through.max(id);
                    if id >= head {
                        break 'read;
                    }
                }
                Some(Event::Error { code, msg, .. }) => {
                    if out.is_empty() {
                        return error(code, &msg);
                    }
                    break 'read;
                }
                // Nothing came in the wait, and what is here is a whole prefix.
                None => break 'read,
                Some(_) => {}
            }
            while let Some(frame) = pending.pop_front() {
                let mut at = 0;
                for record in format::Records::new(&frame) {
                    let Ok(record) = record else {
                        return error(ErrorCode::Malformed, "a stored record doesn't parse");
                    };
                    let bytes = &frame[at..at + record.header.total_bytes()];
                    at += bytes.len();
                    if got_count >= limit_count || out.len() + bytes.len() > max_bytes.max(1) {
                        break 'read;
                    }
                    if !held.try_hold(bytes.len() as u64) {
                        // What is here is a whole prefix, and `x-through` says where to go on.
                        if out.is_empty() {
                            return error(
                                ErrorCode::RateLimited,
                                "too many read responses are waiting on their clients",
                            );
                        }
                        break 'read;
                    }
                    out.extend_from_slice(bytes);
                    got_count += 1;
                    through = through.max(record.header.last_id());
                    if through >= head {
                        break 'read;
                    }
                }
            }
        }
    }
    let mut response = Response::binary(CONTENT_TYPE_RECORDS, out)
        .header("x-head", head.to_string())
        .header("x-through", through.to_string())
        .header("x-count", got_count.to_string());
    response.held = Some(held);
    response
}

fn retention_name(retention_ms: u64) -> String {
    RETENTION_CLASSES
        .iter()
        .find(|(_, class_ms)| *class_ms == retention_ms)
        .map_or_else(
            || format!("{retention_ms}ms"),
            |(name, _)| (*name).to_owned(),
        )
}

fn config_json(config: &StreamConfig) -> String {
    format!(
        "{{\"retention\":{},\"default_ack\":{},\"min_ack\":{}}}",
        quoted(&retention_name(config.retention_ms)),
        config.default_ack_level,
        config.min_ack_level
    )
}

/// How a sink is doing. Its secret is never shown, and neither is its URL, which often holds one.
/// `pause_reason` and `last_error` are raw error text, which can name the destination's host and
/// address, so only `admin` sees them, like the URL (D188, D240).
fn sink_status_json(spec: &SinkSpec, is_admin: bool) -> String {
    let text = |value: &Option<String>| match value.as_deref() {
        Some(_) if !is_admin => "\"hidden\"".to_owned(),
        value => value.map_or("null".to_owned(), quoted),
    };
    let last_gap = spec.last_gap.map_or("null".to_owned(), |gap| {
        format!(
            "{{\"first_id\":\"{}\",\"last_id\":\"{}\"}}",
            gap.first_id, gap.last_id
        )
    });
    format!(
        "{{\"pause_reason\":{},\"failure_count\":{},\"last_error\":{},\"expiry_warning_id\":{},\
         \"gap_count\":{},\"last_gap\":{}}}",
        text(&spec.pause_reason),
        spec.failure_count,
        text(&spec.last_error),
        spec.expiry_warning_id
            .map_or("null".to_owned(), |id| format!("\"{id}\"")),
        spec.gap_count,
        last_gap
    )
}

fn info_json(stream: &str, info: &StreamInfo, is_admin: bool) -> String {
    let mut sinks = String::new();
    for (id, spec, cursor) in &info.sinks {
        if !sinks.is_empty() {
            sinks.push(',');
        }
        sinks.push_str(&format!(
            "{{\"id\":\"{id}\",\"kind\":{},\"paused\":{},\"cursor\":\"{}\",\"status\":{}}}",
            quoted(&spec.kind),
            spec.paused,
            cursor.unwrap_or(0),
            sink_status_json(spec, is_admin)
        ));
    }
    format!(
        "{{\"stream\":{},\"head\":\"{}\",\"state_ver\":\"{}\",\"retention\":{},\
         \"default_ack\":{},\"min_ack\":{},\"sinks\":[{sinks}]}}",
        quoted(stream),
        info.head,
        info.state_ver,
        quoted(&retention_name(info.config.retention_ms)),
        info.config.default_ack_level,
        info.config.min_ack_level
    )
}

fn object_fields(body: &[u8]) -> Result<Vec<(String, Json)>, Response> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(Vec::new());
    }
    match Json::parse(body) {
        Some(Json::Object(fields)) => Ok(fields),
        _ => Err(malformed("the body is a JSON object")),
    }
}

fn parse_config(body: &[u8]) -> Result<StreamConfigPatch, Response> {
    let mut patch = StreamConfigPatch::default();
    let level = |value: &Json, name: &str| -> Result<u8, Response> {
        match value.as_u64() {
            Some(level) if level <= 3 => Ok(level as u8),
            _ => Err(malformed(&format!("{name} is a level from 0 to 3"))),
        }
    };
    for (key, value) in object_fields(body)? {
        match key.as_str() {
            "retention" => {
                let class = value
                    .as_str()
                    .and_then(|name| RETENTION_CLASSES.iter().find(|(known, _)| *known == name));
                let Some((_, class_ms)) = class else {
                    return Err(malformed("retention is 1d, 7d, 30d or 365d"));
                };
                patch.retention_ms = Some(*class_ms);
            }
            "default_ack" => patch.default_ack_level = Some(level(&value, "default_ack")?),
            "min_ack" => patch.min_ack_level = Some(level(&value, "min_ack")?),
            _ => return Err(malformed("unknown config field")),
        }
    }
    Ok(patch)
}

/// A forward's target, if the body is a sink that has one. Anything malformed is left for
/// `parse_sink` to answer.
fn forward_target(body: &[u8], keys: &Keys) -> Option<String> {
    let (spec, _, target) = parse_sink(body, keys).ok()?;
    if spec.kind == "forward" { target } else { None }
}

fn parse_start(value: &Json) -> Result<SinkStart, Response> {
    match value.as_str() {
        Some("now") => Ok(SinkStart::Now),
        Some("earliest") => Ok(SinkStart::Earliest),
        _ => value
            .as_u64()
            .map(SinkStart::Id)
            .ok_or_else(|| malformed("start is now, earliest or an id")),
    }
}

fn parse_sink_reset(body: &[u8]) -> Result<SinkStart, Response> {
    let mut start = SinkStart::Now;
    for (key, value) in object_fields(body)? {
        match key.as_str() {
            "start" => start = parse_start(&value)?,
            _ => return Err(malformed("unknown field")),
        }
    }
    Ok(start)
}

/// The spec and start, and a forward's target as named, which the token is checked against.
fn parse_sink(body: &[u8], keys: &Keys) -> Result<(SinkSpec, SinkStart, Option<String>), Response> {
    let mut spec = SinkSpec {
        on_expire: "stop".to_owned(),
        ..SinkSpec::default()
    };
    let mut start = SinkStart::Now;
    let mut target = None;
    let text = |value: &Json, name: &str| -> Result<String, Response> {
        let text = value
            .as_str()
            .ok_or_else(|| malformed(&format!("{name} is a string")))?;
        if text.len() > SINK_TEXT_MAX_BYTES {
            return Err(malformed(&format!(
                "{name} is over {SINK_TEXT_MAX_BYTES} bytes"
            )));
        }
        Ok(text.to_owned())
    };
    for (key, value) in object_fields(body)? {
        match key.as_str() {
            "kind" => spec.kind = text(&value, "kind")?,
            "url" => spec.url = Some(text(&value, "url")?),
            "target" => {
                let name = text(&value, "target")?;
                if !is_valid_stream_id(&name) {
                    return Err(malformed("target is a stream id"));
                }
                spec.target = Some(keys.hash(&name));
                target = Some(name);
            }
            "filter" => {
                let filter = text(&value, "filter")?;
                if parse_filter(&filter).is_none() {
                    return Err(malformed("filter isn't valid (D26)"));
                }
                spec.filter = Some(filter);
            }
            "on_expire" => {
                spec.on_expire = text(&value, "on_expire")?;
                if spec.on_expire != "stop" && spec.on_expire != "skip" {
                    return Err(malformed("on_expire is stop or skip"));
                }
            }
            "secret" => {
                let secret = text(&value, "secret")?;
                if secret.is_empty() {
                    // Anyone can compute an HMAC under an empty key.
                    return Err(malformed("secret can't be empty"));
                }
                spec.secret = Some(secret);
            }
            "budget_bytes" => {
                spec.budget_bytes = value
                    .as_u64()
                    .ok_or_else(|| malformed("budget_bytes is a number"))?;
            }
            "compression" => {
                let compression = text(&value, "compression")?;
                if compression != "zstd" && compression != "gzip" {
                    return Err(malformed("compression is zstd or gzip"));
                }
                spec.compression = Some(compression);
            }
            "start" => start = parse_start(&value)?,
            _ => return Err(malformed("unknown sink field")),
        }
    }
    match spec.kind.as_str() {
        "webhook.notify" | "webhook.deliver" => {
            let url = spec.url.as_deref().unwrap_or("");
            if crate::sink::parse_url(url).is_none() {
                return Err(malformed("a webhook needs an http or https url"));
            }
            if spec.kind == "webhook.deliver" && spec.secret.is_none() {
                return Err(malformed("webhook.deliver needs a secret to sign with"));
            }
            if spec.kind == "webhook.notify" && spec.compression.is_some() {
                return Err(malformed("only webhook.deliver compresses"));
            }
        }
        _ if spec.compression.is_some() => {
            return Err(malformed("only webhook.deliver compresses"));
        }
        "forward" => {
            if spec.target.is_none() {
                return Err(malformed("forward needs a target stream"));
            }
        }
        "archive" => {}
        _ => {
            return Err(malformed(
                "kind is webhook.notify, webhook.deliver, forward or archive",
            ));
        }
    }
    Ok((spec, start, target))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn an_empty_sink_secret_is_refused() {
        let deliver = br#"{"kind":"webhook.deliver","url":"http://x/","secret":""}"#;
        let keys = crate::NodeConfig::new(1).keys;
        assert_eq!(
            parse_sink(deliver, &keys).err().map(|r| r.status),
            Some(400)
        );
        let notify = br#"{"kind":"webhook.notify","url":"http://x/","secret":""}"#;
        assert_eq!(parse_sink(notify, &keys).err().map(|r| r.status), Some(400));
    }

    #[test]
    fn a_stream_id_past_what_the_formats_hold_is_refused() {
        // A checkpoint page and a WAL entry both hold a name in a u16 (D42).
        let at_limit = format!("/v1/stream/{}", "a".repeat(65_535));
        assert!(route("PUT", &at_limit).is_ok());
        let over_limit = format!("/v1/stream/{}", "a".repeat(65_536));
        assert_eq!(route("PUT", &over_limit).err().map(|r| r.status), Some(400));
    }

    #[test]
    fn routes_match_section_11_2_and_d21() {
        let ok = |method, path, expected: Route| {
            assert_eq!(route(method, path).map(|(_, r)| r).ok(), Some(expected));
        };
        ok("GET", "/v1/stream/a.b", Route::Info);
        ok("PUT", "/v1/stream/a.b", Route::Put);
        ok("DELETE", "/v1/stream/a.b", Route::Delete);
        ok("PUT", "/v1/stream/a.b/core", Route::Core);
        ok("POST", "/v1/stream/a.b/publish", Route::Publish);
        ok("GET", "/v1/stream/a.b/read", Route::Read);
        ok("GET", "/v1/stream/a.b/state", Route::StateGet);
        ok("PATCH", "/v1/stream/a.b/state", Route::StatePatch);
        ok(
            "PUT",
            "/v1/stream/a.b/cursor/c%20d",
            Route::Cursor("c d".into()),
        );
        ok("PUT", "/v1/stream/a.b/sink/7", Route::SinkPut(7));
        ok("DELETE", "/v1/stream/a.b/sink/7", Route::SinkDelete(7));
        ok("POST", "/v1/stream/a.b/sink/7/pause", Route::SinkPause(7));
        ok("POST", "/v1/stream/a.b/sink/7/resume", Route::SinkResume(7));
        ok("POST", "/v1/stream/a.b/sink/7/reset", Route::SinkReset(7));
        assert_eq!(route("GET", "/v1/stream/a%2Fb").err().unwrap().status, 400);
        assert_eq!(route("POST", "/v1/stream/a").err().unwrap().status, 405);
        assert_eq!(route("GET", "/v2/x").err().unwrap().status, 404);
        assert_eq!(route("GET", "/v1/stream/a/nope").err().unwrap().status, 404);
        assert_eq!(
            route("PUT", "/v1/stream/a/sink/x").err().unwrap().status,
            400
        );
    }

    #[test]
    fn x_options_reads_hex_and_decimal() {
        assert_eq!(parse_u64("0x8000000000000000"), Some(1 << 63));
        assert_eq!(parse_u64("0XFF"), Some(255));
        assert_eq!(parse_u64("42"), Some(42));
        assert_eq!(parse_u64("0xZZ"), None);
        assert_eq!(parse_u64("0x10000000000000000"), None);
    }

    #[test]
    fn filters_read_d26s_text() {
        let (clauses, author) = parse_filter("eq:0xFF:0x01,any:0x100,author:9").unwrap();
        assert_eq!(
            clauses,
            [
                Clause::Eq {
                    mask: 255,
                    value: 1
                },
                Clause::Any { mask: 256 }
            ]
        );
        assert_eq!(author, Some(9));
        assert_eq!(parse_filter("").unwrap().0, []);
        assert!(parse_filter("eq:1").is_none());
        assert!(parse_filter("eq:x:1").is_none());
        assert!(parse_filter("nope:1").is_none());
        assert!(
            parse_filter("any:1,any:1,any:1,any:1,any:1").is_none(),
            "over 4"
        );
    }

    #[test]
    fn requests_with_two_lengths_or_a_transfer_encoding_are_refused() {
        let parse = |head: &str| parse_request(head.as_bytes(), Vec::new());
        assert!(parse("GET / HTTP/1.1\r\ncontent-length: 1\r\nContent-Length: 2").is_err());
        // RFC 9110: digits only, or a proxy could frame the request differently.
        for length in ["+3", "-3", "", "3 3", "0x3", "3,3", "\u{0663}"] {
            let head = format!("POST /a HTTP/1.1\r\nContent-Length: {length}");
            assert!(parse(&head).is_err(), "{length:?}");
        }
        assert!(parse("GET / HTTP/1.1\r\ntransfer-encoding: chunked").is_err());
        assert!(parse("GET / HTTP/1.1\r\n bad: fold").is_err());
        assert!(parse("GET / HTTP/2").is_err());
        assert!(parse("GET /").is_err());
        let ok = parse("POST /a?x=1&y=a%20b HTTP/1.1\r\nX-Up: V\r\nContent-Length: 3").unwrap();
        assert_eq!(ok.query("y"), Some("a b"));
        assert_eq!(ok.header("x-up"), Some("V"));
        assert_eq!(ok.content_length, 3);
    }

    proptest! {
        #[test]
        fn random_heads_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _ = parse_request(&bytes, Vec::new());
        }

        #[test]
        fn random_routes_never_panic(path in "/[a-z0-9/%.]{0,40}") {
            for method in ["GET", "PUT", "POST", "PATCH", "DELETE"] {
                let _ = route(method, &path);
            }
        }

        #[test]
        fn random_filters_never_panic(text in "[a-z0-9:,x]{0,40}") {
            let _ = parse_filter(&text);
        }
    }
}
