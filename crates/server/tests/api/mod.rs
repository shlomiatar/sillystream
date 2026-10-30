//! A real server over real TLS sockets, and the clients to talk to it. Certificates are
//! throwaway ones from `rcgen`.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use object_store::memory::InMemory;
use proto::{ClientFrame, ErrorCode, ServerFrame, decode_server};
use server::net::{Server, ServerConfig};
use server::{Clock, ManualClock, NodeConfig};
use storage::DirVolume;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Message;

pub const SECRET: &[u8] = b"a test secret that is long enough for the check";
pub const START_MS: u64 = 1_800_000_000_000;
pub const AUTHOR: u64 = 77;
pub const WAIT_MS: u64 = 10_000;

pub struct TestServer {
    pub server: Server,
    pub clock: Arc<ManualClock>,
    pub connector: TlsConnector,
    pub cert_der: Vec<u8>,
    pub _wal: TempDir,
}

pub struct TestCert {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
    pub der: Vec<u8>,
}

pub fn test_cert() -> TestCert {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    TestCert {
        cert_pem: certified.cert.pem().into_bytes(),
        key_pem: certified.key_pair.serialize_pem().into_bytes(),
        der: certified.cert.der().to_vec(),
    }
}

pub fn tls_client_config(
    der: &[u8],
    versions: &[&'static rustls::SupportedProtocolVersion],
) -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(der.to_vec().into()).unwrap();
    rustls::ClientConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_protocol_versions(versions)
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth()
}

/// One core unless the test says otherwise, a real WAL file, and an in-memory store. Ids run
/// on a clock the test controls, unless `system_clock`.
pub fn start_with(node: NodeConfig, clock: Arc<dyn Clock>, manual: Arc<ManualClock>) -> TestServer {
    let cert = test_cert();
    let wal = tempfile::tempdir().unwrap();
    let server = Server::start(
        ServerConfig {
            node,
            listen: "127.0.0.1:0".parse().unwrap(),
            cert_pem: cert.cert_pem.clone(),
            key_pem: cert.key_pem.clone(),
            token_secret: SECRET.to_vec(),
        },
        DirVolume::new(wal.path(), 1024 * 1024),
        Arc::new(InMemory::new()),
        clock,
    )
    .unwrap();
    let config = tls_client_config(&cert.der, &[&rustls::version::TLS13]);
    TestServer {
        server,
        clock: manual,
        connector: TlsConnector::from(Arc::new(config)),
        cert_der: cert.der,
        _wal: wal,
    }
}

pub fn node_config(core_count: usize) -> NodeConfig {
    let mut node = NodeConfig::new(core_count);
    node.seal_window_ms = None;
    node.rebalance_period_ms = None;
    node.progress_interval_ms = 100;
    node
}

pub fn start() -> TestServer {
    start_node(node_config(2))
}

pub fn start_node(node: NodeConfig) -> TestServer {
    let manual = Arc::new(ManualClock::new(START_MS));
    start_with(node, manual.clone(), manual)
}

pub fn token_at(expiry_ms: u64, author: u64, grants: &[(&str, u8)]) -> String {
    auth::Token {
        expiry_ms,
        author: std::num::NonZeroU64::new(author).unwrap(),
        grants: grants
            .iter()
            .map(|(pattern, perms)| auth::Grant {
                pattern: (*pattern).to_owned(),
                perms: *perms,
            })
            .collect(),
    }
    .sign(SECRET)
    .unwrap()
}

pub fn token(perms: u8) -> String {
    token_at(START_MS + 3_600_000, AUTHOR, &[("*", perms)])
}

pub const ALL: u8 = auth::PERM_READ | auth::PERM_PUBLISH | auth::PERM_STATE | auth::PERM_ADMIN;

/// A record ready to publish: id and author 0.
pub fn record(options: u64, data: &[u8]) -> Vec<u8> {
    let header = format::RecordHeader {
        id: 0,
        options,
        author: 0,
        len: data.len() as u32,
        count: 1,
        codec: 0,
    };
    let mut out = header.to_bytes().to_vec();
    out.extend_from_slice(data);
    out.resize(out.len().next_multiple_of(8), 0);
    out
}

/// `(id, options, author, data)` of each record.
pub fn parse_records(bytes: &[u8]) -> Vec<(u64, u64, u64, Vec<u8>)> {
    format::Records::new(bytes)
        .map(|record| {
            let record = record.unwrap();
            (
                record.header.id,
                record.header.options,
                record.header.author,
                record.data.to_vec(),
            )
        })
        .collect()
}

pub fn cbor(value: &ciborium::value::Value) -> Vec<u8> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(value, &mut out).unwrap();
    out
}

pub fn cbor_map(pairs: &[(&str, i64)]) -> Vec<u8> {
    use ciborium::value::Value;
    cbor(&Value::Map(
        pairs
            .iter()
            .map(|(key, value)| {
                (
                    Value::Text((*key).to_owned()),
                    Value::Integer((*value).into()),
                )
            })
            .collect(),
    ))
}

impl TestServer {
    pub async fn tls(&self) -> TlsStream<TcpStream> {
        // A run before this one can leave the machine's local ports in TIME_WAIT for a while.
        let mut tries = 0;
        let socket = loop {
            match TcpStream::connect(self.server.local_addr()).await {
                Err(error)
                    if error.kind() == std::io::ErrorKind::AddrNotAvailable && tries < 120 =>
                {
                    tries += 1;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                other => break other.unwrap(),
            }
        };
        self.connector
            .connect("localhost".try_into().unwrap(), socket)
            .await
            .unwrap()
    }

    /// `tokio-tungstenite` as the client: not the SDK, and not code from this repository.
    pub async fn ws_raw(&self) -> Ws {
        let tls = self.tls().await;
        let url = format!("wss://localhost:{}/v1/ws", self.server.local_addr().port());
        let (socket, response) = tokio_tungstenite::client_async(url, tls).await.unwrap();
        assert_eq!(response.status(), 101);
        Ws { socket }
    }

    /// Connected, with `HELLO` done.
    pub async fn ws(&self, token: &str) -> Ws {
        let mut ws = self.ws_raw().await;
        ws.send(&ClientFrame::Hello { ver: 1, token }).await;
        match ws.recv_owned().await {
            Owned::Welcome { .. } => {}
            other => panic!("expected WELCOME, got {other:?}"),
        }
        ws
    }

    /// One whole HTTP exchange on its own connection.
    pub async fn http(&self, request: &Http<'_>) -> HttpReply {
        let mut tls = self.tls().await;
        tls.write_all(&request.bytes()).await.unwrap();
        read_http_reply(&mut tls).await
    }
}

pub struct Http<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub token: Option<&'a str>,
    pub headers: Vec<(&'a str, &'a str)>,
    pub body: Vec<u8>,
}

impl<'a> Http<'a> {
    pub fn new(method: &'a str, path: &'a str, token: &'a str) -> Http<'a> {
        Http {
            method,
            path,
            token: Some(token),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Http<'a> {
        self.body = body.into();
        self
    }

    pub fn header(mut self, name: &'a str, value: &'a str) -> Http<'a> {
        self.headers.push((name, value));
        self
    }

    pub fn bytes(&self) -> Vec<u8> {
        let mut out = format!(
            "{} {} HTTP/1.1\r\nhost: localhost\r\n",
            self.method, self.path
        );
        if let Some(token) = self.token {
            out.push_str(&format!("authorization: Bearer {token}\r\n"));
        }
        for (name, value) in &self.headers {
            out.push_str(&format!("{name}: {value}\r\n"));
        }
        out.push_str(&format!("content-length: {}\r\n\r\n", self.body.len()));
        let mut out = out.into_bytes();
        out.extend_from_slice(&self.body);
        out
    }
}

#[derive(Debug)]
pub struct HttpReply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpReply {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// The `code` an error body carries.
    pub fn code(&self) -> Option<u16> {
        let text = self.text();
        let at = text.find("\"code\":")?;
        let rest = &text[at + 7..];
        let end = rest.find(|c: char| !c.is_ascii_digit())?;
        rest[..end].parse().ok()
    }

    /// A field of a result, whether a string or a number.
    pub fn field(&self, name: &str) -> Option<String> {
        let text = self.text();
        let key = format!("\"{name}\":");
        let at = text.find(&key)?;
        let rest = text[at + key.len()..].trim_start_matches('"');
        let end = rest.find(['"', ',', '}'])?;
        Some(rest[..end].to_owned())
    }
}

pub async fn read_http_reply(tls: &mut (impl tokio::io::AsyncRead + Unpin)) -> HttpReply {
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 16 * 1024];
    let head_end = loop {
        if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            // An interim response has no body and isn't the reply.
            if bytes.starts_with(b"HTTP/1.1 100") {
                bytes.drain(..end + 4);
                continue;
            }
            break end;
        }
        let read = tokio::time::timeout(Duration::from_millis(WAIT_MS), tls.read(&mut buffer))
            .await
            .expect("the reply should come")
            .unwrap();
        assert_ne!(
            read,
            0,
            "closed before a reply: {:?}",
            String::from_utf8_lossy(&bytes)
        );
        bytes.extend_from_slice(&buffer[..read]);
    };
    let head = String::from_utf8(bytes[..head_end].to_vec()).unwrap();
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .unwrap()
        .split(' ')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let headers: Vec<(String, String)> = lines
        .map(|line| {
            let (name, value) = line.split_once(": ").unwrap();
            (name.to_owned(), value.to_owned())
        })
        .collect();
    let length: usize = headers
        .iter()
        .find(|(name, _)| name == "content-length")
        .unwrap()
        .1
        .parse()
        .unwrap();
    let mut body = bytes[head_end + 4..].to_vec();
    while body.len() < length {
        let read = tokio::time::timeout(Duration::from_millis(WAIT_MS), tls.read(&mut buffer))
            .await
            .expect("the body should come")
            .unwrap();
        assert_ne!(read, 0);
        body.extend_from_slice(&buffer[..read]);
    }
    HttpReply {
        status,
        headers,
        body,
    }
}

pub struct Ws {
    pub socket: WebSocketStream<TlsStream<TcpStream>>,
}

/// A decoded server frame that outlives its message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Owned {
    Welcome {
        max_frame_bytes: u32,
        server_ms: u64,
    },
    Subscribed {
        sub: u32,
        head: u64,
        state_ver: u64,
        state: Vec<u8>,
    },
    Data {
        sub: u32,
        records: Vec<u8>,
    },
    Ack {
        handle: u32,
        producer_seq: u64,
        first_id: u64,
        count: u32,
    },
    State {
        sub: u32,
        ver: u64,
        doc: Vec<u8>,
    },
    Behind {
        sub: u32,
        resume_from: u64,
    },
    Progress {
        sub: u32,
        id: u64,
    },
    StateResult {
        req: u32,
        ok: bool,
        ver: u64,
    },
    Nack {
        handle: u32,
        producer_seq: u64,
        code: ErrorCode,
    },
    Error {
        reference: u32,
        code: ErrorCode,
    },
    Pong {
        nonce: u32,
    },
    Closed,
}

impl Ws {
    pub async fn send(&mut self, frame: &ClientFrame<'_>) {
        let mut out = Vec::new();
        frame.encode(&mut out).unwrap();
        self.socket.send(Message::Binary(out)).await.unwrap();
    }

    pub async fn send_bytes(&mut self, bytes: Vec<u8>) {
        self.socket.send(Message::Binary(bytes)).await.unwrap();
    }

    pub async fn recv_owned(&mut self) -> Owned {
        self.recv_within(WAIT_MS)
            .await
            .expect("a frame should come")
    }

    /// The next frame, or `None` if nothing comes in time.
    pub async fn recv_within(&mut self, wait_ms: u64) -> Option<Owned> {
        loop {
            let message = tokio::time::timeout(Duration::from_millis(wait_ms), self.socket.next())
                .await
                .ok()?;
            let message = match message {
                None | Some(Err(_)) => return Some(Owned::Closed),
                Some(Ok(message)) => message,
            };
            let bytes = match message {
                Message::Binary(bytes) => bytes,
                Message::Close(_) => return Some(Owned::Closed),
                _ => continue,
            };
            let frame = decode_server(&bytes, usize::MAX).unwrap();
            return Some(match frame {
                ServerFrame::Welcome {
                    max_frame_bytes,
                    server_ms,
                    ..
                } => Owned::Welcome {
                    max_frame_bytes,
                    server_ms,
                },
                ServerFrame::Subscribed {
                    sub,
                    head,
                    state_ver,
                    state,
                } => Owned::Subscribed {
                    sub,
                    head,
                    state_ver,
                    state: state.to_vec(),
                },
                ServerFrame::Data { sub, records } => Owned::Data {
                    sub,
                    records: records.to_vec(),
                },
                ServerFrame::Ack {
                    handle,
                    producer_seq,
                    first_id,
                    count,
                } => Owned::Ack {
                    handle,
                    producer_seq,
                    first_id,
                    count,
                },
                ServerFrame::State { sub, ver, doc } => Owned::State {
                    sub,
                    ver,
                    doc: doc.to_vec(),
                },
                ServerFrame::Behind { sub, resume_from } => Owned::Behind { sub, resume_from },
                ServerFrame::Progress { sub, id } => Owned::Progress { sub, id },
                ServerFrame::StateResult { req, ok, ver } => Owned::StateResult { req, ok, ver },
                ServerFrame::Nack {
                    handle,
                    producer_seq,
                    code,
                    ..
                } => Owned::Nack {
                    handle,
                    producer_seq,
                    code,
                },
                ServerFrame::Error {
                    reference, code, ..
                } => Owned::Error { reference, code },
                ServerFrame::Pong { nonce } => Owned::Pong { nonce },
                ServerFrame::Redirect { .. } => panic!("deferred with multiple nodes"),
            });
        }
    }

    pub async fn open(&mut self, handle: u32, stream: &str) {
        self.send(&ClientFrame::Open { handle, stream }).await;
    }

    pub async fn publish(&mut self, handle: u32, seq: u64, ack: proto::AckLevel, records: &[u8]) {
        self.send(&ClientFrame::Publish(proto::Publish {
            handle,
            producer_id: 9,
            producer_seq: seq,
            ack,
            state: None,
            records,
        }))
        .await;
    }

    pub async fn subscribe(&mut self, sub: u32, stream: &str, from: u64) {
        self.send(&ClientFrame::Subscribe(proto::Subscribe {
            sub,
            from,
            linger_ms: 0,
            author: None,
            clauses: proto::WireClauses::default(),
            stream,
            name: None,
            with_state: false,
            state_only: false,
            include_system: false,
        }))
        .await;
    }

    /// `PING`'s answer: so everything sent before it has been read.
    pub async fn round_trip(&mut self, nonce: u32) {
        self.send(&ClientFrame::Ping { nonce }).await;
        loop {
            match self.recv_owned().await {
                Owned::Pong { nonce: got } if got == nonce => return,
                Owned::Closed => panic!("closed"),
                _ => {}
            }
        }
    }
}
