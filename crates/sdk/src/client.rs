//! One task owns the socket and everything unanswered, so a reconnect has one place to look. The
//! rest of the client talks to it by message.
//!
//! Publishes wait in a per-producer queue from `add` until their ack: a dropped connection sends
//! them again in order with the same `(producer_id, producer_seq)`, which the server drops as
//! duplicates if they had got through. A state op has no such key, so it fails with
//! `Disconnected` and the caller decides.

use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use proto::{
    AckLevel, ClientFrame, ErrorCode, PROTOCOL_VERSION, PublishState, ServerFrame, StateOp,
    WireClause, WireClauses, decode_server,
};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};

use crate::batch::{BatchConfig, Item, build_records, decode_records, last_id};
use crate::{FROM_LIVE, FROM_START, SdkError};

/// What a frame needs around its records: `type flags handle id seq ack` and a length.
const PUBLISH_OVERHEAD_BYTES: usize = 27;
const FIRST_WIRE_ID: u32 = 1 << 31;

#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// `wss://host:port`, or `host:port`.
    pub url: String,
    /// The roots to trust, as PEM. This client doesn't read the system's.
    pub ca_pem: Vec<u8>,
    pub token: String,
    pub reconnect_min_ms: u64,
    pub reconnect_max_ms: u64,
    /// A connection silent for two of these is taken as dead.
    pub ping_interval_ms: u64,
}

impl ClientConfig {
    pub fn new(url: &str, ca_pem: &[u8], token: &str) -> ClientConfig {
        ClientConfig {
            url: url.to_owned(),
            ca_pem: ca_pem.to_vec(),
            token: token.to_owned(),
            reconnect_min_ms: 50,
            reconnect_max_ms: 5_000,
            ping_interval_ms: 15_000,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PublisherConfig {
    pub ack: AckLevel,
    pub batch: BatchConfig,
    /// `None` picks a random one. Set it, with `first_seq`, to carry on after a restart.
    pub producer_id: Option<u64>,
    pub first_seq: u64,
    /// Records unanswered, in bytes. `send` waits past this.
    pub max_inflight_bytes: usize,
}

impl Default for PublisherConfig {
    fn default() -> PublisherConfig {
        PublisherConfig {
            ack: AckLevel::WalFsync,
            batch: BatchConfig::default(),
            producer_id: None,
            first_seq: 1,
            max_inflight_bytes: 64 * 1024 * 1024,
        }
    }
}

/// What a publish got back. `first_id = 0` is section 3's older retry: it had already committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ack {
    pub producer_seq: u64,
    pub first_id: u64,
    pub count: u32,
}

/// A publish on its way.
pub struct AckWait(oneshot::Receiver<Result<Ack, SdkError>>);

impl AckWait {
    pub async fn wait(self) -> Result<Ack, SdkError> {
        self.0.await.unwrap_or(Err(SdkError::Closed))
    }
}

/// A state change carried on a publish, or sent by itself.
#[derive(Debug, Clone)]
pub struct StateChange {
    /// 0 is unconditional.
    pub expect_ver: u64,
    pub op: StateOp,
    /// CBOR.
    pub doc: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Subscribe {
    pub stream: String,
    pub from: u64,
    pub linger_ms: u16,
    pub author: Option<u64>,
    pub clauses: Vec<WireClause>,
    pub name: Option<String>,
    pub with_state: bool,
    pub state_only: bool,
    pub include_system: bool,
    /// Records waiting for `next`, in bytes. Past it the subscription is dropped until they have
    /// been read, and comes back from its cursor.
    pub max_queue_bytes: usize,
    pub max_decompressed_bytes: usize,
}

impl Subscribe {
    pub fn new(stream: &str, from: u64) -> Subscribe {
        Subscribe {
            stream: stream.to_owned(),
            from,
            linger_ms: 0,
            author: None,
            clauses: Vec::new(),
            name: None,
            with_state: false,
            state_only: false,
            include_system: false,
            max_queue_bytes: 64 * 1024 * 1024,
            max_decompressed_bytes: 256 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The subscription is in place, on this connection. `state` is empty unless asked for.
    Subscribed {
        head: u64,
        state_ver: u64,
        state: Vec<u8>,
    },
    Items(Vec<Item>),
    /// Nothing matches up to this id.
    Progress(u64),
    State {
        ver: u64,
        doc: Vec<u8>,
    },
    /// The server fell behind sending to this subscription and is catching it up from here.
    Behind {
        resume_from: u64,
    },
}

enum Command {
    AddProducer {
        key: u32,
        stream: Arc<str>,
        producer_id: u64,
    },
    DropProducer {
        key: u32,
    },
    Publish {
        key: u32,
        entry: Entry,
    },
    State {
        stream: String,
        change: StateChange,
        reply: oneshot::Sender<Result<u64, SdkError>>,
    },
    Subscribe {
        key: u32,
        spec: Subscribe,
        queued_bytes: Arc<AtomicUsize>,
        events: mpsc::UnboundedSender<Result<Raw, SdkError>>,
    },
    Unsubscribe {
        key: u32,
    },
    Commit {
        key: u32,
        id: u64,
    },
    Drained {
        key: u32,
    },
    SetToken(String),
}

/// What the socket task hands a `Subscription`, which opens it on the consumer's time.
enum Raw {
    Subscribed {
        head: u64,
        state_ver: u64,
        state: Vec<u8>,
    },
    Data(Bytes),
    Progress(u64),
    State {
        ver: u64,
        doc: Vec<u8>,
    },
    Behind(u64),
}

struct Entry {
    seq: u64,
    ack: AckLevel,
    state: Option<StateChange>,
    records: Bytes,
    reply: oneshot::Sender<Result<Ack, SdkError>>,
    sent: bool,
    _permit: OwnedSemaphorePermit,
}

impl Entry {
    /// Section 3: later publishes wait for this one's answer, because a later seq succeeding
    /// would make its retry look like a duplicate.
    fn blocks_later(&self) -> bool {
        self.state
            .as_ref()
            .is_some_and(|state| state.expect_ver != 0)
    }
}

struct Producer {
    stream: Arc<str>,
    producer_id: u64,
    queue: VecDeque<Entry>,
    opened: bool,
    dropped: bool,
}

struct SubState {
    spec: Subscribe,
    /// What this connection knows it as: a resubscribe takes a fresh number, so that late frames
    /// for the old one are recognised and left out.
    wire_sub: Option<u32>,
    cursor: u64,
    committed: u64,
    paused: bool,
    /// The server ended it, and coming back would only get the same answer.
    ended: bool,
    queued_bytes: Arc<AtomicUsize>,
    events: mpsc::UnboundedSender<Result<Raw, SdkError>>,
}

type Tls = TlsStream<TcpStream>;
type Ws = WebSocketStream<Tls>;

/// A connection to a server. Cheap to clone.
#[derive(Clone)]
pub struct Client {
    commands: mpsc::UnboundedSender<Command>,
    keys: Arc<AtomicU32>,
    max_frame_bytes: Arc<AtomicUsize>,
}

pub(crate) fn parse_address(url: &str) -> Result<(String, u16), SdkError> {
    let rest = url
        .strip_prefix("wss://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url);
    let rest = rest.split('/').next().unwrap_or(rest);
    let (host, port) = rest
        .rsplit_once(':')
        .ok_or_else(|| SdkError::Config(format!("{url}: no port")))?;
    let port = port
        .parse()
        .map_err(|_| SdkError::Config(format!("{url}: bad port")))?;
    Ok((host.to_owned(), port))
}

pub(crate) fn tls_connector(ca_pem: &[u8]) -> Result<Arc<TlsConnector>, SdkError> {
    let mut roots = rustls::RootCertStore::empty();
    let mut reader = ca_pem;
    for cert in rustls_pemfile::certs(&mut reader) {
        let cert = cert.map_err(|error| SdkError::Config(format!("CA: {error}")))?;
        roots
            .add(cert)
            .map_err(|error| SdkError::Config(format!("CA: {error}")))?;
    }
    if roots.is_empty() {
        return Err(SdkError::Config("CA: no certificates".to_owned()));
    }
    let provider = rustls::crypto::ring::default_provider();
    let config = rustls::ClientConfig::builder_with_provider(provider.into())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| SdkError::Config(error.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(TlsConnector::from(Arc::new(config))))
}

impl Client {
    /// Connects once before returning, so that a bad address or token fails here. After that it
    /// reconnects by itself.
    pub async fn connect(config: ClientConfig) -> Result<Client, SdkError> {
        let (host, port) = parse_address(&config.url)?;
        let connector = tls_connector(&config.ca_pem)?;
        let (commands, receiver) = mpsc::unbounded_channel();
        let max_frame_bytes = Arc::new(AtomicUsize::new(proto::DEFAULT_MAX_FRAME_BYTES));
        let mut driver = Driver {
            host,
            port,
            connector,
            token: config.token.clone(),
            reconnect_min_ms: config.reconnect_min_ms,
            reconnect_max_ms: config.reconnect_max_ms,
            ping_interval_ms: config.ping_interval_ms,
            commands: receiver,
            commands_open: true,
            producers: HashMap::new(),
            subs: HashMap::new(),
            wire_subs: HashMap::new(),
            state_handles: HashMap::new(),
            state_waiters: HashMap::new(),
            // Producers' handles count up from 1 in the same space that errors refer to.
            next_wire: FIRST_WIRE_ID,
            max_frame_bytes: Arc::clone(&max_frame_bytes),
            out: None,
        };
        let ws = driver.connect().await?;
        tokio::spawn(driver.run(ws));
        Ok(Client {
            commands,
            keys: Arc::new(AtomicU32::new(1)),
            max_frame_bytes,
        })
    }

    fn send(&self, command: Command) -> Result<(), SdkError> {
        self.commands.send(command).map_err(|_| SdkError::Closed)
    }

    pub fn publisher(&self, stream: &str, config: PublisherConfig) -> Result<Publisher, SdkError> {
        if !proto::is_valid_stream_id(stream) {
            return Err(SdkError::Config(format!("{stream}: not a stream id")));
        }
        let key = self.keys.fetch_add(1, Ordering::Relaxed);
        let producer_id = config.producer_id.unwrap_or_else(random_nonzero);
        self.send(Command::AddProducer {
            key,
            stream: stream.into(),
            producer_id,
        })?;
        Ok(Publisher {
            client: self.clone(),
            key,
            next_seq: config.first_seq,
            pending: Vec::new(),
            credit: Arc::new(Semaphore::new(config.max_inflight_bytes)),
            config,
        })
    }

    /// Sends a state op by itself and returns the new version.
    pub async fn state_op(&self, stream: &str, change: StateChange) -> Result<u64, SdkError> {
        let (reply, answer) = oneshot::channel();
        self.send(Command::State {
            stream: stream.to_owned(),
            change,
            reply,
        })?;
        answer.await.unwrap_or(Err(SdkError::Closed))
    }

    pub fn subscribe(&self, spec: Subscribe) -> Result<Subscription, SdkError> {
        WireClauses::new(&spec.clauses).map_err(|_| SdkError::Config("too many clauses".into()))?;
        let key = self.keys.fetch_add(1, Ordering::Relaxed);
        let (events, receiver) = mpsc::unbounded_channel();
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        let skip_through = match spec.from {
            FROM_START | FROM_LIVE => 0,
            from => from,
        };
        let max_decompressed_bytes = spec.max_decompressed_bytes;
        let max_queue_bytes = spec.max_queue_bytes;
        self.send(Command::Subscribe {
            key,
            spec,
            queued_bytes: Arc::clone(&queued_bytes),
            events,
        })?;
        Ok(Subscription {
            client: self.clone(),
            key,
            events: receiver,
            queued_bytes,
            skip_through,
            max_decompressed_bytes,
            max_queue_bytes,
        })
    }

    /// The token this connection and later ones use. Section 11's renewal: a second `HELLO`.
    pub fn set_token(&self, token: &str) -> Result<(), SdkError> {
        self.send(Command::SetToken(token.to_owned()))
    }

    pub fn max_frame_bytes(&self) -> usize {
        self.max_frame_bytes.load(Ordering::Relaxed)
    }
}

fn random_nonzero() -> u64 {
    loop {
        let value = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        if value != 0 {
            return value;
        }
    }
}

/// Publishes to one stream as one producer.
pub struct Publisher {
    client: Client,
    key: u32,
    next_seq: u64,
    pending: Vec<(u64, Vec<u8>)>,
    credit: Arc<Semaphore>,
    config: PublisherConfig,
}

impl Publisher {
    /// Holds an item for the next `send`.
    pub fn add(&mut self, options: u64, bytes: impl Into<Vec<u8>>) {
        self.pending.push((options, bytes.into()));
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Sends what `add` held: sorted, grouped into records, and over as many publishes as the
    /// maximum frame makes it. Ids follow that order. Waits only for room in the window, and
    /// each returned wait is that publish's ack.
    pub async fn send(&mut self) -> Result<Vec<AckWait>, SdkError> {
        self.send_inner(None).await
    }

    /// As `send`, and the first publish carries a state change, committing with it. Without any
    /// `add` that isn't possible, because a publish needs a record.
    pub async fn send_with_state(&mut self, state: StateChange) -> Result<Vec<AckWait>, SdkError> {
        if self.pending.is_empty() {
            return Err(SdkError::Config("a publish needs at least one item".into()));
        }
        self.send_inner(Some(state)).await
    }

    /// `send`, then every ack.
    pub async fn flush(&mut self) -> Result<Vec<Ack>, SdkError> {
        let waits = self.send().await?;
        let mut acks = Vec::with_capacity(waits.len());
        for wait in waits {
            acks.push(wait.wait().await?);
        }
        Ok(acks)
    }

    async fn send_inner(
        &mut self,
        mut state: Option<StateChange>,
    ) -> Result<Vec<AckWait>, SdkError> {
        let items = std::mem::take(&mut self.pending);
        let borrowed: Vec<(u64, &[u8])> = items
            .iter()
            .map(|(options, bytes)| (*options, bytes.as_slice()))
            .collect();
        let records = build_records(&borrowed, &self.config.batch)?;
        let max_bytes = self.client.max_frame_bytes();
        let room = max_bytes.saturating_sub(PUBLISH_OVERHEAD_BYTES);
        let state_bytes = state.as_ref().map_or(0, |state| state.doc.len() + 13);
        // Whatever can't ever go is refused before any of it does.
        if records
            .iter()
            .any(|record| record.len() + state_bytes > room)
        {
            self.pending = items;
            return Err(SdkError::TooLarge);
        }
        let mut waits = Vec::new();
        let mut frame = Vec::new();
        for record in records {
            let used = if state.is_some() { state_bytes } else { 0 };
            if !frame.is_empty() && frame.len() + record.len() + used > room {
                waits.push(
                    self.send_records(&std::mem::take(&mut frame), state.take())
                        .await?,
                );
            }
            frame.extend_from_slice(&record);
        }
        waits.push(self.send_records(&frame, state.take()).await?);
        Ok(waits)
    }

    /// One publish of records that are already encoded, as they are. The load generator and
    /// other producers with their own batches use it.
    pub async fn send_records(
        &mut self,
        records: &[u8],
        state: Option<StateChange>,
    ) -> Result<AckWait, SdkError> {
        // A conditional change needs its answer, so level 0 can't apply to it.
        let ack = match (&state, self.config.ack) {
            (Some(change), AckLevel::None) if change.expect_ver != 0 => AckLevel::Memory,
            (_, ack) => ack,
        };
        let weight = (records.len() + state.as_ref().map_or(0, |state| state.doc.len())).max(1);
        let limit = self.config.max_inflight_bytes;
        let permit = Arc::clone(&self.credit)
            .acquire_many_owned(weight.min(limit) as u32)
            .await
            .map_err(|_| SdkError::Closed)?;
        let (reply, answer) = oneshot::channel();
        let seq = self.next_seq;
        self.next_seq += 1;
        self.client.send(Command::Publish {
            key: self.key,
            entry: Entry {
                seq,
                ack,
                state,
                records: Bytes::copy_from_slice(records),
                reply,
                sent: false,
                _permit: permit,
            },
        })?;
        Ok(AckWait(answer))
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        let _ = self.client.send(Command::DropProducer { key: self.key });
    }
}

/// Reads one subscription. Dropping it unsubscribes.
pub struct Subscription {
    client: Client,
    key: u32,
    events: mpsc::UnboundedReceiver<Result<Raw, SdkError>>,
    queued_bytes: Arc<AtomicUsize>,
    /// Section 5.2's skip by id: whatever has been handed over isn't handed over again, whether
    /// the repeat comes from the middle of a record or a resume after a reconnect.
    skip_through: u64,
    max_decompressed_bytes: usize,
    max_queue_bytes: usize,
}

impl Subscription {
    /// The next thing to happen, or `None` once the client has stopped. An `Err` from the server
    /// ends the subscription, except error 8 for a bad `commit`.
    pub async fn next(&mut self) -> Option<Result<Event, SdkError>> {
        loop {
            let raw = match self.events.recv().await? {
                Ok(raw) => raw,
                Err(error) => return Some(Err(error)),
            };
            return Some(Ok(match raw {
                Raw::Subscribed {
                    head,
                    state_ver,
                    state,
                } => Event::Subscribed {
                    head,
                    state_ver,
                    state,
                },
                Raw::Data(records) => {
                    let before = self
                        .queued_bytes
                        .fetch_sub(records.len(), Ordering::Relaxed);
                    let after = before - records.len();
                    if before > self.max_queue_bytes / 2 && after <= self.max_queue_bytes / 2 {
                        let _ = self.client.send(Command::Drained { key: self.key });
                    }
                    let items = match decode_records(
                        &records,
                        self.skip_through,
                        self.max_decompressed_bytes,
                    ) {
                        Ok(items) => items,
                        Err(error) => return Some(Err(error)),
                    };
                    let Some(last) = items.last() else {
                        continue;
                    };
                    self.skip_through = last.id;
                    Event::Items(items)
                }
                Raw::Progress(id) => {
                    self.skip_through = self.skip_through.max(id);
                    Event::Progress(id)
                }
                Raw::State { ver, doc } => Event::State { ver, doc },
                Raw::Behind(resume_from) => Event::Behind { resume_from },
            }));
        }
    }

    /// Moves this subscription's named cursor. It moves forward only (section 5.1).
    pub fn commit(&self, id: u64) -> Result<(), SdkError> {
        self.client.send(Command::Commit { key: self.key, id })
    }

    /// The last id handed over. Keep it to resume from elsewhere with a client-held cursor.
    pub fn cursor(&self) -> u64 {
        self.skip_through
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let _ = self.client.send(Command::Unsubscribe { key: self.key });
    }
}

struct Driver {
    host: String,
    port: u16,
    connector: Arc<TlsConnector>,
    token: String,
    reconnect_min_ms: u64,
    reconnect_max_ms: u64,
    ping_interval_ms: u64,
    commands: mpsc::UnboundedReceiver<Command>,
    commands_open: bool,
    producers: HashMap<u32, Producer>,
    subs: HashMap<u32, SubState>,
    wire_subs: HashMap<u32, u32>,
    /// Per connection: the handle state ops on a stream go through.
    state_handles: HashMap<String, u32>,
    state_waiters: HashMap<u32, oneshot::Sender<Result<u64, SdkError>>>,
    next_wire: u32,
    max_frame_bytes: Arc<AtomicUsize>,
    /// Empty while there is no connection.
    out: Option<mpsc::UnboundedSender<Message>>,
}

fn encode(frame: &ClientFrame<'_>) -> Result<Message, SdkError> {
    let mut bytes = Vec::new();
    frame.encode(&mut bytes).map_err(|_| SdkError::TooLarge)?;
    Ok(Message::Binary(bytes))
}

enum SessionEnd {
    Disconnected,
    /// Everything is answered and nobody can ask for more.
    Finished,
    /// The token isn't accepted any longer, so trying again can't help.
    Refused(SdkError),
}

impl Driver {
    fn wire_id(&mut self) -> u32 {
        let id = self.next_wire;
        self.next_wire = self.next_wire.wrapping_add(1).max(FIRST_WIRE_ID);
        id
    }

    /// Socket, TLS, upgrade, `HELLO` and `WELCOME`.
    async fn connect(&mut self) -> Result<Ws, SdkError> {
        let failed = |error: std::io::Error| SdkError::Connect(error.to_string());
        let socket = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(failed)?;
        socket.set_nodelay(true).map_err(failed)?;
        let name = rustls::pki_types::ServerName::try_from(self.host.clone())
            .map_err(|error| SdkError::Config(error.to_string()))?;
        let tls = self.connector.connect(name, socket).await.map_err(failed)?;
        let url = format!("wss://{}:{}/v1/ws", self.host, self.port);
        let config = WebSocketConfig {
            max_message_size: None,
            max_frame_size: None,
            ..WebSocketConfig::default()
        };
        let (mut ws, _) = tokio_tungstenite::client_async_with_config(url, tls, Some(config))
            .await
            .map_err(|error| SdkError::Connect(error.to_string()))?;
        let hello = encode(&ClientFrame::Hello {
            ver: PROTOCOL_VERSION,
            token: &self.token,
        })?;
        ws.send(hello)
            .await
            .map_err(|error| SdkError::Connect(error.to_string()))?;
        let wait = Duration::from_millis(self.ping_interval_ms.max(10_000));
        loop {
            let message = tokio::time::timeout(wait, ws.next())
                .await
                .map_err(|_| SdkError::Connect("no WELCOME in time".to_owned()))?;
            let bytes = match message {
                Some(Ok(Message::Binary(bytes))) => bytes,
                Some(Ok(Message::Close(_))) | None => {
                    return Err(SdkError::Connect("closed before WELCOME".to_owned()));
                }
                Some(Ok(_)) => continue,
                Some(Err(error)) => return Err(SdkError::Connect(error.to_string())),
            };
            match decode_server(&bytes, usize::MAX) {
                Ok(ServerFrame::Welcome {
                    max_frame_bytes, ..
                }) => {
                    self.max_frame_bytes
                        .store(max_frame_bytes as usize, Ordering::Relaxed);
                    return Ok(ws);
                }
                Ok(ServerFrame::Error { code, msg, .. }) => {
                    return Err(SdkError::Server {
                        code,
                        msg: msg.to_owned(),
                    });
                }
                _ => return Err(SdkError::Connect("no WELCOME".to_owned())),
            }
        }
    }

    async fn run(mut self, first: Ws) {
        let mut next = Some(first);
        let mut backoff_ms = self.reconnect_min_ms;
        loop {
            if let Some(ws) = next.take() {
                backoff_ms = self.reconnect_min_ms;
                match self.session(ws).await {
                    SessionEnd::Finished => return,
                    SessionEnd::Refused(error) => return self.fail_all(error),
                    SessionEnd::Disconnected => {}
                }
            }
            if !self.commands_open && self.nothing_unanswered() {
                return;
            }
            // Commands go on being taken, so that publishes queue up rather than the caller
            // hanging, and only the socket waits.
            let sleep = tokio::time::sleep(Duration::from_millis(backoff_ms));
            tokio::pin!(sleep);
            loop {
                tokio::select! {
                    _ = &mut sleep => break,
                    command = self.commands.recv(), if self.commands_open => {
                        match command {
                            Some(command) => self.apply(command),
                            None => self.commands_open = false,
                        }
                    }
                }
            }
            backoff_ms = (backoff_ms * 2).min(self.reconnect_max_ms);
            match self.connect().await {
                Ok(ws) => next = Some(ws),
                Err(SdkError::Server { code, msg }) => {
                    return self.fail_all(SdkError::Server { code, msg });
                }
                Err(_) => {}
            }
        }
    }

    fn nothing_unanswered(&self) -> bool {
        self.producers
            .values()
            .all(|producer| producer.queue.is_empty())
    }

    fn fail_all(&mut self, error: SdkError) {
        for producer in self.producers.values_mut() {
            for entry in producer.queue.drain(..) {
                let _ = entry.reply.send(Err(error.clone()));
            }
        }
        for (_, waiter) in self.state_waiters.drain() {
            let _ = waiter.send(Err(error.clone()));
        }
        for sub in self.subs.values() {
            let _ = sub.events.send(Err(error.clone()));
        }
    }

    /// Runs one connection to its end. The writer is its own task so that a socket with no room
    /// never stops this one reading, which is what the server may be waiting for.
    async fn session(&mut self, ws: Ws) -> SessionEnd {
        let (sink, mut stream) = ws.split();
        let (out, writes) = mpsc::unbounded_channel();
        self.out = Some(out);
        let writer = tokio::spawn(write_loop(sink, writes));
        self.after_connect();
        let end = self.session_loop(&mut stream).await;
        writer.abort();
        self.out = None;
        self.after_disconnect();
        end
    }

    async fn session_loop(&mut self, stream: &mut SplitStream<Ws>) -> SessionEnd {
        let interval = Duration::from_millis(self.ping_interval_ms.max(1));
        let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        let mut quiet_ticks = 0u32;
        let mut nonce = 0u32;
        loop {
            if !self.commands_open && self.nothing_unanswered() {
                return SessionEnd::Finished;
            }
            tokio::select! {
                biased;
                message = stream.next() => {
                    quiet_ticks = 0;
                    let bytes = match message {
                        Some(Ok(Message::Binary(bytes))) => bytes,
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                            return SessionEnd::Disconnected;
                        }
                        Some(Ok(_)) => continue,
                    };
                    match decode_server(&bytes, usize::MAX) {
                        Ok(frame) => {
                            if let Some(end) = self.on_frame(frame) {
                                return end;
                            }
                        }
                        Err(_) => return SessionEnd::Disconnected,
                    }
                }
                command = self.commands.recv(), if self.commands_open => {
                    match command {
                        Some(command) => self.apply(command),
                        None => self.commands_open = false,
                    }
                }
                _ = ticks.tick() => {
                    quiet_ticks += 1;
                    if quiet_ticks > 2 {
                        return SessionEnd::Disconnected;
                    }
                    nonce = nonce.wrapping_add(1);
                    self.write(&ClientFrame::Ping { nonce });
                }
            }
        }
    }

    fn write(&self, frame: &ClientFrame<'_>) {
        if let (Some(out), Ok(message)) = (&self.out, encode(frame)) {
            let _ = out.send(message);
        }
    }

    fn after_connect(&mut self) {
        let keys: Vec<u32> = self.producers.keys().copied().collect();
        for key in keys {
            self.send_ready(key);
        }
        let keys: Vec<u32> = self.subs.keys().copied().collect();
        for key in keys {
            if !self.subs[&key].paused && !self.subs[&key].ended {
                self.send_subscribe(key);
            }
        }
    }

    fn after_disconnect(&mut self) {
        self.state_handles.clear();
        self.wire_subs.clear();
        for producer in self.producers.values_mut() {
            producer.opened = false;
            for entry in &mut producer.queue {
                entry.sent = false;
            }
        }
        for sub in self.subs.values_mut() {
            sub.wire_sub = None;
        }
        for (_, waiter) in self.state_waiters.drain() {
            let _ = waiter.send(Err(SdkError::Disconnected));
        }
    }

    fn apply(&mut self, command: Command) {
        match command {
            Command::AddProducer {
                key,
                stream,
                producer_id,
            } => {
                self.producers.insert(
                    key,
                    Producer {
                        stream,
                        producer_id,
                        queue: VecDeque::new(),
                        opened: false,
                        dropped: false,
                    },
                );
            }
            Command::DropProducer { key } => {
                if let Some(producer) = self.producers.get_mut(&key) {
                    producer.dropped = true;
                }
                self.forget_if_done(key);
            }
            Command::Publish { key, entry } => {
                if let Some(producer) = self.producers.get_mut(&key) {
                    producer.queue.push_back(entry);
                    self.send_ready(key);
                }
            }
            Command::State {
                stream,
                change,
                reply,
            } => {
                if self.out.is_none() {
                    let _ = reply.send(Err(SdkError::Disconnected));
                    return;
                }
                let handle = match self.state_handles.get(&stream) {
                    Some(handle) => *handle,
                    None => {
                        let handle = self.wire_id();
                        self.write(&ClientFrame::Open {
                            handle,
                            stream: &stream,
                        });
                        self.state_handles.insert(stream, handle);
                        handle
                    }
                };
                let req = self.wire_id();
                self.state_waiters.insert(req, reply);
                self.write(&ClientFrame::StateOp {
                    handle,
                    req,
                    expect_ver: change.expect_ver,
                    op: change.op,
                    doc: &change.doc,
                });
            }
            Command::Subscribe {
                key,
                spec,
                queued_bytes,
                events,
            } => {
                let cursor = spec.from;
                self.subs.insert(
                    key,
                    SubState {
                        spec,
                        wire_sub: None,
                        cursor,
                        committed: 0,
                        paused: false,
                        ended: false,
                        queued_bytes,
                        events,
                    },
                );
                if self.out.is_some() {
                    self.send_subscribe(key);
                }
            }
            Command::Unsubscribe { key } => {
                if let Some(sub) = self.subs.remove(&key)
                    && let Some(wire) = sub.wire_sub
                {
                    self.wire_subs.remove(&wire);
                    self.write(&ClientFrame::Unsubscribe { sub: wire });
                }
            }
            Command::Commit { key, id } => {
                if let Some(sub) = self.subs.get_mut(&key) {
                    sub.committed = sub.committed.max(id);
                    if let Some(wire) = sub.wire_sub {
                        self.write(&ClientFrame::Commit { sub: wire, id });
                    }
                }
            }
            Command::Drained { key } => {
                if let Some(sub) = self.subs.get_mut(&key)
                    && sub.paused
                    && !sub.ended
                {
                    sub.paused = false;
                    if self.out.is_some() {
                        self.send_subscribe(key);
                    }
                }
            }
            Command::SetToken(token) => {
                self.write(&ClientFrame::Hello {
                    ver: PROTOCOL_VERSION,
                    token: &token,
                });
                self.token = token;
            }
        }
    }

    fn forget_if_done(&mut self, key: u32) {
        if self
            .producers
            .get(&key)
            .is_some_and(|producer| producer.dropped && producer.queue.is_empty())
        {
            self.producers.remove(&key);
        }
    }

    /// Sends what may go now: in order, stopping behind an unanswered `expect_ver`.
    fn send_ready(&mut self, key: u32) {
        let Some(out) = self.out.clone() else {
            return;
        };
        let Some(producer) = self.producers.get_mut(&key) else {
            return;
        };
        if !producer.opened && !producer.queue.is_empty() {
            let open = encode(&ClientFrame::Open {
                handle: key,
                stream: &producer.stream,
            });
            if let Ok(open) = open {
                let _ = out.send(open);
            }
            producer.opened = true;
        }
        let producer_id = producer.producer_id;
        let mut at = 0;
        while at < producer.queue.len() {
            let entry = &mut producer.queue[at];
            if entry.sent {
                if entry.blocks_later() {
                    break;
                }
                at += 1;
                continue;
            }
            let message = encode(&ClientFrame::Publish(proto::Publish {
                handle: key,
                producer_id,
                producer_seq: entry.seq,
                ack: entry.ack,
                state: entry.state.as_ref().map(|state| PublishState {
                    expect_ver: state.expect_ver,
                    op: state.op,
                    doc: &state.doc,
                }),
                records: &entry.records,
            }));
            let Ok(message) = message else {
                let entry = producer.queue.remove(at).unwrap();
                let _ = entry.reply.send(Err(SdkError::TooLarge));
                continue;
            };
            let _ = out.send(message);
            entry.sent = true;
            if entry.ack == AckLevel::None {
                // Level 0: nothing will answer, and nothing is resent.
                let entry = producer.queue.remove(at).unwrap();
                let _ = entry.reply.send(Ok(Ack {
                    producer_seq: entry.seq,
                    first_id: 0,
                    count: 0,
                }));
                continue;
            }
            if entry.blocks_later() {
                break;
            }
            at += 1;
        }
        self.forget_if_done(key);
    }

    fn send_subscribe(&mut self, key: u32) {
        let wire = self.wire_id();
        let Some(sub) = self.subs.get_mut(&key) else {
            return;
        };
        sub.wire_sub = Some(wire);
        self.wire_subs.insert(wire, key);
        let clauses = WireClauses::new(&sub.spec.clauses).unwrap_or_default();
        let frame = ClientFrame::Subscribe(proto::Subscribe {
            sub: wire,
            from: sub.cursor,
            linger_ms: sub.spec.linger_ms,
            author: sub.spec.author,
            clauses,
            stream: &sub.spec.stream,
            name: sub.spec.name.as_deref(),
            with_state: sub.spec.with_state,
            state_only: sub.spec.state_only,
            include_system: sub.spec.include_system,
        });
        let committed = sub.committed;
        if let (Some(out), Ok(message)) = (&self.out, encode(&frame)) {
            let _ = out.send(message);
        }
        // The commit that got lost with the connection, if one did.
        if committed != 0 {
            self.write(&ClientFrame::Commit {
                sub: wire,
                id: committed,
            });
        }
    }

    fn on_frame(&mut self, frame: ServerFrame<'_>) -> Option<SessionEnd> {
        match frame {
            ServerFrame::Ack {
                handle,
                producer_seq,
                first_id,
                count,
            } => self.answer(
                handle,
                producer_seq,
                Ok(Ack {
                    producer_seq,
                    first_id,
                    count,
                }),
            ),
            ServerFrame::Nack {
                handle,
                producer_seq,
                code,
                msg,
            } => {
                let error = if code == ErrorCode::VersionConflict {
                    SdkError::VersionConflict { ver: 0 }
                } else {
                    SdkError::Server {
                        code,
                        msg: msg.to_owned(),
                    }
                };
                self.answer(handle, producer_seq, Err(error));
            }
            ServerFrame::StateResult { req, ok, ver } => {
                if let Some(waiter) = self.state_waiters.remove(&req) {
                    let _ = waiter.send(if ok {
                        Ok(ver)
                    } else {
                        Err(SdkError::VersionConflict { ver })
                    });
                }
            }
            ServerFrame::Subscribed {
                sub,
                head,
                state_ver,
                state,
            } => {
                if let Some(entry) = self.sub_of(sub) {
                    // A live-only start has a position now, for resuming without a gap.
                    if entry.cursor == FROM_LIVE {
                        entry.cursor = head;
                    }
                    let _ = entry.events.send(Ok(Raw::Subscribed {
                        head,
                        state_ver,
                        state: state.to_vec(),
                    }));
                }
            }
            ServerFrame::Data { sub, records } => {
                let copied = Bytes::copy_from_slice(records);
                if let Some(entry) = self.sub_of(sub) {
                    match last_id(&copied) {
                        Ok(Some(last)) => entry.cursor = entry.cursor.max(last),
                        Ok(None) => {}
                        Err(_) => return Some(SessionEnd::Disconnected),
                    }
                    let queued = entry
                        .queued_bytes
                        .fetch_add(copied.len(), Ordering::Relaxed)
                        + copied.len();
                    let _ = entry.events.send(Ok(Raw::Data(copied)));
                    // Dropped until `Drained` brings it back from its cursor.
                    if queued > entry.spec.max_queue_bytes {
                        entry.paused = true;
                        entry.wire_sub = None;
                        self.wire_subs.remove(&sub);
                        self.write(&ClientFrame::Unsubscribe { sub });
                    }
                }
            }
            ServerFrame::Progress { sub, id } => {
                if let Some(entry) = self.sub_of(sub) {
                    entry.cursor = entry.cursor.max(id);
                    let _ = entry.events.send(Ok(Raw::Progress(id)));
                }
            }
            ServerFrame::State { sub, ver, doc } => {
                if let Some(entry) = self.sub_of(sub) {
                    let _ = entry.events.send(Ok(Raw::State {
                        ver,
                        doc: doc.to_vec(),
                    }));
                }
            }
            ServerFrame::Behind { sub, resume_from } => {
                if let Some(entry) = self.sub_of(sub) {
                    let _ = entry.events.send(Ok(Raw::Behind(resume_from)));
                }
            }
            ServerFrame::Error {
                reference,
                code,
                msg,
            } => return self.on_error(reference, code, msg),
            ServerFrame::Welcome {
                max_frame_bytes, ..
            } => {
                // The answer to a renewal `HELLO`.
                self.max_frame_bytes
                    .store(max_frame_bytes as usize, Ordering::Relaxed);
            }
            ServerFrame::Pong { .. } | ServerFrame::Redirect { .. } => {}
        }
        None
    }

    fn sub_of(&mut self, wire: u32) -> Option<&mut SubState> {
        let key = self.wire_subs.get(&wire)?;
        self.subs.get_mut(key)
    }

    fn answer(&mut self, handle: u32, seq: u64, result: Result<Ack, SdkError>) {
        let Some(producer) = self.producers.get_mut(&handle) else {
            return;
        };
        // An ack for a level-0 publish the stream raised has no entry, and that's fine.
        if let Some(at) = producer.queue.iter().position(|entry| entry.seq == seq) {
            let entry = producer.queue.remove(at).unwrap();
            let _ = entry.reply.send(result);
        }
        self.send_ready(handle);
    }

    fn on_error(&mut self, reference: u32, code: ErrorCode, msg: &str) -> Option<SessionEnd> {
        let error = SdkError::Server {
            code,
            msg: msg.to_owned(),
        };
        if reference == 0 {
            // The connection's own: a token that ended or a frame it wouldn't take.
            if code == ErrorCode::Unauthorized {
                return Some(SessionEnd::Refused(error));
            }
            return None;
        }
        if let Some(waiter) = self.state_waiters.remove(&reference) {
            let _ = waiter.send(Err(error));
            return None;
        }
        if let Some(key) = self.wire_subs.get(&reference).copied() {
            let keep = code == ErrorCode::Malformed;
            if let Some(sub) = self.subs.get_mut(&key) {
                let _ = sub.events.send(Err(error));
                if !keep {
                    sub.wire_sub = None;
                    sub.ended = true;
                }
            }
            if !keep {
                self.wire_subs.remove(&reference);
            }
            return None;
        }
        if let Some(producer) = self.producers.get_mut(&reference) {
            // Its handle was refused: nothing it sent on it can be answered otherwise.
            producer.opened = false;
            let mut kept = VecDeque::new();
            for entry in producer.queue.drain(..) {
                if entry.sent {
                    let _ = entry.reply.send(Err(error.clone()));
                } else {
                    kept.push_back(entry);
                }
            }
            producer.queue = kept;
            self.send_ready(reference);
            return None;
        }
        // A state handle: its op's own answer comes as well, or the next op's does.
        let mut failed = None;
        for (stream, handle) in &self.state_handles {
            if *handle == reference {
                failed = Some(stream.clone());
            }
        }
        if let Some(stream) = failed {
            self.state_handles.remove(&stream);
        }
        None
    }
}

async fn write_loop(
    mut sink: SplitSink<Ws, Message>,
    mut writes: mpsc::UnboundedReceiver<Message>,
) {
    while let Some(message) = writes.recv().await {
        if sink.feed(message).await.is_err() {
            return;
        }
        while let Ok(message) = writes.try_recv() {
            if sink.feed(message).await.is_err() {
                return;
            }
        }
        if sink.flush().await.is_err() {
            return;
        }
    }
}
