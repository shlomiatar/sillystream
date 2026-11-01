//! The server's sockets (SPEC section 11): one TLS 1.3 port with HTTP and WebSocket (D22).
//!
//! The listener hands each connection to a core round robin (D24), and it lives there as one
//! task. It holds no read buffer between reads: it waits for the socket to be readable,
//! borrows a part of the core's current slab without yielding, and hands back what the read
//! didn't fill (section 6.5). So a publish whose frame came whole is read straight into the
//! slab that takes it to the WAL and S3, and is unmasked in place (D23). One still arriving
//! waits in its own buffer until then (D121, D130).

mod conn;
pub(crate) mod http;
pub mod json;
mod tls;
pub mod ws;
mod wsconn;

use std::cell::RefCell;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use object_store::ObjectStore;
use storage::WalVolume;
use tokio::sync::Notify;

use crate::clock::Clock;
use crate::node::{Node, NodeConfig, NodeExit, NodeHandle};
use crate::recovery::{RecoverConfig, RecoveryReport};
use crate::slab::SlabPool;

pub use tls::tls13_only_config;

/// A signing secret shorter than this is refused at the start (D47).
pub const MIN_TOKEN_SECRET_BYTES: usize = 32;

/// What the binary or a test starts a server with.
#[derive(Clone)]
pub struct ServerConfig {
    pub node: NodeConfig,
    pub listen: SocketAddr,
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
    pub token_secret: Vec<u8>,
}

/// What the sockets count, summed over the cores.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoStats {
    /// How many times a connection borrowed part of a slab to read into.
    pub read_borrow_count: u64,
    /// Buffers connections hold across a wait: a frame or request partly read, or frames
    /// waiting for a full core. An idle connection holds none.
    pub held_buffer_count: u64,
    pub open_connection_count: u64,
    pub accepted_count: u64,
    pub handshake_failed_count: u64,
    /// Closed on accept for being past `max_connections`.
    pub refused_connection_count: u64,
}

#[derive(Default)]
pub(crate) struct IoCounters {
    pub read_borrow_count: AtomicU64,
    pub held_buffer_count: AtomicU64,
    pub open_connection_count: AtomicU64,
    pub accepted_count: AtomicU64,
    pub handshake_failed_count: AtomicU64,
}

/// What every connection shares. Read only.
pub(crate) struct Shared {
    pub secret: Vec<u8>,
    pub clock: Arc<dyn Clock>,
    pub node: NodeHandle,
    pub max_frame_bytes: u64,
    pub max_subscriptions: usize,
    pub partial_frame_timeout_ms: u64,
    pub progress_interval_ms: u64,
    pub acceptor: tokio_rustls::TlsAcceptor,
    pub counters: Vec<IoCounters>,
    pub session_count: AtomicU64,
    pub max_connections: u64,
    pub connection_count: AtomicU64,
    pub refused_connection_count: AtomicU64,
    /// Bytes of HTTP read responses waiting on their clients, on every core. It is only added
    /// up, so no core waits on another for it.
    pub held_response_bytes: Arc<AtomicU64>,
    pub held_response_cap_bytes: u64,
}

/// The slab a core's sockets read into. It is per thread, so it needs no lock, and connection
/// tasks only touch it between awaits.
pub(crate) struct CoreIo {
    pub pool: SlabPool,
    pub scratch: Vec<u8>,
}

thread_local! {
    static CORE_IO: RefCell<Option<CoreIo>> = const { RefCell::new(None) };
}

pub(crate) fn with_core_io<R>(
    shared: &Shared,
    home: usize,
    work: impl FnOnce(&mut CoreIo) -> R,
) -> R {
    CORE_IO.with_borrow_mut(|io| {
        let io = io.get_or_insert_with(|| CoreIo {
            pool: shared.node.read_pool(home),
            scratch: Vec::new(),
        });
        work(io)
    })
}

/// A running server: a node with a listener in front of it.
pub struct Server {
    local_addr: SocketAddr,
    node: Option<Node>,
    shared: Arc<Shared>,
    stop: Arc<Notify>,
    listener: Option<JoinHandle<()>>,
}

impl Server {
    /// Starts the node, then listens. This binds before it returns, so `local_addr` works at
    /// once, including for port 0.
    pub fn start<V: WalVolume + 'static>(
        config: ServerConfig,
        volume: V,
        store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
    ) -> io::Result<Server> {
        let start = move |node_config, clock| Node::start(node_config, volume, store, clock);
        Server::start_with(config, clock, start)
    }

    /// As `start`, after the replay and ownership step of section 8.1. What that found comes
    /// back with the server, for the caller to log.
    pub fn start_recovering(
        config: ServerConfig,
        recover: RecoverConfig,
        store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
    ) -> io::Result<(Server, RecoveryReport)> {
        let mut report = None;
        let server = Server::start_with(config, clock, |node_config, clock| {
            let (node, found) = Node::recover(node_config, recover, store, clock)?;
            report = Some(found);
            Ok(node)
        })?;
        Ok((server, report.expect("a started server recovered first")))
    }

    fn start_with(
        config: ServerConfig,
        clock: Arc<dyn Clock>,
        start: impl FnOnce(crate::NodeConfig, Arc<dyn Clock>) -> io::Result<Node>,
    ) -> io::Result<Server> {
        if config.token_secret.len() < MIN_TOKEN_SECRET_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("the token secret must be at least {MIN_TOKEN_SECRET_BYTES} bytes"),
            ));
        }
        let tls_config = tls13_only_config(&config.cert_pem, &config.key_pem)?;
        let listener = bind_listener(config.listen, config.node.listen_backlog)?;
        let local_addr = listener.local_addr()?;
        let max_connections = config.node.max_connections;
        let max_frame_bytes = config.node.max_frame_bytes;
        let max_subscriptions = config.node.max_subscriptions_per_connection;
        let partial_frame_timeout_ms = config.node.partial_frame_timeout_ms;
        let progress_interval_ms = config.node.progress_interval_ms;
        let core_count = config.node.core_count;
        let held_response_cap_bytes = config.node.memory_cap_bytes / 4;
        let node = start(config.node, Arc::clone(&clock))?;
        let shared = Arc::new(Shared {
            secret: config.token_secret,
            clock,
            node: node.handle(),
            max_frame_bytes,
            max_subscriptions,
            partial_frame_timeout_ms,
            progress_interval_ms,
            acceptor: tokio_rustls::TlsAcceptor::from(Arc::new(tls_config)),
            counters: (0..core_count).map(|_| IoCounters::default()).collect(),
            session_count: AtomicU64::new(0),
            max_connections,
            connection_count: AtomicU64::new(0),
            refused_connection_count: AtomicU64::new(0),
            held_response_bytes: Arc::new(AtomicU64::new(0)),
            held_response_cap_bytes,
        });
        let stop = Arc::new(Notify::new());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let thread = {
            let shared = Arc::clone(&shared);
            let stop = Arc::clone(&stop);
            std::thread::Builder::new()
                .name("listener".to_owned())
                .spawn(move || {
                    runtime.block_on(accept_loop(listener, shared, stop));
                })?
        };
        Ok(Server {
            local_addr,
            node: Some(node),
            shared,
            stop,
            listener: Some(thread),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn node(&self) -> &Node {
        self.node.as_ref().expect("taken only when stopping")
    }

    pub fn io_stats(&self) -> IoStats {
        let mut stats = IoStats::default();
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        for counters in &self.shared.counters {
            stats.read_borrow_count += load(&counters.read_borrow_count);
            stats.held_buffer_count += load(&counters.held_buffer_count);
            stats.open_connection_count += load(&counters.open_connection_count);
            stats.accepted_count += load(&counters.accepted_count);
            stats.handshake_failed_count += load(&counters.handshake_failed_count);
        }
        stats.refused_connection_count = load(&self.shared.refused_connection_count);
        stats
    }

    /// Slab bytes alive on each core, which is what the memory cap looks at.
    pub fn pinned_bytes(&self) -> u64 {
        (0..self.shared.node.core_count())
            .map(|core| self.shared.node.account(core).pinned_bytes())
            .sum()
    }

    /// Seals what's open and saves its metadata, waiting up to `wait_ms`, so a clean stop leaves
    /// little to replay. False if that didn't finish, which a restart makes up for.
    pub fn seal_before_stop(&self, wait_ms: u64) -> bool {
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
        else {
            return false;
        };
        runtime.block_on(async {
            let wait = std::time::Duration::from_millis(wait_ms);
            tokio::time::timeout(wait, self.node().seal_and_checkpoint())
                .await
                .unwrap_or(false)
        })
    }

    /// Stops listening, then stops the node. Connections end with their cores.
    pub fn shutdown(mut self) -> NodeExit {
        self.stop_listener();
        self.node
            .take()
            .expect("taken only here, by value")
            .shutdown()
    }

    fn stop_listener(&mut self) {
        self.stop.notify_one();
        if let Some(thread) = self.listener.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop_listener();
    }
}

fn bind_listener(listen: SocketAddr, backlog: i32) -> io::Result<std::net::TcpListener> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(listen),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.bind(&listen.into())?;
    socket.listen(backlog)?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

/// One of `max_connections`, held for as long as the connection lives.
struct ConnectionSlot(Arc<Shared>);

impl ConnectionSlot {
    fn take(shared: &Arc<Shared>) -> Option<ConnectionSlot> {
        shared
            .connection_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                (count < shared.max_connections).then_some(count + 1)
            })
            .ok()?;
        Some(ConnectionSlot(Arc::clone(shared)))
    }
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        self.0.connection_count.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn accept_loop(listener: std::net::TcpListener, shared: Arc<Shared>, stop: Arc<Notify>) {
    let Ok(listener) = tokio::net::TcpListener::from_std(listener) else {
        return;
    };
    let mut next_core = 0;
    loop {
        tokio::select! {
            biased;
            _ = stop.notified() => return,
            accepted = listener.accept() => match accepted {
                Ok((socket, _)) => {
                    let Some(slot) = ConnectionSlot::take(&shared) else {
                        shared.refused_connection_count.fetch_add(1, Ordering::Relaxed);
                        continue;
                    };
                    let _ = socket.set_nodelay(true);
                    let Ok(socket) = socket.into_std() else {
                        continue;
                    };
                    let home = next_core;
                    next_core = (next_core + 1) % shared.node.core_count();
                    shared.counters[home].accepted_count.fetch_add(1, Ordering::Relaxed);
                    let task_shared = Arc::clone(&shared);
                    shared.node.run_on_core(home, Box::new(move || {
                        tokio::spawn(async move {
                            let _slot = slot;
                            conn::serve(task_shared, home, socket).await;
                        });
                    }));
                }
                // Out of descriptors, most likely. It passes when a connection ends.
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            },
        }
    }
}
