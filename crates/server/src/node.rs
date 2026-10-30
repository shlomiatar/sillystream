//! A node: its cores, the WAL writer, the uploader, and a control thread that runs what
//! is node-wide: the seal window (section 7.1), rebalancing (section 6.1), and the
//! in-process API that later milestones put sockets in front of.

use std::collections::HashMap;
use std::io;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;
use proto::ErrorCode;
use storage::{
    DurableWatch, StreamExtent, UploadEvent, UploadKey, Uploader, UploaderConfig, WalConfig,
    WalError, WalHandle, WalSendError, WalSender, WalStats, WalVolume, durable_watches,
    start_wal_writer,
};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::clock::Clock;
use crate::core::{Core, CoreConfig, CoreInbox, CoreParts, CoreToNode, Links};
use crate::message::{
    ControlMsg, CoreStats, PublishMsg, Reply, SnapshotReply, StateChange, StateOpMsg, StreamConfig,
    WriteMsg,
};
use crate::placement::{ShardMove, plan_moves, shard_of};
use crate::slab::{SlabAccount, SlabBytes, SlabHook, SlabPool};
use crate::state::StateOp;

#[derive(Clone)]
pub struct NodeConfig {
    pub node_name: String,
    pub generation: u64,
    pub last_valid_id: u64,
    pub core_count: usize,
    pub virtual_shard_count: u32,
    /// How many streams can have a shard of their own.
    pub override_shard_count: u32,
    pub slab_bytes: usize,
    /// Slab bytes alive per client home core, above which publishers wait (D7, Q9).
    pub memory_cap_bytes: u64,
    pub wal_channel_capacity: usize,
    pub upload_channel_capacity: usize,
    pub write_inbox_capacity: usize,
    /// `None` seals only when asked, or at the size cap.
    pub seal_window_ms: Option<u64>,
    /// Each core asks for a seal at its share of this (Q18, D64).
    pub segment_cap_bytes: u64,
    /// A stream with this much unsealed gets a dedicated object.
    pub stream_buffer_bytes: u64,
    pub upload_retry_initial_ms: u64,
    pub upload_retry_max_ms: u64,
    /// `None` rebalances only when asked.
    pub rebalance_period_ms: Option<u64>,
    /// A core is overloaded above this × the mean.
    pub overload_ratio: f64,
    pub max_moves_per_check: usize,
    pub slab_hook: Option<Arc<dyn SlabHook>>,
}

impl NodeConfig {
    pub fn new(core_count: usize) -> NodeConfig {
        NodeConfig {
            node_name: "node".to_owned(),
            generation: 1,
            last_valid_id: 0,
            core_count,
            virtual_shard_count: 4096,
            override_shard_count: 64,
            slab_bytes: 4 * 1024 * 1024,
            memory_cap_bytes: 4 * 1024 * 1024 * 1024,
            wal_channel_capacity: 4096,
            upload_channel_capacity: 1024,
            write_inbox_capacity: 1024,
            seal_window_ms: Some(10_000),
            segment_cap_bytes: 64 * 1024 * 1024,
            stream_buffer_bytes: 32 * 1024 * 1024,
            upload_retry_initial_ms: 100,
            upload_retry_max_ms: 10_000,
            rebalance_period_ms: Some(10_000),
            overload_ratio: 1.25,
            max_moves_per_check: 16,
            slab_hook: None,
        }
    }
}

/// How one seal ended.
#[derive(Debug, Clone)]
pub enum SealOutcome {
    Uploaded {
        seal: u64,
        path: Path,
        len_bytes: u64,
        extents: Arc<Vec<StreamExtent>>,
    },
    /// No core had any records, and no object was written.
    Empty {
        seal: u64,
    },
    Failed {
        seal: u64,
        error: String,
    },
    /// The WAL writer has stopped, so no seal can begin.
    WalStopped,
    NodeStopped,
}

enum NodeMsg {
    FromCore(CoreToNode),
    Seal {
        reply: oneshot::Sender<SealOutcome>,
    },
    RetryRotate,
    Rotated {
        seal: u64,
        ok: bool,
    },
    Rebalance {
        reply: oneshot::Sender<Vec<ShardMove>>,
    },
    MoveShard {
        shard: u32,
        to: usize,
        reply: oneshot::Sender<()>,
    },
    SetOverride {
        stream: Arc<str>,
        to: usize,
        reply: oneshot::Sender<Option<u32>>,
    },
    Placement {
        reply: oneshot::Sender<Vec<u16>>,
    },
    Stop,
}

/// The node-wide jobs. It owns the only complete placement table, which it changes when a
/// move has finished. Everything it asks of a core is a message.
struct Control {
    config: NodeConfig,
    links: Links,
    wal: WalSender<SlabBytes>,
    tx: mpsc::UnboundedSender<NodeMsg>,
    location: Vec<u16>,
    overrides: HashMap<Arc<str>, u32>,
    seal_count: u64,
    rotating: bool,
    seal_wanted: bool,
    seal_waiters: Vec<oneshot::Sender<SealOutcome>>,
    waiters_by_seal: HashMap<u64, Vec<oneshot::Sender<SealOutcome>>>,
}

impl Control {
    async fn run(
        mut self,
        mut rx: mpsc::UnboundedReceiver<NodeMsg>,
        mut core_rx: mpsc::UnboundedReceiver<CoreToNode>,
        mut uploads: mpsc::UnboundedReceiver<UploadEvent>,
    ) {
        let after = |period_ms: Option<u64>| {
            period_ms.map(|period_ms| Instant::now() + Duration::from_millis(period_ms))
        };
        let mut next_seal = after(self.config.seal_window_ms);
        let mut next_rebalance = after(self.config.rebalance_period_ms);
        loop {
            tokio::select! {
                biased;
                message = rx.recv() => match message {
                    Some(NodeMsg::Stop) | None => break,
                    Some(message) => self.on_message(message).await,
                },
                message = core_rx.recv() => {
                    if let Some(message) = message {
                        self.on_message(NodeMsg::FromCore(message)).await;
                    }
                },
                event = uploads.recv() => {
                    if let Some(event) = event {
                        self.on_upload_event(event);
                    }
                },
                _ = sleep_until_some(next_seal) => {
                    next_seal = after(self.config.seal_window_ms);
                    self.seal_wanted = true;
                    self.begin_seal();
                },
                _ = sleep_until_some(next_rebalance) => {
                    next_rebalance = after(self.config.rebalance_period_ms);
                    self.rebalance().await;
                },
            }
        }
        for (_, waiters) in self.waiters_by_seal.drain() {
            for waiter in waiters {
                let _ = waiter.send(SealOutcome::NodeStopped);
            }
        }
    }

    async fn on_message(&mut self, message: NodeMsg) {
        match message {
            NodeMsg::FromCore(CoreToNode::SealRequested) => {
                self.seal_wanted = true;
                self.begin_seal();
            }
            NodeMsg::Seal { reply } => {
                self.seal_waiters.push(reply);
                self.seal_wanted = true;
                self.begin_seal();
            }
            NodeMsg::RetryRotate => {
                self.rotating = false;
                self.begin_seal();
            }
            NodeMsg::Rotated { seal, ok } => {
                self.rotating = false;
                if ok {
                    for control in &self.links.control {
                        let _ = control.send(ControlMsg::Seal { seal });
                    }
                    self.begin_seal();
                } else {
                    self.fail_seal_waiters(seal);
                }
            }
            NodeMsg::Rebalance { reply } => {
                let _ = reply.send(self.rebalance().await);
            }
            NodeMsg::MoveShard { shard, to, reply } => {
                self.move_shard(shard, to).await;
                let _ = reply.send(());
            }
            NodeMsg::SetOverride { stream, to, reply } => {
                let _ = reply.send(self.set_override(stream, to).await);
            }
            NodeMsg::Placement { reply } => {
                let _ = reply.send(self.location.clone());
            }
            NodeMsg::Stop => {}
        }
    }

    /// Rotate first, then each core seals what it holds once it hears (D56). A rotation
    /// waits on the writer, so it runs on the side, and control keeps answering.
    fn begin_seal(&mut self) {
        if self.rotating || !self.seal_wanted {
            return;
        }
        match self.wal.try_rotate() {
            Ok(done) => {
                self.rotating = true;
                self.seal_wanted = false;
                self.seal_count += 1;
                let seal = self.seal_count;
                self.waiters_by_seal
                    .insert(seal, std::mem::take(&mut self.seal_waiters));
                let tx = self.tx.clone();
                tokio::spawn(async move {
                    let ok = done.await.is_ok();
                    let _ = tx.send(NodeMsg::Rotated { seal, ok });
                });
            }
            Err(WalSendError::Full(())) => {
                self.rotating = true;
                let tx = self.tx.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    let _ = tx.send(NodeMsg::RetryRotate);
                });
            }
            Err(WalSendError::Stopped(())) => {
                self.seal_wanted = false;
                for waiter in self.seal_waiters.drain(..) {
                    let _ = waiter.send(SealOutcome::WalStopped);
                }
            }
        }
    }

    fn fail_seal_waiters(&mut self, seal: u64) {
        for waiter in self.waiters_by_seal.remove(&seal).unwrap_or_default() {
            let _ = waiter.send(SealOutcome::WalStopped);
        }
    }

    fn on_upload_event(&mut self, event: UploadEvent) {
        let (seal, outcome) = match event {
            UploadEvent::Uploaded {
                key: UploadKey::Seal(seal),
                path,
                len_bytes,
                extents,
            } => (
                seal,
                SealOutcome::Uploaded {
                    seal,
                    path,
                    len_bytes,
                    extents,
                },
            ),
            UploadEvent::Empty {
                key: UploadKey::Seal(seal),
            } => (seal, SealOutcome::Empty { seal }),
            UploadEvent::Failed {
                key: UploadKey::Seal(seal),
                error,
            } => (seal, SealOutcome::Failed { seal, error }),
            _ => return,
        };
        for waiter in self.waiters_by_seal.remove(&seal).unwrap_or_default() {
            let _ = waiter.send(outcome.clone());
        }
    }

    async fn move_shard(&mut self, shard: u32, to: usize) {
        let from = self.location[shard as usize] as usize;
        if from == to {
            return;
        }
        let (done, finished) = oneshot::channel();
        let sent = self.links.control[from].send(ControlMsg::MoveShard { shard, to, done });
        if sent.is_ok() && finished.await.is_ok() {
            self.location[shard as usize] = to as u16;
        }
    }

    async fn rebalance(&mut self) -> Vec<ShardMove> {
        let mut shards = Vec::new();
        for (core, control) in self.links.control.iter().enumerate() {
            let (reply, report) = oneshot::channel();
            if control.send(ControlMsg::ReportLoad { reply }).is_err() {
                return Vec::new();
            }
            let Ok(loads) = report.await else {
                return Vec::new();
            };
            shards.extend(loads.into_iter().map(|(shard, load)| (shard, core, load)));
        }
        // Each core's report is in its own map's order.
        shards.sort_by_key(|(shard, _, _)| *shard);
        let moves = plan_moves(
            &shards,
            self.config.core_count,
            self.config.overload_ratio,
            self.config.max_moves_per_check,
        );
        for planned in &moves {
            self.move_shard(planned.shard, planned.to).await;
        }
        moves
    }

    async fn set_override(&mut self, stream: Arc<str>, to: usize) -> Option<u32> {
        if let Some(shard) = self.overrides.get(&*stream).copied() {
            self.move_shard(shard, to).await;
            return Some(shard);
        }
        let used = self.overrides.len() as u32;
        if used >= self.config.override_shard_count {
            return None;
        }
        let new_shard = self.config.virtual_shard_count + used;
        let at = self.location[shard_of(&stream, self.config.virtual_shard_count) as usize];
        let (done, finished) = oneshot::channel();
        self.links.control[at as usize]
            .send(ControlMsg::SplitStream {
                stream: Arc::clone(&stream),
                new_shard,
                to,
                done,
            })
            .ok()?;
        finished.await.ok()?;
        self.overrides.insert(stream, new_shard);
        self.location[new_shard as usize] = to as u16;
        Some(new_shard)
    }
}

async fn sleep_until_some(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// What a finished node did.
#[derive(Debug)]
pub struct NodeExit {
    pub wal: Result<WalStats, WalError>,
    /// Uploads that didn't finish. Their records are in the WAL for a restart to replay.
    pub unfinished_upload_count: usize,
}

pub struct Node {
    config: NodeConfig,
    links: Links,
    to_control: mpsc::UnboundedSender<NodeMsg>,
    accounts: Vec<SlabAccount>,
    threads: Vec<JoinHandle<()>>,
    client_count: AtomicU64,
    uploader: Option<Uploader<SlabBytes>>,
    wal: Option<WalHandle>,
}

fn runtime() -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
}

impl Node {
    /// Starts the WAL writer on `volume`, the uploader on `store`, and every core.
    pub fn start<V: WalVolume + 'static>(
        config: NodeConfig,
        volume: V,
        store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
    ) -> io::Result<Node> {
        let core_count = config.core_count;
        let (notify, durable): (DurableWatch, _) = durable_watches(core_count);
        let (wal, wal_handle) = start_wal_writer::<SlabBytes, _, _>(
            volume,
            notify,
            WalConfig {
                first_seq: 1,
                closed_seqs: Vec::new(),
                channel_capacity: config.wal_channel_capacity,
            },
        )?;

        let mut upload_txs = Vec::with_capacity(core_count + 1);
        let mut upload_rxs = Vec::with_capacity(core_count);
        for _ in 0..core_count {
            let (tx, rx) = mpsc::unbounded_channel();
            upload_txs.push(tx);
            upload_rxs.push(rx);
        }
        let (control_uploads_tx, control_uploads) = mpsc::unbounded_channel();
        upload_txs.push(control_uploads_tx);
        let (upload_sender, uploader) = Uploader::<SlabBytes>::start(UploaderConfig {
            store,
            node: config.node_name.clone(),
            generation: config.generation,
            section_count: core_count,
            channel_capacity: config.upload_channel_capacity,
            retry_initial_ms: config.upload_retry_initial_ms,
            retry_max_ms: config.upload_retry_max_ms,
            cache: None,
            events: upload_txs,
        })?;

        let mut links = Links {
            control: Vec::new(),
            forwarded: Vec::new(),
            direct: Vec::new(),
        };
        let mut inboxes = Vec::new();
        for (durable, uploads) in durable.into_iter().zip(upload_rxs) {
            let (control_tx, control) = mpsc::unbounded_channel();
            let (forwarded_tx, forwarded) = mpsc::unbounded_channel();
            let (direct_tx, direct) = mpsc::channel(config.write_inbox_capacity);
            links.control.push(control_tx);
            links.forwarded.push(forwarded_tx);
            links.direct.push(direct_tx);
            inboxes.push(CoreInbox {
                control,
                forwarded,
                direct,
                durable,
                uploads,
            });
        }

        let (to_control, control_rx) = mpsc::unbounded_channel();
        let (core_to_node, core_rx) = mpsc::unbounded_channel();
        let accounts: Vec<SlabAccount> = (0..core_count).map(|_| SlabAccount::new()).collect();
        let mut threads = Vec::with_capacity(core_count + 1);
        for (id, inbox) in inboxes.into_iter().enumerate() {
            let core = Core::new(CoreParts {
                config: CoreConfig {
                    id,
                    virtual_shard_count: config.virtual_shard_count,
                    override_shard_count: config.override_shard_count,
                    core_count,
                    segment_cap_share_bytes: config.segment_cap_bytes / core_count as u64,
                    stream_buffer_bytes: config.stream_buffer_bytes,
                    last_valid_id: config.last_valid_id,
                },
                clock: Arc::clone(&clock),
                links: links.clone(),
                inbox,
                wal: wal.clone(),
                uploader: upload_sender.clone(),
                to_node: core_to_node.clone(),
                pool: SlabPool::new(
                    config.slab_bytes,
                    accounts[id].clone(),
                    config.slab_hook.clone(),
                    (id as u64 + 1) << 40,
                ),
            });
            let runtime = runtime()?;
            threads.push(
                std::thread::Builder::new()
                    .name(format!("core-{id}"))
                    .spawn(move || runtime.block_on(core.run()))?,
            );
        }
        // Keeps its own copy of what a seal or rebalance needs.
        drop(core_to_node);
        drop(upload_sender);

        let total_shards = config.virtual_shard_count + config.override_shard_count;
        let location = (0..total_shards)
            .map(|shard| {
                if shard < config.virtual_shard_count {
                    (shard as usize % core_count) as u16
                } else {
                    u16::MAX
                }
            })
            .collect();
        let control = Control {
            config: config.clone(),
            links: links.clone(),
            wal,
            tx: to_control.clone(),
            location,
            overrides: HashMap::new(),
            seal_count: 0,
            rotating: false,
            seal_wanted: false,
            seal_waiters: Vec::new(),
            waiters_by_seal: HashMap::new(),
        };
        let runtime = runtime()?;
        threads.push(
            std::thread::Builder::new()
                .name("node-control".to_owned())
                .spawn(move || {
                    runtime.block_on(control.run(control_rx, core_rx, control_uploads))
                })?,
        );

        Ok(Node {
            config,
            links,
            to_control,
            accounts,
            threads,
            client_count: AtomicU64::new(0),
            uploader: Some(uploader),
            wal: Some(wal_handle),
        })
    }

    pub fn core_count(&self) -> usize {
        self.config.core_count
    }

    pub fn virtual_shard_count(&self) -> u32 {
        self.config.virtual_shard_count
    }

    /// A client living on `home_core`, as a connection would (D24).
    pub async fn client(&self, home_core: usize) -> Client {
        let location_hint = self.placement().await;
        let client_number = self.client_count.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = mpsc::unbounded_channel();
        Client {
            links: self.links.clone(),
            virtual_shard_count: self.config.virtual_shard_count,
            location_hint,
            routes: HashMap::new(),
            reply_tx,
            reply_rx,
            pool: SlabPool::new(
                self.config.slab_bytes,
                self.accounts[home_core].clone(),
                self.config.slab_hook.clone(),
                // Above the cores' ids, so one hook can tell every pool's slabs apart.
                (self.config.core_count as u64 + 1 + client_number) << 40,
            ),
            memory_cap_bytes: self.config.memory_cap_bytes,
        }
    }

    pub async fn placement(&self) -> Vec<u16> {
        let (reply, answer) = oneshot::channel();
        let _ = self.to_control.send(NodeMsg::Placement { reply });
        answer.await.unwrap_or_default()
    }

    /// Answered from the core's event loop, so this shows the loop is running.
    pub async fn ping(&self, core: usize) -> Option<usize> {
        let (reply, answer) = oneshot::channel();
        self.links.control[core]
            .send(ControlMsg::Ping { reply })
            .ok()?;
        answer.await.ok()
    }

    pub async fn stats(&self, core: usize) -> Option<CoreStats> {
        let (reply, answer) = oneshot::channel();
        self.links.control[core]
            .send(ControlMsg::Stats { reply })
            .ok()?;
        answer.await.ok()
    }

    /// Stamped batches `core` holds for a stream, in id order: what M5 reads from.
    pub async fn read_memory(&self, core: usize, stream: &str) -> Vec<SlabBytes> {
        let (reply, answer) = oneshot::channel();
        let _ = self.links.control[core].send(ControlMsg::ReadMemory {
            stream: Arc::from(stream),
            reply,
        });
        answer.await.unwrap_or_default()
    }

    /// Seals now, and waits for the upload.
    pub async fn seal(&self) -> SealOutcome {
        let (reply, answer) = oneshot::channel();
        let _ = self.to_control.send(NodeMsg::Seal { reply });
        answer.await.unwrap_or(SealOutcome::NodeStopped)
    }

    /// Runs the periodic check now, and returns its moves once they have finished.
    pub async fn rebalance_now(&self) -> Vec<ShardMove> {
        let (reply, answer) = oneshot::channel();
        let _ = self.to_control.send(NodeMsg::Rebalance { reply });
        answer.await.unwrap_or_default()
    }

    pub async fn move_shard(&self, shard: u32, to: usize) {
        let (reply, answer) = oneshot::channel();
        let _ = self
            .to_control
            .send(NodeMsg::MoveShard { shard, to, reply });
        let _ = answer.await;
    }

    /// Gives a stream its own shard, on core `to`. `None` if the table is full.
    pub async fn set_override(&self, stream: &str, to: usize) -> Option<u32> {
        let (reply, answer) = oneshot::channel();
        let _ = self.to_control.send(NodeMsg::SetOverride {
            stream: Arc::from(stream),
            to,
            reply,
        });
        answer.await.ok().flatten()
    }

    fn request_stop(&self) {
        let _ = self.to_control.send(NodeMsg::Stop);
        for control in &self.links.control {
            let _ = control.send(ControlMsg::Stop);
        }
    }

    /// Stops everything and waits. A WAL held in a `sync_data` has to be let go first.
    pub fn shutdown(mut self) -> NodeExit {
        self.request_stop();
        for thread in self.threads.drain(..) {
            if let Err(panic) = thread.join() {
                std::panic::resume_unwind(panic);
            }
        }
        let unfinished_upload_count = self
            .uploader
            .take()
            .map_or(0, |uploader| uploader.shutdown().unfinished.len());
        let wal = self
            .wal
            .take()
            .expect("shutdown takes the node by value")
            .join();
        NodeExit {
            wal,
            unfinished_upload_count,
        }
    }
}

impl Drop for Node {
    /// A dropped node's threads end by themselves. The uploader's own drop stops it.
    fn drop(&mut self) {
        self.request_stop();
    }
}

/// One publish, as a `PUBLISH` frame carries it.
#[derive(Debug, Clone)]
pub struct Publish<'a> {
    pub stream: &'a str,
    pub handle: u32,
    pub producer_id: u64,
    pub producer_seq: u64,
    /// `None` takes the stream's default.
    pub ack_level: Option<u8>,
    /// From the token, and never 0.
    pub author: NonZeroU64,
    pub state: Option<StateChange>,
    pub records: &'a [u8],
}

#[derive(Debug, Clone)]
pub struct StateOpRequest<'a> {
    pub stream: &'a str,
    pub req: u32,
    pub author: NonZeroU64,
    pub expect_ver: u64,
    pub op: StateOp,
    pub doc: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientError {
    /// The core's inbox is full, or the memory cap is reached. Only from `try_publish`.
    Full,
    NodeStopped,
}

struct ClientRoute {
    stream: Arc<str>,
    core: usize,
    barrier_pending: bool,
}

/// One connection's view of the node: a slab pool to read frames into, and where each
/// stream's messages go. Used from one task. Replies come back on `recv`.
///
/// Order across this client's messages for a stream holds through moves. The route only
/// changes after a barrier sent down the old route comes back, so everything sent before
/// has been handled by then, and nothing can overtake it (D65).
pub struct Client {
    links: Links,
    virtual_shard_count: u32,
    location_hint: Vec<u16>,
    routes: HashMap<Arc<str>, ClientRoute>,
    reply_tx: mpsc::UnboundedSender<Reply>,
    reply_rx: mpsc::UnboundedReceiver<Reply>,
    pool: SlabPool,
    memory_cap_bytes: u64,
}

impl Client {
    fn route(&mut self, stream: &str) -> (Arc<str>, usize) {
        if let Some(route) = self.routes.get(stream) {
            return (Arc::clone(&route.stream), route.core);
        }
        let shard = shard_of(stream, self.virtual_shard_count);
        let core = self.location_hint[shard as usize] as usize;
        let name: Arc<str> = Arc::from(stream);
        self.routes.insert(
            Arc::clone(&name),
            ClientRoute {
                stream: Arc::clone(&name),
                core,
                barrier_pending: false,
            },
        );
        (name, core)
    }

    fn publish_msg(&mut self, publish: Publish<'_>) -> (usize, WriteMsg) {
        let (stream, core) = self.route(publish.stream);
        let records = self.pool.copy_in(publish.records);
        (
            core,
            WriteMsg::Publish(Box::new(PublishMsg {
                stream,
                handle: publish.handle,
                producer_id: publish.producer_id,
                producer_seq: publish.producer_seq,
                ack_level: publish.ack_level,
                author: publish.author,
                state: publish.state,
                records,
                reply: self.reply_tx.clone(),
            })),
        )
    }

    /// Reads the frame into a slab and hands it to the owner. Waits while the memory cap
    /// is reached or the core's inbox is full, which is what stops reading a socket (D7).
    pub async fn publish(&mut self, publish: Publish<'_>) -> Result<(), ClientError> {
        while self.pool.account().pinned_bytes() >= self.memory_cap_bytes {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let (core, message) = self.publish_msg(publish);
        self.links.direct[core]
            .send(message)
            .await
            .map_err(|_| ClientError::NodeStopped)
    }

    /// As `publish`, returning at once instead of waiting.
    pub fn try_publish(&mut self, publish: Publish<'_>) -> Result<(), ClientError> {
        if self.pool.account().pinned_bytes() >= self.memory_cap_bytes {
            return Err(ClientError::Full);
        }
        let (core, message) = self.publish_msg(publish);
        self.links.direct[core]
            .try_send(message)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => ClientError::Full,
                mpsc::error::TrySendError::Closed(_) => ClientError::NodeStopped,
            })
    }

    pub async fn state_op(&mut self, request: StateOpRequest<'_>) -> Result<(), ClientError> {
        let (stream, core) = self.route(request.stream);
        let message = WriteMsg::StateOp(Box::new(StateOpMsg {
            stream,
            req: request.req,
            author: request.author,
            change: StateChange {
                expect_ver: request.expect_ver,
                op: request.op,
                doc: request.doc,
            },
            reply: self.reply_tx.clone(),
        }));
        self.links.direct[core]
            .send(message)
            .await
            .map_err(|_| ClientError::NodeStopped)
    }

    /// State and head as one step. Error 2 if the stream doesn't exist.
    pub async fn snapshot(&mut self, stream: &str) -> Result<SnapshotReply, ErrorCode> {
        let (stream, core) = self.route(stream);
        let (reply, answer) = oneshot::channel();
        let _ = self.links.control[core].send(ControlMsg::Snapshot { stream, reply });
        answer.await.unwrap_or(Err(ErrorCode::NotFound))
    }

    /// Creates the stream if needed, and sets its config.
    pub async fn configure(&mut self, stream: &str, config: StreamConfig) {
        let (stream, core) = self.route(stream);
        let (reply, answer) = oneshot::channel();
        let _ = self.links.control[core].send(ControlMsg::Configure {
            stream,
            config,
            reply,
        });
        let _ = answer.await;
    }

    /// The core this client currently sends a stream's messages to.
    pub fn route_of(&mut self, stream: &str) -> usize {
        self.route(stream).1
    }

    fn on_internal(&mut self, reply: Reply) -> Option<Reply> {
        match reply {
            Reply::Moved { stream } => {
                if let Some(route) = self.routes.get_mut(&*stream)
                    && !route.barrier_pending
                {
                    // If the inbox is full, the next forwarded message tries again.
                    let barrier = WriteMsg::Barrier {
                        stream,
                        reply: self.reply_tx.clone(),
                    };
                    route.barrier_pending = self.links.direct[route.core].try_send(barrier).is_ok();
                }
                None
            }
            Reply::RouteDone { stream, core } => {
                if let Some(route) = self.routes.get_mut(&*stream) {
                    route.core = core;
                    route.barrier_pending = false;
                }
                None
            }
            other => Some(other),
        }
    }

    /// The next reply. `None` if the node has gone and no more can come.
    pub async fn recv(&mut self) -> Option<Reply> {
        loop {
            let reply = self.reply_rx.recv().await?;
            if let Some(reply) = self.on_internal(reply) {
                return Some(reply);
            }
        }
    }

    /// The next reply within `wait_ms`.
    pub async fn recv_within(&mut self, wait_ms: u64) -> Option<Reply> {
        tokio::time::timeout(Duration::from_millis(wait_ms), self.recv())
            .await
            .ok()
            .flatten()
    }

    /// Lets go of the slab being filled, so it can be freed once its references go.
    pub fn retire_slab(&mut self) {
        self.pool.retire_current();
    }

    /// Slab bytes alive for this client's home core.
    pub fn pinned_bytes(&self) -> u64 {
        self.pool.account().pinned_bytes()
    }
}
