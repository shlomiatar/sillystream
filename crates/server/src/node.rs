//! A node: its cores, the WAL writer, the uploader, and a control thread that runs what
//! is node-wide: the seal window (section 7.1), rebalancing (section 6.1), and the
//! in-process API that later milestones put sockets in front of.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;
use proto::ErrorCode;
use storage::{
    CheckpointSet, DirVolume, DurableWatch, MetadataStore, SegmentCache, StreamExtent, UploadEvent,
    UploadKey, Uploader, UploaderConfig, WalConfig, WalError, WalHandle, WalSendError, WalSender,
    WalStats, WalVolume, durable_watches, start_wal_writer,
};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::clock::Clock;
use crate::compaction::{
    CompactCmd, CompactReport, CompactionConfig, Compactor, ExpireReport, MANIFEST_STREAM,
    RetentionClass,
};
use crate::connection::{Connection, DeliverySettings, Stores};
use crate::core::{Core, CoreConfig, CoreInbox, CoreParts, CoreToNode, Links};
use crate::message::{
    ControlMsg, CoreStats, CursorOp, PublishMsg, Reply, RunOnCore, SinkOp, SnapshotReply,
    StateChange, StateOpMsg, StreamConfig, StreamConfigPatch, StreamInfo, WriteMsg,
};
use crate::metadata::{
    CheckpointDone, CheckpointJob, Checkpointer, LocalMeta, run_checkpointer, run_loader,
};
use crate::placement::{ShardMove, plan_moves, shard_of};
use crate::recovery::{RecoverConfig, RecoveryReport, RestoredStream, ScannedObject, prepare};
use crate::slab::{SlabAccount, SlabBytes, SlabHook, SlabPool, SlabRegion};
use crate::state::StateOp;

/// The local tier of section 6.2: copies of uploaded segments.
#[derive(Debug, Clone)]
pub struct LocalCacheConfig {
    pub dir: PathBuf,
    pub cap_bytes: u64,
}

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
    /// `None` reads sealed records from the object store alone.
    pub local_cache: Option<LocalCacheConfig>,
    /// A subscription's queue bound (section 5.2).
    pub subscriber_queue_bytes: u64,
    /// `PROGRESS` goes at most this often (section 5.2).
    pub progress_interval_ms: u64,
    pub max_frame_bytes: u64,
    pub max_subscriptions_per_connection: usize,
    /// How long a WebSocket message may take to arrive once it has begun. Past it the
    /// connection is closed, because what it sent so far is held in memory.
    pub partial_frame_timeout_ms: u64,
    /// What a subscription sends per turn on its connection (section 5.2).
    pub fair_share_bytes: u64,
    /// Bytes a connection can have that its client hasn't taken. M6 has a socket for this.
    pub connection_window_bytes: u64,
    /// How much one catch-up read asks for.
    pub catch_up_read_bytes: u64,
    /// With a metadata store, how many streams may stay in RAM with nobody on them, split over
    /// the cores (section 7.6). `None` never evicts.
    pub active_stream_limit: Option<usize>,
    /// With a metadata store, every this many checkpoint files one holds everything, and older
    /// ones can go (D20).
    pub full_checkpoint_every_count: usize,
    /// With a metadata store: seal manifests, the compactor and the delete job (sections 7.4,
    /// 7.5). `None` keeps every record for good.
    pub compaction: Option<CompactionConfig>,
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
            local_cache: None,
            subscriber_queue_bytes: 8 * 1024 * 1024,
            progress_interval_ms: 5_000,
            max_frame_bytes: 16 * 1024 * 1024,
            max_subscriptions_per_connection: 1_024,
            partial_frame_timeout_ms: 60_000,
            fair_share_bytes: 256 * 1024,
            connection_window_bytes: 4 * 1024 * 1024,
            catch_up_read_bytes: 1024 * 1024,
            active_stream_limit: None,
            full_checkpoint_every_count: 64,
            compaction: None,
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

pub(crate) enum NodeMsg {
    FromCore(CoreToNode),
    Seal {
        reply: oneshot::Sender<SealOutcome>,
    },
    /// Asks for a seal, and answers true when a checkpoint has what it held. False if the
    /// node has none or can no longer seal.
    SealAndCheckpoint {
        reply: oneshot::Sender<bool>,
    },
    /// Answers when the next seal's checkpoint is saved, without asking for a seal.
    NextCheckpoint {
        reply: oneshot::Sender<bool>,
    },
    /// An object's manifest is in the WAL, so a checkpoint may record it (section 7.5).
    ManifestDurable {
        path: String,
    },
    CheckpointSaved(CheckpointDone),
    RetryRotate,
    Rotated {
        seal: u64,
        closed_seq: Option<u64>,
    },
    Rebalance {
        reply: oneshot::Sender<Vec<ShardMove>>,
    },
    MoveShard {
        shard: u32,
        to: usize,
        reply: oneshot::Sender<bool>,
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
    /// `None` on a node without a metadata store.
    meta: Option<ControlMeta>,
    /// Whether objects get a manifest.
    manifests: bool,
    /// Objects a startup found that need one.
    startup_manifests: Vec<(String, u64)>,
}

/// Who waits for a checkpoint, and what each is told if none can come.
type CheckpointWaiter = (oneshot::Sender<bool>, bool);

/// What control tracks to save metadata at each seal (D18).
struct ControlMeta {
    jobs: mpsc::UnboundedSender<CheckpointJob>,
    /// The WAL file each seal closed.
    closed_seq_by_seal: HashMap<u64, u64>,
    /// Objects no checkpoint has listed yet, which wait for their extents to reach a gather.
    objects: Vec<String>,
    gather_count: u64,
    next_waiters: Vec<CheckpointWaiter>,
    waiters_by_seal: BTreeMap<u64, Vec<CheckpointWaiter>>,
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
        for (path, len_bytes) in std::mem::take(&mut self.startup_manifests) {
            self.publish_manifest(path, len_bytes);
        }
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
                        self.on_upload_event(event).await;
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
        self.fail_checkpoint_waiters();
    }

    async fn on_message(&mut self, message: NodeMsg) {
        match message {
            NodeMsg::FromCore(CoreToNode::SealRequested) => {
                self.seal_wanted = true;
                self.begin_seal();
            }
            NodeMsg::FromCore(CoreToNode::DurableRequested(reply)) => {
                // Already deleted here: a crash before the checkpoint is what's being waited
                // out, so with no checkpoint to come it still answers that it was deleted.
                self.wait_for_checkpoint((reply, true));
            }
            NodeMsg::SealAndCheckpoint { reply } => self.wait_for_checkpoint((reply, false)),
            NodeMsg::NextCheckpoint { reply } => match &mut self.meta {
                Some(meta) => meta.next_waiters.push((reply, false)),
                None => {
                    let _ = reply.send(false);
                }
            },
            NodeMsg::ManifestDurable { path } => {
                if let Some(meta) = &mut self.meta {
                    meta.objects.push(path);
                }
            }
            NodeMsg::CheckpointSaved(done) => self.on_checkpoint_saved(done),
            NodeMsg::Seal { reply } => {
                self.seal_waiters.push(reply);
                self.seal_wanted = true;
                self.begin_seal();
            }
            NodeMsg::RetryRotate => {
                self.rotating = false;
                self.begin_seal();
            }
            NodeMsg::Rotated { seal, closed_seq } => {
                self.rotating = false;
                if let Some(closed_seq) = closed_seq {
                    storage::crash_point("seal.after_rotate");
                    if let Some(meta) = &mut self.meta {
                        meta.closed_seq_by_seal.insert(seal, closed_seq);
                    }
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
                let _ = reply.send(self.checked_move(shard, to).await);
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
                if let Some(meta) = &mut self.meta {
                    let waiters = std::mem::take(&mut meta.next_waiters);
                    meta.waiters_by_seal.insert(seal, waiters);
                }
                let tx = self.tx.clone();
                tokio::spawn(async move {
                    let closed_seq = done.await.ok().map(|rotated| rotated.closed_seq);
                    let _ = tx.send(NodeMsg::Rotated { seal, closed_seq });
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
                self.fail_checkpoint_waiters();
            }
        }
    }

    fn fail_seal_waiters(&mut self, seal: u64) {
        for waiter in self.waiters_by_seal.remove(&seal).unwrap_or_default() {
            let _ = waiter.send(SealOutcome::WalStopped);
        }
        self.fail_checkpoint_waiters();
    }

    fn wait_for_checkpoint(&mut self, waiter: CheckpointWaiter) {
        let Some(meta) = &mut self.meta else {
            let (reply, when_stopped) = waiter;
            let _ = reply.send(when_stopped);
            return;
        };
        meta.next_waiters.push(waiter);
        self.seal_wanted = true;
        self.begin_seal();
    }

    fn fail_checkpoint_waiters(&mut self) {
        let Some(meta) = &mut self.meta else {
            return;
        };
        let by_seal = std::mem::take(&mut meta.waiters_by_seal);
        let next = std::mem::take(&mut meta.next_waiters);
        for (reply, when_stopped) in by_seal.into_values().flatten().chain(next) {
            let _ = reply.send(when_stopped);
        }
    }

    /// A seal's upload ended, one way or another. This is the seal's metadata step: it asks
    /// every core what changed and sends that to be saved. Control waits for the cores, which
    /// never wait for it, so moves can't interleave with a gather (D137).
    async fn checkpoint_seal(&mut self, seal: u64) {
        storage::crash_point("seal.after_upload");
        let Some(meta) = &mut self.meta else {
            return;
        };
        let closed_seq = meta.closed_seq_by_seal.remove(&seal);
        meta.gather_count += 1;
        let gather = meta.gather_count;
        let mut clean = true;
        for control in &self.links.control {
            let (reply, answer) = oneshot::channel();
            if control
                .send(ControlMsg::CheckpointClean { seal, reply })
                .is_err()
            {
                return;
            }
            let Ok(core_clean) = answer.await else {
                return;
            };
            clean &= core_clean;
        }
        let mut metas = Vec::new();
        for control in &self.links.control {
            let (reply, answer) = oneshot::channel();
            if control
                .send(ControlMsg::CheckpointGather { gather, reply })
                .is_err()
            {
                return;
            }
            let Ok(gathered) = answer.await else {
                return;
            };
            metas.extend(gathered);
        }
        let objects = if clean {
            std::mem::take(&mut meta.objects)
        } else {
            Vec::new()
        };
        let _ = meta.jobs.send(CheckpointJob {
            seal,
            gather,
            metas,
            objects,
            recycle_seq: if clean { closed_seq } else { None },
        });
    }

    fn on_checkpoint_saved(&mut self, done: CheckpointDone) {
        storage::crash_point("checkpoint.after_put");
        if let Some(seq) = done.recycle_seq {
            // A full channel only leaves the file until the next seal's recycle covers it.
            let _ = self.wal.try_recycle(seq);
        }
        for control in &self.links.control {
            let _ = control.send(ControlMsg::CheckpointDone {
                gather: done.gather,
            });
        }
        if let Some(meta) = &mut self.meta {
            let later = meta.waiters_by_seal.split_off(&(done.seal + 1));
            let finished = std::mem::replace(&mut meta.waiters_by_seal, later);
            for (reply, _) in finished.into_values().flatten() {
                let _ = reply.send(true);
            }
        }
    }

    async fn on_upload_event(&mut self, event: UploadEvent) {
        if let UploadEvent::Uploaded {
            path,
            len_bytes,
            extents,
            ..
        } = &event
        {
            // The rule section 7.5 asks for: the manifest stream's own objects get none, or
            // each manifest would make an object that makes a manifest (Q4).
            let others = extents
                .iter()
                .any(|extent| &*extent.stream != MANIFEST_STREAM);
            if self.manifests && others {
                self.publish_manifest(path.to_string(), *len_bytes);
            } else if let Some(meta) = &mut self.meta {
                meta.objects.push(path.to_string());
            }
        }
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
        self.checkpoint_seal(seal).await;
        for waiter in self.waiters_by_seal.remove(&seal).unwrap_or_default() {
            let _ = waiter.send(outcome.clone());
        }
    }

    /// The manifest of an uploaded object. Only once it is in the WAL does a checkpoint record
    /// the object: a startup looks at any object no checkpoint records and publishes again, so
    /// a crash can't leave an object the compactor will never hear of (section 7.5).
    fn publish_manifest(&mut self, path: String, len_bytes: u64) {
        storage::crash_point("manifest.before_publish");
        let (reply, mut answer) = mpsc::unbounded_channel();
        let shard = shard_of(MANIFEST_STREAM, self.config.virtual_shard_count);
        let core = self.location[shard as usize] as usize;
        let doc = format::encode_manifest_doc(len_bytes, &path);
        let _ = self.links.control[core].send(ControlMsg::PublishManifest { doc, reply });
        let tx = self.tx.clone();
        tokio::spawn(async move {
            if let Some(Reply::Ack { .. }) = answer.recv().await {
                let _ = tx.send(NodeMsg::ManifestDurable { path });
            }
        });
    }

    /// What the API can ask for, unlike the rebalancer's plan, may not exist: a core that
    /// isn't there, a shard past the table, or an override shard with no stream yet (D80).
    async fn checked_move(&mut self, shard: u32, to: usize) -> bool {
        let known = self
            .location
            .get(shard as usize)
            .is_some_and(|at| *at != u16::MAX);
        if !known || to >= self.config.core_count {
            return false;
        }
        self.move_shard(shard, to).await;
        true
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
        if to >= self.config.core_count {
            return None;
        }
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

pub(crate) async fn sleep_until_some(deadline: Option<Instant>) {
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

/// What a socket task needs to reach the node. A connection holds a cheap copy of it, so
/// that idle connections cost little (section 6.5).
#[derive(Clone)]
pub(crate) struct NodeHandle {
    links: Arc<Links>,
    virtual_shard_count: u32,
    location_hint: Arc<Vec<u16>>,
    accounts: Vec<SlabAccount>,
    slab_bytes: usize,
    slab_hook: Option<Arc<dyn SlabHook>>,
    memory_cap_bytes: u64,
    window_bytes: u64,
    core_count: usize,
    client_count: Arc<AtomicU64>,
}

impl NodeHandle {
    pub(crate) fn core_count(&self) -> usize {
        self.core_count
    }

    pub(crate) fn account(&self, core: usize) -> &SlabAccount {
        &self.accounts[core]
    }

    /// A client whose frames are already in slabs. It never fills a slab of its own.
    pub(crate) fn client(&self, home_core: usize) -> Client {
        let client_number = self.client_count.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = mpsc::unbounded_channel();
        Client {
            links: Arc::clone(&self.links),
            virtual_shard_count: self.virtual_shard_count,
            location_hint: Arc::clone(&self.location_hint),
            routes: HashMap::new(),
            reply_tx,
            reply_rx,
            received: VecDeque::new(),
            pool: SlabPool::new(
                self.slab_bytes,
                self.accounts[home_core].clone(),
                self.slab_hook.clone(),
                (self.core_count as u64 + 1 + client_number) << 40,
            ),
            memory_cap_bytes: self.memory_cap_bytes,
        }
    }

    pub(crate) fn read_pool(&self, home_core: usize) -> SlabPool {
        SlabPool::new(
            self.slab_bytes,
            self.accounts[home_core].clone(),
            self.slab_hook.clone(),
            (self.core_count as u64 + 1 + self.client_count.fetch_add(1, Ordering::Relaxed)) << 40,
        )
    }

    pub(crate) async fn connection(&self, home_core: usize) -> Option<Connection> {
        Connection::open(self.links.control[home_core].clone(), self.window_bytes).await
    }

    /// Runs `work` on a core's thread, inside its runtime. False if the core has stopped.
    pub(crate) fn run_on_core(&self, core: usize, work: Box<dyn FnOnce() + Send>) -> bool {
        self.links.control[core]
            .send(ControlMsg::Run(RunOnCore(work)))
            .is_ok()
    }
}

pub struct Node {
    config: NodeConfig,
    links: Links,
    to_control: mpsc::UnboundedSender<NodeMsg>,
    accounts: Vec<SlabAccount>,
    threads: Vec<JoinHandle<()>>,
    client_count: Arc<AtomicU64>,
    uploader: Option<Uploader<SlabBytes>>,
    wal: Option<WalHandle>,
    /// The metadata threads, which go last since cores and control send to them.
    meta_threads: Vec<JoinHandle<()>>,
    meta_stop: Arc<AtomicBool>,
    /// `None` without a metadata store or with compaction off.
    compact_tx: Option<mpsc::UnboundedSender<CompactCmd>>,
    compact_stop: Option<oneshot::Sender<()>>,
}

/// What a recovering start adds to a plain one.
struct RecoveredParts {
    local: Arc<LocalMeta>,
    checkpoints: CheckpointSet,
    max_id: u64,
    first_seq: u64,
    closed_seqs: Vec<u64>,
    restored: Vec<RestoredStream>,
    scanned_objects: Vec<ScannedObject>,
}

fn runtime() -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}

/// A delivery bound past the memory cap stops nothing, because publishers stop first (D96).
fn delivery_bound_bytes(config: &NodeConfig, bound_bytes: u64) -> u64 {
    bound_bytes.min(config.memory_cap_bytes / 4)
}

impl Node {
    /// Starts the WAL writer on `volume`, the uploader on `store`, and every core. Nothing is
    /// replayed, and no metadata is kept: `recover` is what a real node starts with.
    pub fn start<V: WalVolume + 'static>(
        config: NodeConfig,
        volume: V,
        store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
    ) -> io::Result<Node> {
        Node::start_parts(config, volume, store, clock, None)
    }

    /// The startup of section 8.1, for a new node and a crashed one alike. Fails if another
    /// process takes ownership first or the local store is held, having changed nothing but
    /// what S3 already backs: a checkpoint it owed, and a store another generation left stale.
    pub fn recover(
        mut config: NodeConfig,
        recover: RecoverConfig,
        store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
    ) -> io::Result<(Node, RecoveryReport)> {
        std::fs::create_dir_all(&recover.wal_dir)?;
        std::fs::create_dir_all(&recover.metadata_dir)?;
        let local_store = MetadataStore::open(&recover.metadata_dir).map_err(io::Error::other)?;
        let now_ms = clock.now_ms();
        let prepared = {
            // On its own thread, since the caller may already be inside a runtime.
            let (wal_dir, store) = (recover.wal_dir.clone(), Arc::clone(&store));
            std::thread::spawn(move || {
                runtime()?
                    .block_on(prepare(&wal_dir, local_store, &store, now_ms))
                    .map_err(io::Error::other)
            })
            .join()
            .map_err(|_| io::Error::other("startup replay panicked"))??
        };
        let (prepared, local) = prepared;
        config.generation = prepared.report.generation;
        config.last_valid_id = prepared.report.last_valid_id;
        let report = prepared.report;
        let node = Node::start_parts(
            config,
            DirVolume::for_generation(
                &recover.wal_dir,
                recover.wal_preallocate_bytes,
                report.generation,
            ),
            store,
            clock,
            Some(RecoveredParts {
                local,
                checkpoints: prepared.checkpoints,
                max_id: prepared.max_id,
                first_seq: prepared.first_seq,
                closed_seqs: prepared.closed_seqs,
                restored: prepared.restored,
                scanned_objects: prepared.scanned_objects,
            }),
        )?;
        Ok((node, report))
    }

    fn start_parts<V: WalVolume + 'static>(
        config: NodeConfig,
        volume: V,
        store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
        recovered: Option<RecoveredParts>,
    ) -> io::Result<Node> {
        let core_count = config.core_count;
        let meta_stop = Arc::new(AtomicBool::new(false));
        let (first_seq, closed_seqs) = recovered.as_ref().map_or((1, Vec::new()), |parts| {
            (parts.first_seq, parts.closed_seqs.clone())
        });
        let cache = match &config.local_cache {
            Some(local) => {
                Some(SegmentCache::open(&local.dir, local.cap_bytes).map_err(io::Error::other)?)
            }
            None => None,
        };
        let stores = Stores {
            local: cache.as_ref().map(|cache| {
                let store: Arc<dyn ObjectStore> = cache.store();
                store
            }),
            object: Arc::clone(&store),
        };
        let (notify, durable): (DurableWatch, _) = durable_watches(core_count);
        let (wal, wal_handle) = start_wal_writer::<SlabBytes, _, _>(
            volume,
            notify,
            WalConfig {
                first_seq,
                closed_seqs,
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
            store: Arc::clone(&store),
            node: config.node_name.clone(),
            generation: config.generation,
            section_count: core_count,
            channel_capacity: config.upload_channel_capacity,
            retry_initial_ms: config.upload_retry_initial_ms,
            retry_max_ms: config.upload_retry_max_ms,
            cache,
            events: upload_txs,
        })?;

        let mut links = Links {
            control: Vec::new(),
            forwarded: Vec::new(),
            direct: Vec::new(),
            delivery: Vec::new(),
            reads: Vec::new(),
        };
        let mut inboxes = Vec::new();
        for (durable, uploads) in durable.into_iter().zip(upload_rxs) {
            let (control_tx, control) = mpsc::unbounded_channel();
            let (forwarded_tx, forwarded) = mpsc::unbounded_channel();
            let (direct_tx, direct) = mpsc::channel(config.write_inbox_capacity);
            links.control.push(control_tx);
            links.forwarded.push(forwarded_tx);
            links.direct.push(direct_tx);
            let (delivery_tx, delivery) = mpsc::unbounded_channel();
            links.delivery.push(delivery_tx);
            let (reads_tx, reads) = mpsc::unbounded_channel();
            links.reads.push(reads_tx);
            inboxes.push(CoreInbox {
                control,
                forwarded,
                direct,
                delivery,
                reads,
                durable,
                uploads,
            });
        }

        let (core_to_node, core_rx) = mpsc::unbounded_channel();
        let accounts: Vec<SlabAccount> = (0..core_count).map(|_| SlabAccount::new()).collect();
        let settings = DeliverySettings {
            queue_bytes: delivery_bound_bytes(&config, config.subscriber_queue_bytes),
            progress_interval_ms: config.progress_interval_ms,
            max_frame_bytes: config.max_frame_bytes,
            max_subscriptions: config.max_subscriptions_per_connection,
            share_bytes: config.fair_share_bytes,
            window_bytes: delivery_bound_bytes(&config, config.connection_window_bytes),
            read_bytes: config.catch_up_read_bytes,
            memory_cap_bytes: config.memory_cap_bytes,
        };
        let mut threads = Vec::with_capacity(core_count + 1);
        let (to_control, control_rx) = mpsc::unbounded_channel();
        let loader = recovered.as_ref().map(|_| mpsc::unbounded_channel());
        let core_limit = config
            .active_stream_limit
            .map(|limit| (limit / core_count).max(1));
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
                    active_stream_limit: core_limit,
                    write_park_cap_count: config.write_inbox_capacity,
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
                settings,
                stores: stores.clone(),
                accounts: accounts.clone(),
                loader: loader.as_ref().map(|(tx, _)| tx.clone()),
            });
            let runtime = runtime()?;
            threads.push(
                std::thread::Builder::new()
                    .name(format!("core-{id}"))
                    .spawn(move || {
                        runtime.block_on(core.run());
                        // A read in progress on a hung store mustn't hold up the stop.
                        runtime.shutdown_background();
                    })?,
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
        let mut meta_threads = Vec::new();
        let mut meta = None;
        let mut startup_manifests = Vec::new();
        let mut manifests = false;
        let mut compactor = None;
        if let (Some(parts), Some((loader_tx, loader_rx))) = (recovered, loader) {
            // Before anything can reach the cores: these come first in each one's control
            // channel, and control is what a core reads first (D134).
            for stream in parts.restored {
                let shard = shard_of(&stream.meta.name, config.virtual_shard_count);
                let core = shard as usize % core_count;
                let _ = links.control[core].send(ControlMsg::Restore(Box::new(stream)));
            }
            manifests = config.compaction.is_some();
            let mut known_objects = Vec::new();
            for object in parts.scanned_objects {
                if manifests && object.needs_manifest {
                    startup_manifests.push((object.path, object.len_bytes));
                } else {
                    known_objects.push(object.path);
                }
            }
            if let Some(compaction) = &config.compaction {
                compactor = Some(Compactor {
                    config: compaction.clone(),
                    node_name: config.node_name.clone(),
                    generation: config.generation,
                    ownership: parts.checkpoints.ownership.clone(),
                    store: Arc::clone(&store),
                    clock: Arc::clone(&clock),
                    links: links.clone(),
                    virtual_shard_count: config.virtual_shard_count,
                    core_count,
                    window_bytes: delivery_bound_bytes(&config, config.connection_window_bytes),
                    has_seal_window: config.seal_window_ms.is_some(),
                    to_control: to_control.clone(),
                    noted: std::sync::Mutex::new(
                        parts
                            .checkpoints
                            .recorded_objects()
                            .map(str::to_owned)
                            .collect(),
                    ),
                });
            }
            let (jobs, job_rx) = mpsc::unbounded_channel();
            let checkpointer = Checkpointer {
                generation: config.generation,
                local: Arc::clone(&parts.local),
                store: Arc::clone(&store),
                checkpoints: parts.checkpoints.clone(),
                full_every_count: config.full_checkpoint_every_count,
                max_id: parts.max_id,
                loader: loader_tx,
                stop: Arc::clone(&meta_stop),
            };
            let saved = to_control.clone();
            meta_threads.push(
                std::thread::Builder::new()
                    .name("checkpointer".to_owned())
                    .spawn(move || {
                        run_checkpointer(checkpointer, job_rx, |done| {
                            let _ = saved.send(NodeMsg::CheckpointSaved(done));
                        });
                    })?,
            );
            let (local, store, control, stop) = (
                parts.local,
                Arc::clone(&store),
                links.control.clone(),
                Arc::clone(&meta_stop),
            );
            let checkpoints = parts.checkpoints;
            meta_threads.push(
                std::thread::Builder::new()
                    .name("meta-loader".to_owned())
                    .spawn(move || {
                        run_loader(loader_rx, local, store, checkpoints, control, stop);
                    })?,
            );
            meta = Some(ControlMeta {
                jobs,
                closed_seq_by_seal: HashMap::new(),
                objects: known_objects,
                gather_count: 0,
                next_waiters: Vec::new(),
                waiters_by_seal: BTreeMap::new(),
            });
        }
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
            meta,
            manifests,
            startup_manifests,
        };
        let runtime = runtime()?;
        threads.push(
            std::thread::Builder::new()
                .name("node-control".to_owned())
                .spawn(move || {
                    runtime.block_on(control.run(control_rx, core_rx, control_uploads))
                })?,
        );

        let (mut compact_tx, mut compact_stop) = (None, None);
        if let Some(compactor) = compactor {
            let (tx, rx) = mpsc::unbounded_channel();
            let (stop, stopped) = oneshot::channel();
            compact_tx = Some(tx);
            compact_stop = Some(stop);
            meta_threads.push(
                std::thread::Builder::new()
                    .name("compactor".to_owned())
                    .spawn(move || compactor.run(rx, stopped))?,
            );
        }

        Ok(Node {
            config,
            links,
            to_control,
            accounts,
            threads,
            client_count: Arc::new(AtomicU64::new(0)),
            uploader: Some(uploader),
            wal: Some(wal_handle),
            meta_threads,
            meta_stop,
            compact_tx,
            compact_stop,
        })
    }

    pub fn core_count(&self) -> usize {
        self.config.core_count
    }

    pub fn virtual_shard_count(&self) -> u32 {
        self.config.virtual_shard_count
    }

    pub(crate) fn handle(&self) -> NodeHandle {
        let shards = self.config.virtual_shard_count + self.config.override_shard_count;
        NodeHandle {
            links: Arc::new(self.links.clone()),
            virtual_shard_count: self.config.virtual_shard_count,
            // The start's placement. A hint that has aged just gets forwarded (D65).
            location_hint: Arc::new(
                (0..shards)
                    .map(|shard| (shard as usize % self.config.core_count) as u16)
                    .collect(),
            ),
            accounts: self.accounts.clone(),
            slab_bytes: self.config.slab_bytes,
            slab_hook: self.config.slab_hook.clone(),
            memory_cap_bytes: self.config.memory_cap_bytes,
            window_bytes: delivery_bound_bytes(&self.config, self.config.connection_window_bytes),
            core_count: self.config.core_count,
            client_count: Arc::clone(&self.client_count),
        }
    }

    /// A client living on `home_core`, as a connection would (D24).
    pub async fn client(&self, home_core: usize) -> Client {
        let location_hint = Arc::new(self.placement().await);
        let client_number = self.client_count.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = mpsc::unbounded_channel();
        Client {
            links: Arc::new(self.links.clone()),
            virtual_shard_count: self.config.virtual_shard_count,
            location_hint,
            routes: HashMap::new(),
            reply_tx,
            reply_rx,
            received: VecDeque::new(),
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

    /// A connection living on `home_core`, as a socket accepted there would (D24). `None`
    /// if the node has stopped.
    pub async fn connection(&self, home_core: usize) -> Option<Connection> {
        Connection::open(
            self.links.control[home_core].clone(),
            delivery_bound_bytes(&self.config, self.config.connection_window_bytes),
        )
        .await
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

    /// Stamped batches `core` holds for a stream, in id order.
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

    /// Seals now, and waits until the checkpoint with what it held is saved. False if this
    /// node keeps no metadata or can't seal any more.
    pub async fn seal_and_checkpoint(&self) -> bool {
        let (reply, answer) = oneshot::channel();
        let _ = self.to_control.send(NodeMsg::SealAndCheckpoint { reply });
        answer.await.unwrap_or(false)
    }

    /// Runs these classes' workers now, up to everything published, whatever `merge_wait_ms` says,
    /// and seals to make it durable. `None` if this node has no compactor.
    pub async fn compact_now(&self, classes: &[RetentionClass]) -> Option<CompactReport> {
        let (reply, answer) = oneshot::channel();
        self.compact_tx
            .as_ref()?
            .send(CompactCmd::Compact {
                classes: classes.to_vec(),
                reply,
            })
            .ok()?;
        answer.await.ok()
    }

    /// Runs the delete job now (section 7.4). `None` if this node has no compactor.
    pub async fn expire_now(&self) -> Option<ExpireReport> {
        let (reply, answer) = oneshot::channel();
        self.compact_tx
            .as_ref()?
            .send(CompactCmd::Expire { reply })
            .ok()?;
        answer.await.ok()
    }

    /// Runs the periodic check now, and returns its moves once they have finished.
    pub async fn rebalance_now(&self) -> Vec<ShardMove> {
        let (reply, answer) = oneshot::channel();
        let _ = self.to_control.send(NodeMsg::Rebalance { reply });
        answer.await.unwrap_or_default()
    }

    /// False, with nothing changed, if the core or the shard doesn't exist.
    pub async fn move_shard(&self, shard: u32, to: usize) -> bool {
        let (reply, answer) = oneshot::channel();
        let _ = self
            .to_control
            .send(NodeMsg::MoveShard { shard, to, reply });
        answer.await.unwrap_or(false)
    }

    /// Gives a stream its own shard, on core `to`. `None` if the table is full or the core
    /// doesn't exist.
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
        self.compact_stop.take();
        self.request_stop();
        self.meta_stop.store(true, Ordering::SeqCst);
        for thread in self.threads.drain(..) {
            if let Err(panic) = thread.join() {
                std::panic::resume_unwind(panic);
            }
        }
        for thread in self.meta_threads.drain(..) {
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
        self.compact_stop.take();
        self.request_stop();
        self.meta_stop.store(true, Ordering::SeqCst);
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

/// A `PUBLISH` read off a socket. Its records are already in a slab, so nothing is copied.
#[derive(Debug)]
pub(crate) struct RegionPublish {
    pub stream: Arc<str>,
    pub handle: u32,
    pub producer_id: u64,
    pub producer_seq: u64,
    pub ack_level: Option<u8>,
    pub author: NonZeroU64,
    pub state: Option<StateChange>,
    pub records: SlabRegion,
}

/// A `STATE_OP` read off a socket.
#[derive(Debug)]
pub(crate) struct OwnedStateOp {
    pub stream: Arc<str>,
    pub req: u32,
    pub author: NonZeroU64,
    pub change: StateChange,
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
/// Order across this client's messages for a stream holds through moves. While a barrier
/// is out, writes to that stream wait. The route only changes when the barrier comes back,
/// so everything sent down the old route has been handled by then, and nothing can
/// overtake it (D65).
pub struct Client {
    links: Arc<Links>,
    virtual_shard_count: u32,
    location_hint: Arc<Vec<u16>>,
    routes: HashMap<Arc<str>, ClientRoute>,
    reply_tx: mpsc::UnboundedSender<Reply>,
    reply_rx: mpsc::UnboundedReceiver<Reply>,
    /// Replies that arrived while a write waited for a barrier, for `recv` to return.
    received: VecDeque<Reply>,
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

    /// Waits for a barrier out on this stream's route, so that a write can't be sent behind
    /// it. Safe to cancel: what has arrived is kept.
    async fn wait_for_barrier(&mut self, stream: &str) -> Result<(), ClientError> {
        while let Some(route) = self
            .routes
            .get(stream)
            .filter(|route| route.barrier_pending)
        {
            let core = route.core;
            tokio::select! {
                biased;
                reply = self.reply_rx.recv() => {
                    if let Some(reply) = reply.and_then(|reply| self.on_internal(reply)) {
                        self.received.push_back(reply);
                    }
                }
                _ = self.links.direct[core].closed() => return Err(ClientError::NodeStopped),
            }
        }
        Ok(())
    }

    /// `wait_for_barrier`, with only what has already arrived. True if none is out.
    fn barrier_is_back(&mut self, stream: &str) -> bool {
        while self
            .routes
            .get(stream)
            .is_some_and(|route| route.barrier_pending)
        {
            let Ok(reply) = self.reply_rx.try_recv() else {
                return false;
            };
            if let Some(reply) = self.on_internal(reply) {
                self.received.push_back(reply);
            }
        }
        true
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
                passed: Vec::new(),
            })),
        )
    }

    /// Reads the frame into a slab and hands it to the owner. Waits while the memory cap
    /// is reached or the core's inbox is full, which is what stops reading a socket (D7).
    pub async fn publish(&mut self, publish: Publish<'_>) -> Result<(), ClientError> {
        while self.pool.account().pinned_bytes() >= self.memory_cap_bytes {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        self.wait_for_barrier(publish.stream).await?;
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
        if !self.barrier_is_back(publish.stream) {
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

    /// A permit to send on this stream's route, or nothing if a write has to wait: at the
    /// memory cap, while a barrier is out, or with the core's inbox full.
    fn write_permit(
        &mut self,
        stream: &str,
    ) -> Result<(Arc<str>, mpsc::Permit<'_, WriteMsg>), ClientError> {
        if self.pool.account().pinned_bytes() >= self.memory_cap_bytes {
            return Err(ClientError::Full);
        }
        if !self.barrier_is_back(stream) {
            return Err(ClientError::Full);
        }
        let (name, core) = self.route(stream);
        let permit = self.links.direct[core]
            .try_reserve()
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(()) => ClientError::Full,
                mpsc::error::TrySendError::Closed(()) => ClientError::NodeStopped,
            })?;
        Ok((name, permit))
    }

    /// Hands a publish to its owner, or gives it back if that has to wait (D7). A socket
    /// task must go on writing meanwhile, so it can't await this.
    // The large `Err` is the publish coming back, moved once, on a path that waits anyway.
    #[allow(clippy::result_large_err)]
    pub(crate) fn try_publish_region(
        &mut self,
        publish: RegionPublish,
    ) -> Result<(), (ClientError, RegionPublish)> {
        let reply = self.reply_tx.clone();
        match self.write_permit(&publish.stream) {
            Err(error) => Err((error, publish)),
            Ok((stream, permit)) => {
                permit.send(WriteMsg::Publish(Box::new(PublishMsg {
                    stream,
                    handle: publish.handle,
                    producer_id: publish.producer_id,
                    producer_seq: publish.producer_seq,
                    ack_level: publish.ack_level,
                    author: publish.author,
                    state: publish.state,
                    records: publish.records,
                    reply,
                    passed: Vec::new(),
                })));
                Ok(())
            }
        }
    }

    /// As `try_publish_region`.
    #[allow(clippy::result_large_err)]
    pub(crate) fn try_state_op(
        &mut self,
        op: OwnedStateOp,
    ) -> Result<(), (ClientError, OwnedStateOp)> {
        let reply = self.reply_tx.clone();
        match self.write_permit(&op.stream) {
            Err(error) => Err((error, op)),
            Ok((stream, permit)) => {
                permit.send(WriteMsg::StateOp(Box::new(StateOpMsg {
                    stream,
                    req: op.req,
                    author: op.author,
                    change: op.change,
                    reply,
                    passed: Vec::new(),
                })));
                Ok(())
            }
        }
    }

    pub async fn stream_info(&mut self, stream: &str) -> Result<StreamInfo, ErrorCode> {
        let (stream, core) = self.route(stream);
        let (reply, answer) = oneshot::channel();
        let _ = self.links.control[core].send(ControlMsg::StreamInfo { stream, reply });
        answer.await.unwrap_or(Err(ErrorCode::NotFound))
    }

    /// Creates the stream if needed, and changes what the patch sets.
    pub(crate) async fn configure_patch(
        &mut self,
        stream: &str,
        patch: StreamConfigPatch,
    ) -> Option<StreamConfig> {
        let (stream, core) = self.route(stream);
        let (reply, answer) = oneshot::channel();
        let _ = self.links.control[core].send(ControlMsg::ConfigurePatch {
            stream,
            patch,
            reply,
        });
        answer.await.ok()
    }

    /// False if there was no such stream. On a node with a metadata store this answers once a
    /// checkpoint holds the delete (D20).
    pub async fn delete_stream(&mut self, stream: &str) -> bool {
        let (stream, core) = self.route(stream);
        let (reply, answer) = oneshot::channel();
        let _ = self.links.control[core].send(ControlMsg::DeleteStream { stream, reply });
        answer.await.unwrap_or(false)
    }

    pub(crate) async fn sink_op(&mut self, stream: &str, op: SinkOp) -> Result<(), ErrorCode> {
        let (stream, core) = self.route(stream);
        let (reply, answer) = oneshot::channel();
        let _ = self.links.control[core].send(ControlMsg::Sink { stream, op, reply });
        answer.await.unwrap_or(Err(ErrorCode::NotFound))
    }

    /// `PUT .../cursor/<name>`: forward only unless `reset`. Answers with where it ended up.
    pub(crate) async fn put_cursor(
        &mut self,
        stream: &str,
        name: &str,
        id: u64,
        reset: bool,
    ) -> Result<u64, ErrorCode> {
        let (stream, core) = self.route(stream);
        let (reply, answer) = oneshot::channel();
        let _ = self.links.control[core].send(ControlMsg::Cursor {
            stream,
            name: Arc::from(name),
            op: CursorOp::Put { id, reset, reply },
            report: None,
            ticket: None,
        });
        answer.await.unwrap_or(Err(ErrorCode::NotFound))
    }

    /// A reply if one is here already.
    pub(crate) fn try_recv(&mut self) -> Option<Reply> {
        if let Some(reply) = self.received.pop_front() {
            return Some(reply);
        }
        while let Ok(reply) = self.reply_rx.try_recv() {
            if let Some(reply) = self.on_internal(reply) {
                return Some(reply);
            }
        }
        None
    }

    pub async fn state_op(&mut self, request: StateOpRequest<'_>) -> Result<(), ClientError> {
        self.wait_for_barrier(request.stream).await?;
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
            passed: Vec::new(),
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
                        passed: Vec::new(),
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
        if let Some(reply) = self.received.pop_front() {
            return Some(reply);
        }
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
