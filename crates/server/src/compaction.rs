//! Retention and compaction (SPEC sections 7.4, 7.5): seal manifests, one worker per retention
//! class following them with a named cursor, and the delete job.
//!
//! Workers read segments and write segments, and copy records without opening them: only the
//! manifests themselves, which are system records, are ever read. A worker changes what readers
//! see only through a stream's owner core, one message per extent, so a reader finds either the
//! old extents or the new one and goes by id either way. The old objects stay until their hour
//! expires. A cursor moves only once a checkpoint holds the new extents, so a crash at any point
//! redoes work, and what it left behind is an object nothing refers to.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use format::{ENTRY_SEAL_MANIFEST, KeyHash, Records, SystemRecord, decode_manifest_doc, id_ms};
use object_store::ObjectStore;
use object_store::path::Path;
use proto::ErrorCode;
use storage::{
    Ownership, SEALED_PREFIX, SegmentBuilder, SegmentError, SegmentReader, StreamExtent,
    compacted_path, list_hour_objects, list_hours, object_generation, object_hour, put_new_object,
    read_ownership,
};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::clock::Clock;
use crate::connection::{Connection, Event, Subscription};
use crate::core::Links;
use crate::delivery::ReplaceResult;
use crate::keys::Keys;
use crate::message::{CompactSource, CompactView, ControlMsg, CursorOp};
use crate::node::{NodeMsg, sleep_until_some};
use crate::placement::shard_of;
use crate::recovery::accepted_prefix;

const HOUR_MS: u64 = 3_600_000;
const DAY_MS: u64 = 24 * HOUR_MS;
/// How long `sealed/` keeps an hour after it has ended: section 7.4's "about 2 days".
pub const SEALED_EXPIRY_MS: u64 = 2 * DAY_MS;
/// What a class's prefix keeps past its length. A prefix is named for its highest id's hour
/// (D175), so nothing needs this today; it is a margin for a clock that steps back.
pub const CLASS_GRACE_MS: u64 = HOUR_MS;
/// How long a worker waits for an owner to hear of an object it was told of.
const NOT_READY_RETRY_MS: u64 = 20;
const NOT_READY_RETRY_COUNT: u32 = 100;
/// How long a read of the manifest stream waits with nothing arriving.
const LOG_QUIET_MS: u64 = 5_000;

/// The classes that are compacted. `1d` isn't one: it is served from `sealed/` (section 7.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RetentionClass {
    Days7,
    Days30,
    Days365,
}

impl RetentionClass {
    pub const ALL: [RetentionClass; 3] = [
        RetentionClass::Days7,
        RetentionClass::Days30,
        RetentionClass::Days365,
    ];

    pub fn prefix(self) -> &'static str {
        match self {
            RetentionClass::Days7 => "7d",
            RetentionClass::Days30 => "30d",
            RetentionClass::Days365 => "365d",
        }
    }

    pub fn length_ms(self) -> u64 {
        match self {
            RetentionClass::Days7 => 7 * DAY_MS,
            RetentionClass::Days30 => 30 * DAY_MS,
            RetentionClass::Days365 => 365 * DAY_MS,
        }
    }

    /// The named cursor of this class's worker. Clients can't use these names.
    pub fn cursor_name(self) -> String {
        format!("compact:{}", self.prefix())
    }

    /// `None` for what stays in `sealed/`.
    pub fn of_retention(retention_ms: u64) -> Option<RetentionClass> {
        if retention_ms <= DAY_MS {
            None
        } else if retention_ms <= 7 * DAY_MS {
            Some(RetentionClass::Days7)
        } else if retention_ms <= 30 * DAY_MS {
            Some(RetentionClass::Days30)
        } else {
            Some(RetentionClass::Days365)
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// How often the workers run. `None` runs them only when asked.
    pub compact_period_ms: Option<u64>,
    /// How often the delete job runs. `None` runs it only when asked.
    pub expire_period_ms: Option<u64>,
    /// How long a worker waits for more within an hour before writing what it has.
    pub merge_wait_ms: u64,
    /// A worker writes once it has this many bytes (section 7.3's segment sizes).
    pub target_output_bytes: u64,
    pub sealed_expiry_ms: u64,
}

impl Default for CompactionConfig {
    fn default() -> CompactionConfig {
        CompactionConfig {
            compact_period_ms: Some(60_000),
            expire_period_ms: Some(10 * 60_000),
            merge_wait_ms: HOUR_MS,
            target_output_bytes: 128 * 1024 * 1024,
            sealed_expiry_ms: SEALED_EXPIRY_MS,
        }
    }
}

/// What a run of the workers did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactReport {
    /// Objects written, each referred to by at least one extent unless a crash intervened.
    pub outputs: Vec<Path>,
    /// Extents readers now find in those objects.
    pub replaced_count: u64,
    /// Where each worker that ran has got to.
    pub cursors: Vec<(RetentionClass, u64)>,
    /// Workers that stopped on something that will be tried again.
    pub errors: Vec<String>,
}

/// What a run of the delete job did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExpireReport {
    /// Whole hour prefixes emptied, such as `sealed/500000`.
    pub deleted_prefixes: Vec<String>,
    pub deleted_object_count: u64,
    /// Expired `sealed/` prefixes kept because compaction hasn't passed them (section 7.4).
    pub blocked_prefixes: Vec<String>,
    /// A later generation has claimed the bucket, so this node deletes nothing.
    pub not_owner: bool,
    pub errors: Vec<String>,
}

pub(crate) enum CompactCmd {
    Compact {
        classes: Vec<RetentionClass>,
        reply: oneshot::Sender<CompactReport>,
    },
    Expire {
        reply: oneshot::Sender<ExpireReport>,
    },
}

/// What the manifest stream has said so far. It is read again after a restart, and what it
/// has lost by then went with objects that have expired.
#[derive(Default)]
struct ManifestLog {
    /// `id → path`.
    by_id: BTreeMap<u64, Path>,
    through: u64,
}

impl ManifestLog {
    fn add(&mut self, bytes: &[u8]) {
        for record in Records::new(bytes) {
            let Ok(record) = record else {
                return;
            };
            self.through = self.through.max(record.header.last_id());
            // The one kind of data here that may be read (section 2.2).
            if !record.header.is_system() || record.header.options != ENTRY_SEAL_MANIFEST {
                continue;
            }
            let Ok(system) = SystemRecord::parse(&record) else {
                continue;
            };
            let Ok((_, path)) = decode_manifest_doc(system.doc) else {
                continue;
            };
            if let Ok(path) = Path::parse(path) {
                self.by_id.insert(record.header.id, path);
            }
        }
    }
}

struct Source {
    path: Path,
    extent: StreamExtent,
    next_id_first: Option<u64>,
    /// How far a former generation's object is accepted (D17).
    limit: Option<u64>,
}

/// One output's worth of work.
#[derive(Default)]
struct Batch {
    /// Sorted by stream, so the output is (section 7.5). `(sources, merge_below)`.
    streams: BTreeMap<KeyHash, (Vec<Source>, u64)>,
    bytes: u64,
    /// The last manifest it covers.
    through: Option<u64>,
    hour: u64,
    first_manifest_id: u64,
    /// What couldn't be read and was gone past, to report.
    skipped: Vec<String>,
}

enum Planned {
    /// Nothing above the cursor.
    Nothing,
    /// There is something, and it isn't time yet.
    Wait,
    /// An owner hasn't heard of an object yet.
    NotReady,
    Ready(Batch),
}

struct Run {
    stream: KeyHash,
    sources: Vec<(Path, u64, u64)>,
    first_id: u64,
}

/// Runs on its own thread, and reaches everything through messages and the store.
pub(crate) struct Compactor {
    pub config: CompactionConfig,
    pub node_name: String,
    pub generation: u64,
    pub ownership: Ownership,
    pub store: Arc<dyn ObjectStore>,
    pub clock: Arc<dyn Clock>,
    pub links: Links,
    pub keys: Keys,
    pub virtual_shard_count: u32,
    pub core_count: usize,
    pub window_bytes: u64,
    pub has_seal_window: bool,
    pub to_control: mpsc::UnboundedSender<NodeMsg>,
    /// Objects every owner has heard of: those a checkpoint recorded before this start, and
    /// those this job has dropped. A footer range no stream lists means they dropped it, where
    /// for another object it can mean the owner's report is still on its way.
    pub noted: Mutex<HashSet<String>>,
}

fn is_not_found(error: &SegmentError) -> bool {
    matches!(
        error,
        SegmentError::Store(object_store::Error::NotFound { .. })
    )
}

/// Bytes that are in the store and wrong, which no retry changes. Anything from the store itself
/// may be an outage.
fn is_corrupt(error: &SegmentError) -> bool {
    !matches!(error, SegmentError::Store(_))
}

/// How a footer read went.
enum FooterRead {
    Found(Vec<StreamExtent>),
    /// The object isn't there.
    Missing,
    Corrupt(String),
}

impl Compactor {
    pub(crate) fn run(
        self,
        rx: mpsc::UnboundedReceiver<CompactCmd>,
        stopped: oneshot::Receiver<()>,
    ) {
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return;
        };
        runtime.block_on(async {
            tokio::select! {
                _ = stopped => {}
                _ = self.serve(rx) => {}
            }
        });
        // A store call in progress on a hung store mustn't hold up the stop.
        runtime.shutdown_background();
    }

    async fn serve(&self, mut rx: mpsc::UnboundedReceiver<CompactCmd>) {
        let after = |period_ms: Option<u64>| {
            period_ms.map(|period_ms| Instant::now() + Duration::from_millis(period_ms))
        };
        let mut log = ManifestLog::default();
        let mut next_compact = after(self.config.compact_period_ms);
        let mut next_expire = after(self.config.expire_period_ms);
        loop {
            tokio::select! {
                biased;
                command = rx.recv() => match command {
                    Some(CompactCmd::Compact { classes, reply }) => {
                        let _ = reply.send(self.compact(&mut log, &classes, true).await);
                    }
                    Some(CompactCmd::Expire { reply }) => {
                        let _ = reply.send(self.expire(&mut log).await);
                    }
                    None => return,
                },
                _ = sleep_until_some(next_compact) => {
                    next_compact = after(self.config.compact_period_ms);
                    let report = self.compact(&mut log, &RetentionClass::ALL, false).await;
                    for error in report.errors {
                        eprintln!("compaction, will retry: {error}");
                    }
                }
                _ = sleep_until_some(next_expire) => {
                    next_expire = after(self.config.expire_period_ms);
                    let report = self.expire(&mut log).await;
                    for error in report.errors {
                        eprintln!("the delete job, will retry: {error}");
                    }
                }
            }
        }
    }

    /// Where a message about `stream` goes first. A core that gave it away passes it on (D66).
    fn first_hop(&self, stream: KeyHash) -> &mpsc::UnboundedSender<ControlMsg> {
        let shard = shard_of(stream, self.virtual_shard_count);
        &self.links.control[shard as usize % self.core_count]
    }

    /// Reads whatever the stream has past what `log` has. Stopping early is fine: the next run
    /// carries on, and a worker only moves past what it has read. True if it reached the head,
    /// so a manifest not in `log` is one that has expired.
    async fn refresh(&self, log: &mut ManifestLog) -> bool {
        let Some(mut connection) =
            Connection::open(self.links.control[0].clone(), self.window_bytes).await
        else {
            return false;
        };
        let mut from = log.through;
        let subscribe = |connection: &mut Connection, from: u64| {
            let mut subscription = Subscription::new(1, self.keys.manifest, from);
            subscription.include_system = true;
            connection.subscribe(subscription);
        };
        subscribe(&mut connection, from);
        let mut head = None;
        while let Some(event) = connection.recv_within(LOG_QUIET_MS).await {
            match event {
                // Nothing is left to read, and no `Progress` will say so: all of it has expired.
                Event::Subscribed {
                    head: at,
                    caught_up: true,
                    ..
                } => {
                    log.through = log.through.max(at);
                    return true;
                }
                Event::Subscribed { head: at, .. } => head = Some(at),
                Event::Data { records, .. } => log.add(&records.to_vec()),
                Event::Progress { id, .. } => log.through = log.through.max(id),
                // An earlier read stopped short, and the cursors were already ahead of it, so
                // what it hadn't got to has gone. What's left is where to read from (D179).
                Event::Error {
                    code: ErrorCode::RetentionExpired,
                    ..
                } if from != 0 => {
                    from = 0;
                    subscribe(&mut connection, from);
                }
                Event::Error { code, msg, .. } => {
                    if code == ErrorCode::NotFound {
                        return true;
                    }
                    eprintln!("reading the manifest stream: {msg}");
                    return false;
                }
                Event::Behind { .. } | Event::State { .. } => {}
            }
            if head.is_some_and(|head| log.through >= head) {
                return true;
            }
        }
        false
    }

    async fn read_cursor(&self, class: RetentionClass) -> Result<u64, String> {
        let (reply, answer) = oneshot::channel();
        self.first_hop(self.keys.manifest)
            .send(ControlMsg::Cursor {
                stream: self.keys.manifest,
                name: self.keys.hash(&class.cursor_name()),
                op: CursorOp::Read(reply),
                report: None,
                ticket: None,
            })
            .map_err(|_| "the node has stopped".to_owned())?;
        let cursor = answer
            .await
            .map_err(|_| "the node has stopped".to_owned())?;
        Ok(cursor.unwrap_or(0))
    }

    async fn commit_cursor(&self, class: RetentionClass, id: u64) -> Result<(), String> {
        let (reply, answer) = oneshot::channel();
        self.first_hop(self.keys.manifest)
            .send(ControlMsg::Cursor {
                stream: self.keys.manifest,
                name: self.keys.hash(&class.cursor_name()),
                op: CursorOp::Put {
                    id,
                    reset: false,
                    reply,
                },
                report: None,
                ticket: None,
            })
            .map_err(|_| "the node has stopped".to_owned())?;
        match answer.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(code)) => Err(format!("committing a compaction cursor: {code:?}")),
            Err(_) => Err("the node has stopped".to_owned()),
        }
    }

    /// Waits until a checkpoint holds what the owners have been told. Asking for a seal costs a
    /// PUT, so a node with a seal window just waits for its next one (section 7.1).
    async fn wait_for_checkpoint(&self, seal_now: bool) -> Result<(), String> {
        let (reply, answer) = oneshot::channel();
        let message = if seal_now || !self.has_seal_window {
            NodeMsg::SealAndCheckpoint { reply }
        } else {
            NodeMsg::NextCheckpoint { reply }
        };
        self.to_control
            .send(message)
            .map_err(|_| "the node has stopped".to_owned())?;
        match answer.await {
            Ok(true) => Ok(()),
            _ => Err("no checkpoint could be saved".to_owned()),
        }
    }

    async fn compact(
        &self,
        log: &mut ManifestLog,
        classes: &[RetentionClass],
        forced: bool,
    ) -> CompactReport {
        if forced && let Err(error) = self.wait_for_checkpoint(true).await {
            return CompactReport {
                errors: vec![error],
                ..CompactReport::default()
            };
        }
        self.refresh(log).await;
        let log: &ManifestLog = log;
        // Side by side, so a class stuck on something leaves the others going.
        let run = |class: RetentionClass| async move {
            if classes.contains(&class) {
                Some((class, self.run_class(class, log, forced).await))
            } else {
                None
            }
        };
        let (days7, days30, days365) = tokio::join!(
            run(RetentionClass::Days7),
            run(RetentionClass::Days30),
            run(RetentionClass::Days365)
        );
        let mut report = CompactReport::default();
        for (class, (one, cursor)) in [days7, days30, days365].into_iter().flatten() {
            report.outputs.extend(one.outputs);
            report.replaced_count += one.replaced_count;
            report.errors.extend(one.errors);
            report.cursors.push((class, cursor));
        }
        report
    }

    /// One worker: batches until none is due.
    async fn run_class(
        &self,
        class: RetentionClass,
        log: &ManifestLog,
        forced: bool,
    ) -> (CompactReport, u64) {
        let mut report = CompactReport::default();
        let mut cursor = match self.read_cursor(class).await {
            Ok(cursor) => cursor,
            Err(error) => {
                report.errors.push(error);
                return (report, 0);
            }
        };
        let mut not_ready_count = 0;
        loop {
            let planned = match self.plan_batch(class, log, cursor, forced).await {
                Ok(planned) => planned,
                Err(error) => {
                    report.errors.push(error);
                    return (report, cursor);
                }
            };
            match planned {
                Planned::Nothing | Planned::Wait => return (report, cursor),
                Planned::NotReady => {
                    not_ready_count += 1;
                    if not_ready_count >= NOT_READY_RETRY_COUNT {
                        report
                            .errors
                            .push("an owner hasn't heard of an uploaded object yet".to_owned());
                        return (report, cursor);
                    }
                    tokio::time::sleep(Duration::from_millis(NOT_READY_RETRY_MS)).await;
                }
                Planned::Ready(mut batch) => {
                    let through = batch.through.expect("a ready batch covers a manifest");
                    report.errors.append(&mut batch.skipped);
                    match self.run_batch(class, batch, forced, &mut report).await {
                        Ok(true) => cursor = through,
                        Ok(false) => {
                            report
                                .errors
                                .push("an owner couldn't take a compacted extent yet".to_owned());
                            return (report, cursor);
                        }
                        Err(error) => {
                            report.errors.push(error);
                            return (report, cursor);
                        }
                    }
                }
            }
        }
    }

    async fn read_footer(&self, path: &Path) -> Result<FooterRead, String> {
        let reader = SegmentReader::new(Arc::clone(&self.store), path.clone());
        let extents = async {
            let top = reader.read_top_index().await?;
            reader.all_extents(&top).await
        };
        match extents.await {
            Ok(extents) => Ok(FooterRead::Found(extents)),
            Err(error) if is_not_found(&error) => Ok(FooterRead::Missing),
            Err(error) if is_corrupt(&error) => Ok(FooterRead::Corrupt(format!("{path}: {error}"))),
            Err(error) => Err(format!("{path}: {error}")),
        }
    }

    async fn view(
        &self,
        stream: &KeyHash,
        path: &Path,
        footer: Vec<(u64, u64)>,
    ) -> Result<Option<CompactView>, String> {
        let (reply, answer) = oneshot::channel();
        self.first_hop(*stream)
            .send(ControlMsg::CompactView {
                stream: *stream,
                path: path.clone(),
                footer,
                reply,
            })
            .map_err(|_| "the node has stopped".to_owned())?;
        let mut view = answer
            .await
            .map_err(|_| "the node has stopped".to_owned())?;
        if let Some(view) = &mut view
            && self.is_noted(path)
        {
            view.unnoted = false;
        }
        Ok(view)
    }

    fn is_noted(&self, path: &Path) -> bool {
        self.noted
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(path.as_ref())
    }

    /// The footer's id ranges by stream, leaving out what the manifest stream has (it is never
    /// compacted, Q4) and what readers ignore of a former generation.
    fn footer_by_stream(
        &self,
        extents: &[StreamExtent],
        limit: Option<u64>,
    ) -> BTreeMap<KeyHash, Vec<(u64, u64)>> {
        let mut footer: BTreeMap<KeyHash, Vec<(u64, u64)>> = BTreeMap::new();
        for extent in extents {
            if extent.stream == self.keys.manifest
                || limit.is_some_and(|limit| extent.id_first > limit)
            {
                continue;
            }
            let id_last = limit.map_or(extent.id_last, |limit| extent.id_last.min(limit));
            footer
                .entry(extent.stream)
                .or_default()
                .push((extent.id_first, id_last));
        }
        footer
    }

    async fn plan_batch(
        &self,
        class: RetentionClass,
        log: &ManifestLog,
        cursor: u64,
        forced: bool,
    ) -> Result<Planned, String> {
        let mut batch = Batch::default();
        let mut closed = false;
        let mut not_ready = false;
        'manifests: for (id, path) in log.by_id.range(cursor.saturating_add(1)..) {
            let Some(hour) = object_hour(path) else {
                batch.through = Some(*id);
                continue;
            };
            if !batch.streams.is_empty() && hour != batch.hour {
                closed = true;
                break;
            }
            let footer = match self.read_footer(path).await? {
                FooterRead::Found(footer) => footer,
                // Expired: whatever it held went with it, or lives on in a compacted object.
                FooterRead::Missing => {
                    batch.through = Some(*id);
                    continue;
                }
                // Nothing else waits for it. The delete job can't read it either, so the object
                // and its hour stay, and owners go on listing what's in it (D177).
                FooterRead::Corrupt(error) => {
                    batch.skipped.push(error);
                    batch.through = Some(*id);
                    continue;
                }
            };
            let limit = object_generation(path).and_then(|at| self.ownership.limit_for(at));
            let mut found = Vec::new();
            for (stream, ranges) in self.footer_by_stream(&footer, limit) {
                let Some(view) = self.view(&stream, path, ranges).await? else {
                    continue;
                };
                if RetentionClass::of_retention(view.retention_ms) != Some(class) {
                    continue;
                }
                // Only this generation's uploads can still be on their way to an owner. A former
                // one's were all read at startup, and what is unlisted was left out (D181).
                if view.unnoted && object_generation(path) == Some(self.generation) {
                    not_ready = true;
                    break 'manifests;
                }
                if !view.sources.is_empty() {
                    found.push((stream, view));
                }
            }
            if batch.streams.is_empty() {
                batch.first_manifest_id = *id;
                batch.hour = hour;
            }
            for (stream, view) in found {
                let entry = batch
                    .streams
                    .entry(stream)
                    .or_insert_with(|| (Vec::new(), u64::MAX));
                entry.1 = entry.1.min(view.merge_below);
                for CompactSource {
                    extent,
                    next_id_first,
                } in view.sources
                {
                    batch.bytes += extent.len_bytes;
                    entry.0.push(Source {
                        path: path.clone(),
                        extent,
                        next_id_first,
                        limit,
                    });
                }
            }
            batch.through = Some(*id);
            if batch.bytes >= self.config.target_output_bytes {
                closed = true;
                break;
            }
        }
        if batch.through.is_none() {
            return Ok(if not_ready {
                Planned::NotReady
            } else {
                Planned::Nothing
            });
        }
        if !closed && !not_ready && !forced && !batch.streams.is_empty() {
            let now_ms = self.clock.now_ms();
            // An id ahead of the clock says nothing of its age: ids after a restart are (D221).
            let manifest_ms = id_ms(batch.first_manifest_id);
            let young = manifest_ms <= now_ms
                && now_ms < manifest_ms.saturating_add(self.config.merge_wait_ms);
            if young && batch.hour == now_ms / HOUR_MS {
                return Ok(Planned::Wait);
            }
        }
        Ok(Planned::Ready(batch))
    }

    /// One source's records, as stored. If the object has gone from under its extent, the owner
    /// is told, this batch stops, and the next run goes without it. `Err` of a corrupt source
    /// comes back as `Ok(Err)`: only that source is left behind (D177).
    async fn read_source(
        &self,
        stream: &KeyHash,
        source: &Source,
    ) -> Result<Result<Bytes, String>, String> {
        let reader = SegmentReader::new(Arc::clone(&self.store), source.path.clone());
        let bytes = match reader.read_extent(&source.extent).await {
            Ok(bytes) => bytes,
            Err(error) if is_corrupt(&error) => {
                return Ok(Err(format!("{}: {error}", source.path)));
            }
            Err(error) => {
                if is_not_found(&error) {
                    self.drop_extents(stream, &source.path).await?;
                }
                return Err(format!("{}: {error}", source.path));
            }
        };
        let Some(limit) = source.limit else {
            return Ok(Ok(bytes));
        };
        // An extent of a former generation stops at its last accepted record (D17, D138).
        Ok(Ok(accepted_prefix(
            &bytes,
            limit.min(source.extent.id_last),
        )
        .map_or_else(Bytes::new, |(_, accepted)| accepted)))
    }

    /// True if the cursor may now pass this batch, which it has if the batch covers a manifest.
    async fn run_batch(
        &self,
        class: RetentionClass,
        batch: Batch,
        forced: bool,
        report: &mut CompactReport,
    ) -> Result<bool, String> {
        let through = batch.through;
        let mut builder = SegmentBuilder::new();
        let mut runs = Vec::new();
        for (stream, (mut sources, merge_below)) in batch.streams {
            sources.sort_by_key(|source| source.extent.id_first);
            sources.dedup_by_key(|source| source.extent.id_first);
            let mut start = 0;
            while start < sources.len() {
                // Neighbours become one extent, unless something unsealed could be between them.
                let mut end = start + 1;
                while end < sources.len()
                    && sources[end - 1].next_id_first == Some(sources[end].extent.id_first)
                    && sources[end].extent.id_last < merge_below
                {
                    end += 1;
                }
                let mut pieces = Vec::with_capacity(end - start);
                let mut corrupt = false;
                for source in &sources[start..end] {
                    match self.read_source(&stream, source).await? {
                        Ok(piece) => pieces.push(piece),
                        Err(error) => {
                            report.errors.push(error);
                            corrupt = true;
                        }
                    }
                }
                // Left where they are. The delete job tries again before their hour can go.
                if corrupt {
                    start = end;
                    continue;
                }
                pieces.retain(|piece| !piece.is_empty());
                if let Some(first) = pieces.first() {
                    let first_id = format::RecordHeader::read(first)
                        .map_err(|error| format!("a source of {stream}: {error}"))?
                        .id;
                    builder
                        .add_extent(stream, pieces)
                        .map_err(|error| format!("building for {stream}: {error}"))?;
                    runs.push(Run {
                        stream,
                        sources: sources[start..end]
                            .iter()
                            .map(|source| {
                                (
                                    source.path.clone(),
                                    source.extent.id_first,
                                    source.extent.id_last,
                                )
                            })
                            .collect(),
                        first_id,
                    });
                }
                start = end;
            }
        }
        if !runs.is_empty() {
            let built = builder.finish().map_err(|error| error.to_string())?;
            let first_id = built.first_id.expect("at least one record was added");
            let hour = batch.hour;
            let path = put_new_object(
                self.store.as_ref(),
                |first_id| {
                    compacted_path(
                        class.prefix(),
                        hour,
                        &self.node_name,
                        self.generation,
                        first_id,
                    )
                },
                first_id,
                &built.parts,
            )
            .await?;
            storage::crash_point("compact.after_output");
            let by_first: HashMap<(KeyHash, u64), &StreamExtent> = built
                .extents
                .iter()
                .map(|extent| ((extent.stream, extent.id_first), extent))
                .collect();
            let mut applied_count = 0;
            let mut retry = false;
            for run in &runs {
                let extent = by_first
                    .get(&(run.stream, run.first_id))
                    .expect("every run went into the builder");
                match self
                    .replace_extents(&run.stream, &path, run.sources.clone(), extent)
                    .await?
                {
                    ReplaceResult::Applied => applied_count += 1,
                    ReplaceResult::Gone => {}
                    ReplaceResult::Retry => retry = true,
                }
            }
            storage::crash_point("compact.after_extents");
            report.outputs.push(path);
            report.replaced_count += applied_count;
            if applied_count > 0 {
                self.wait_for_checkpoint(forced).await?;
            }
            if retry {
                return Ok(false);
            }
        }
        storage::crash_point("compact.before_cursor");
        if let Some(through) = through {
            self.commit_cursor(class, through).await?;
        }
        Ok(true)
    }

    async fn replace_extents(
        &self,
        stream: &KeyHash,
        path: &Path,
        sources: Vec<(Path, u64, u64)>,
        extent: &StreamExtent,
    ) -> Result<ReplaceResult, String> {
        let (reply, answer) = oneshot::channel();
        self.first_hop(*stream)
            .send(ControlMsg::ReplaceExtents {
                stream: *stream,
                path: path.clone(),
                sources,
                extent: extent.clone(),
                reply,
            })
            .map_err(|_| "the node has stopped".to_owned())?;
        answer.await.map_err(|_| "the node has stopped".to_owned())
    }

    async fn drop_extents(&self, stream: &KeyHash, path: &Path) -> Result<bool, String> {
        self.noted
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(path.to_string());
        let (reply, answer) = oneshot::channel();
        self.first_hop(*stream)
            .send(ControlMsg::DropExtents {
                stream: *stream,
                path: path.clone(),
                reply,
            })
            .map_err(|_| "the node has stopped".to_owned())?;
        answer.await.map_err(|_| "the node has stopped".to_owned())
    }

    /// Whether an expired `sealed/` object may go: every worker has passed its manifest and the
    /// manifest records it holds. An object with no manifest passes only if no stream points at
    /// it, or its manifest has expired. Everything here errs towards keeping it.
    async fn sealed_object_passed(
        &self,
        path: &Path,
        extents: &[StreamExtent],
        manifest_ids: &HashMap<&Path, u64>,
        log_complete: bool,
        slowest_cursor: u64,
    ) -> Result<bool, String> {
        let holds_unread_manifests = extents
            .iter()
            .any(|extent| extent.stream == self.keys.manifest && extent.id_last > slowest_cursor);
        if holds_unread_manifests {
            return Ok(false);
        }
        let manifest_passed = match manifest_ids.get(path) {
            Some(id) => *id <= slowest_cursor,
            // A recorded object's manifest was saved in an object (D180). With all of the stream
            // read and no sign of it, that object has gone, which takes every worker passing it
            // (D182). A longer class left behind is still copied below.
            None => log_complete && self.is_noted(path),
        };
        let limit = object_generation(path).and_then(|at| self.ownership.limit_for(at));
        let mut passed = true;
        for (stream, ranges) in self.footer_by_stream(extents, limit) {
            let Some(view) = self.view(&stream, path, ranges).await? else {
                continue;
            };
            if !manifest_passed {
                if view.unnoted || !view.sources.is_empty() {
                    return Ok(false);
                }
                continue;
            }
            // Every worker has gone past, each deciding when it got there. A stream of a longer
            // class still here slipped between them: this is its last chance to be copied.
            let Some(class) = RetentionClass::of_retention(view.retention_ms) else {
                continue;
            };
            if !view.sources.is_empty() {
                self.copy_passed_over(class, stream, path, view, limit)
                    .await?;
                passed = false;
            }
        }
        Ok(passed)
    }

    /// What every worker went past copied into its class's prefix, for the next run of the
    /// delete job to find gone from `sealed/` (D176).
    async fn copy_passed_over(
        &self,
        class: RetentionClass,
        stream: KeyHash,
        path: &Path,
        view: CompactView,
        limit: Option<u64>,
    ) -> Result<(), String> {
        let mut batch = Batch {
            hour: object_hour(path).ok_or_else(|| format!("{path}: no hour in the path"))?,
            ..Batch::default()
        };
        let sources = view
            .sources
            .into_iter()
            .map(|source| Source {
                path: path.clone(),
                extent: source.extent,
                next_id_first: source.next_id_first,
                limit,
            })
            .collect();
        batch.streams.insert(stream, (sources, view.merge_below));
        let mut report = CompactReport::default();
        if !self.run_batch(class, batch, false, &mut report).await? {
            return Err("an owner couldn't take a compacted extent yet".to_owned());
        }
        match report.errors.pop() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// The delete job (section 7.4). Only whole hours under a prefix, once expired, and for
    /// `sealed/` only once compaction has passed all of the hour. Readers are moved off first:
    /// the deletes come after a checkpoint no longer points at these objects.
    async fn expire(&self, log: &mut ManifestLog) -> ExpireReport {
        let mut report = ExpireReport::default();
        // A running node can't otherwise tell it has been replaced, and its view of which
        // records were compacted isn't the new owner's (section 8.1).
        match read_ownership(self.store.as_ref()).await {
            Ok(ownership) if ownership.current_generation() == Some(self.generation) => {}
            Ok(_) => {
                report.not_owner = true;
                return report;
            }
            Err(error) => {
                report.errors.push(error.to_string());
                return report;
            }
        }
        let log_complete = self.refresh(log).await;
        let mut slowest_cursor = u64::MAX;
        for class in RetentionClass::ALL {
            match self.read_cursor(class).await {
                Ok(cursor) => slowest_cursor = slowest_cursor.min(cursor),
                Err(error) => {
                    report.errors.push(error);
                    return report;
                }
            }
        }
        let now_ms = self.clock.now_ms();
        let mut going: Vec<(String, Vec<Path>)> = Vec::new();
        let mut any_dropped = false;
        let mut prefixes = vec![(SEALED_PREFIX, self.config.sealed_expiry_ms)];
        for class in RetentionClass::ALL {
            prefixes.push((class.prefix(), class.length_ms() + CLASS_GRACE_MS));
        }
        let manifest_ids: HashMap<&Path, u64> = log
            .by_id
            .iter()
            .rev()
            .map(|(id, path)| (path, *id))
            .collect();
        for (root, expiry_ms) in prefixes {
            let hours = match list_hours(self.store.as_ref(), root).await {
                Ok(hours) => hours,
                Err(error) => {
                    report.errors.push(format!("{root}: {error}"));
                    continue;
                }
            };
            for hour in hours {
                // The hour comes from a listing, so any stray key can make it huge. Saturating
                // puts that one far in the future, where it stays.
                let expires_ms = hour
                    .saturating_add(1)
                    .saturating_mul(HOUR_MS)
                    .saturating_add(expiry_ms);
                if now_ms < expires_ms {
                    continue;
                }
                let prefix = format!("{root}/{hour}");
                match self
                    .check_hour(
                        root,
                        hour,
                        &manifest_ids,
                        log_complete,
                        slowest_cursor,
                        &mut any_dropped,
                    )
                    .await
                {
                    Ok(Some(paths)) => going.push((prefix, paths)),
                    Ok(None) => report.blocked_prefixes.push(prefix),
                    Err(error) => {
                        report.errors.push(format!("{prefix}: {error}"));
                        report.blocked_prefixes.push(prefix);
                    }
                }
            }
        }
        drop(manifest_ids);
        if going.is_empty() {
            return report;
        }
        if any_dropped && let Err(error) = self.wait_for_checkpoint(false).await {
            report.errors.push(error);
            return report;
        }
        storage::crash_point("expire.before_delete");
        for (prefix, paths) in going {
            let mut all_gone = true;
            for path in &paths {
                match self.store.delete(path).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                        report.deleted_object_count += 1;
                    }
                    Err(error) => {
                        all_gone = false;
                        report.errors.push(format!("{path}: {error}"));
                    }
                }
            }
            if all_gone {
                report.deleted_prefixes.push(prefix);
            }
            log.by_id.retain(|_, path| !paths.contains(path));
        }
        report
    }

    /// Everything in one expired hour, if it can all go, with owners told to stop pointing at
    /// it. Nothing is told unless the whole hour passes.
    async fn check_hour(
        &self,
        root: &str,
        hour: u64,
        manifest_ids: &HashMap<&Path, u64>,
        log_complete: bool,
        slowest_cursor: u64,
        any_dropped: &mut bool,
    ) -> Result<Option<Vec<Path>>, String> {
        let objects = list_hour_objects(self.store.as_ref(), root, hour)
            .await
            .map_err(|error| error.to_string())?;
        let mut paths = Vec::with_capacity(objects.len());
        let mut footers = Vec::with_capacity(objects.len());
        for object in objects {
            let path = object.location;
            let extents = match self.read_footer(&path).await? {
                FooterRead::Found(extents) => extents,
                FooterRead::Missing => Vec::new(),
                // A compacted object nobody could read: its time is up all the same. A sealed
                // one may hold what was never copied.
                FooterRead::Corrupt(_) if root != SEALED_PREFIX => Vec::new(),
                FooterRead::Corrupt(error) => return Err(error),
            };
            if root == SEALED_PREFIX
                && !self
                    .sealed_object_passed(
                        &path,
                        &extents,
                        manifest_ids,
                        log_complete,
                        slowest_cursor,
                    )
                    .await?
            {
                return Ok(None);
            }
            paths.push(path);
            footers.push(extents);
        }
        for (path, extents) in paths.iter().zip(&footers) {
            for extent in extents {
                *any_dropped |= self.drop_extents(&extent.stream, path).await?;
            }
        }
        Ok(Some(paths))
    }
}
