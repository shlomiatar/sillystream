//! Retention and compaction (SPEC sections 7.4, 7.5): seal manifests, one worker per retention
//! class following them with a named cursor, and the delete job.
//!
//! Workers read segments and write segments, and copy records without opening them: only the
//! manifests themselves, which are system records, are ever read. A worker changes what readers
//! see only through object table rows, saying which compacted object replaces each source for
//! its class and a range of stream hashes. It changes no stream's metadata (section 7.5). The old
//! objects stay until their hour expires. A cursor moves only once a checkpoint holds the rows,
//! so a crash at any point redoes work, and what it left behind is an object nothing refers to.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use format::{ENTRY_SEAL_MANIFEST, KeyHash, Records, SystemRecord, decode_manifest_doc, id_ms};
use object_store::ObjectStore;
use object_store::path::Path;
use proto::ErrorCode;
use storage::{
    ObjectId, ObjectKey, ObjectState, ObjectTable, Ownership, Replacement, SEALED_PREFIX,
    SegmentBuilder, SegmentError, SegmentReader, StreamExtent, list_hour_objects, list_hours,
    object_generation, object_hour, put_compacted, read_ownership, stream_hash,
};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::clock::Clock;
use crate::connection::{Connection, Event, Subscription};
use crate::core::Links;
use crate::keys::Keys;
use crate::message::{ControlMsg, CursorOp};
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
/// How long a read of the manifest stream waits with nothing arriving.
const LOG_QUIET_MS: u64 = 5_000;

pub use storage::RetentionClass;

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
    /// Objects written, each referred to by at least one row unless a crash intervened.
    pub outputs: Vec<Path>,
    /// Source rows that now lead to those objects.
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

/// One stream's region in one source object.
struct Source {
    object: ObjectId,
    extent: StreamExtent,
    /// How far a former generation's object is accepted (D17).
    limit: Option<u64>,
}

/// One output's worth of work: the objects of one hour, after the cursor.
#[derive(Default)]
struct Batch {
    /// Sorted by stream, so the output is (section 7.5), each stream's sources in list order.
    streams: BTreeMap<KeyHash, Vec<Source>>,
    /// Each source object, with the longer classes it holds streams of (section 7.4).
    objects: BTreeMap<ObjectId, u8>,
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
    Ready(Batch),
}

/// Runs on its own thread, and reaches everything through messages and the store.
pub(crate) struct Compactor {
    pub config: CompactionConfig,
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
    /// The node's object table, which this adds rows to and removes them from.
    pub objects: Arc<RwLock<ObjectTable>>,
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

fn write_table(table: &RwLock<ObjectTable>) -> std::sync::RwLockWriteGuard<'_, ObjectTable> {
    table
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The hash ranges a source object's rows say an output replaces: its streams of the class in
/// hash order, broken at any it holds that the output doesn't.
fn hash_ranges(copied: &[u64], left_out: &[u64]) -> Vec<(u64, u64)> {
    let mut ranges: Vec<(u64, u64)> = Vec::new();
    let mut open: Option<(u64, u64)> = None;
    let mut left = left_out.iter().peekable();
    for hash in copied {
        let broken = open.is_some_and(|(_, last)| {
            while left.peek().is_some_and(|out| **out <= last) {
                left.next();
            }
            left.peek().is_some_and(|out| **out < *hash)
        });
        match &mut open {
            Some(range) if !broken => range.1 = *hash,
            _ => {
                ranges.extend(open.take());
                open = Some((*hash, *hash));
            }
        }
    }
    ranges.extend(open);
    ranges
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
                Planned::Ready(mut batch) => {
                    let through = batch.through.expect("a ready batch covers a manifest");
                    report.errors.append(&mut batch.skipped);
                    match self.run_batch(class, batch, forced, &mut report).await {
                        Ok(()) => cursor = through,
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

    /// A stream's retention as its owner has it, or `None` if there is no such stream.
    async fn retention_of(&self, stream: KeyHash) -> Result<Option<u64>, String> {
        let (reply, answer) = oneshot::channel();
        self.first_hop(stream)
            .send(ControlMsg::RetentionOf { stream, reply })
            .map_err(|_| "the node has stopped".to_owned())?;
        answer.await.map_err(|_| "the node has stopped".to_owned())
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
        for (id, path) in log.by_id.range(cursor.saturating_add(1)..) {
            let (Some(hour), Some(object)) = (object_hour(path), ObjectId::of_path(path)) else {
                batch.through = Some(*id);
                continue;
            };
            if !batch.objects.is_empty() && hour != batch.hour {
                closed = true;
                break;
            }
            // A startup publishes again the manifest of an object no checkpoint recorded, so
            // the log can name one twice.
            if batch.objects.contains_key(&object) || self.copied_for(object, class) {
                batch.through = Some(*id);
                continue;
            }
            let footer = match self.read_footer(path).await? {
                FooterRead::Found(footer) => footer,
                // Expired: whatever it held went with it, or lives on in a compacted object.
                FooterRead::Missing => {
                    batch.through = Some(*id);
                    continue;
                }
                // Nothing else waits for it. The delete job can't pass it either, since its row
                // never says what it holds, so the object and its hour stay (D177).
                FooterRead::Corrupt(error) => {
                    batch.skipped.push(error);
                    batch.through = Some(*id);
                    continue;
                }
            };
            if batch.objects.is_empty() {
                batch.first_manifest_id = *id;
                batch.hour = hour;
            }
            self.look(&mut batch, object, path, footer, class).await?;
            batch.through = Some(*id);
            if batch.bytes >= self.config.target_output_bytes {
                closed = true;
                break;
            }
        }
        if batch.through.is_none() {
            return Ok(Planned::Nothing);
        }
        if !closed && !forced && !batch.streams.is_empty() {
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

    /// Adds an object to a batch: the longer classes its streams have now, and the regions of
    /// `class` to copy.
    async fn look(
        &self,
        batch: &mut Batch,
        object: ObjectId,
        path: &Path,
        footer: Vec<StreamExtent>,
        class: RetentionClass,
    ) -> Result<(), String> {
        let limit = object_generation(path).and_then(|at| self.ownership.limit_for(at));
        let mut held = 0;
        for extent in footer {
            // The manifest stream is never compacted (Q4), and what readers ignore of a former
            // generation isn't copied.
            if extent.stream == self.keys.manifest
                || limit.is_some_and(|limit| extent.id_first > limit)
            {
                continue;
            }
            let Some(retention_ms) = self.retention_of(extent.stream).await? else {
                continue;
            };
            let Some(of) = RetentionClass::of_retention(retention_ms) else {
                continue;
            };
            held |= of.bit();
            if of == class {
                batch.bytes += extent.len_bytes;
                batch
                    .streams
                    .entry(extent.stream)
                    .or_default()
                    .push(Source {
                        object,
                        extent,
                        limit,
                    });
            }
        }
        batch.objects.insert(object, held);
        Ok(())
    }

    /// At expiry every worker has passed the object, so a class its row holds with no copy was
    /// raised after its worker went by. The delete job copies it then, and the object goes at a
    /// later run, once a checkpoint holds the rows (section 7.4). Whether it copied.
    async fn copy_missing_classes(
        &self,
        path: &Path,
        extents: &[StreamExtent],
    ) -> Result<bool, String> {
        let Some(object) = ObjectId::of_path(path) else {
            return Ok(false);
        };
        let mut copied = false;
        for class in RetentionClass::ALL {
            let mut batch = Batch {
                hour: object.hour(),
                ..Batch::default()
            };
            self.look(&mut batch, object, path, extents.to_vec(), class)
                .await?;
            let fresh = batch.objects.get(&object).copied().unwrap_or(0);
            let missing = self.row_of(path).is_some_and(|row| {
                (row.held.unwrap_or(0) | fresh) & class.bit() != 0
                    && !row.replacements.iter().any(|known| known.class == class)
            });
            if !missing {
                continue;
            }
            let mut report = CompactReport::default();
            self.run_batch(class, batch, true, &mut report).await?;
            if !report.errors.is_empty() {
                return Err(report.errors.join("; "));
            }
            copied = true;
        }
        Ok(copied)
    }

    /// Whether a row already leads this class from the object to a copy, which a cursor a crash
    /// lost leaves behind: the batch isn't done again (section 7.5).
    fn copied_for(&self, object: ObjectId, class: RetentionClass) -> bool {
        let table = self
            .objects
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        table
            .get(ObjectKey::sealed(object))
            .is_some_and(|row| row.replacements.iter().any(|known| known.class == class))
    }

    /// One source's records, as stored. `Ok(Err)` is a source that can't be read and is left
    /// behind (D177); `Err` stops the batch, for the next run to try again.
    async fn read_source(&self, source: &Source) -> Result<Result<Bytes, String>, String> {
        let path = ObjectKey::sealed(source.object).path();
        let reader = SegmentReader::new(Arc::clone(&self.store), path.clone());
        let bytes = match reader.read_extent(&source.extent).await {
            Ok(bytes) => bytes,
            // Gone or bad: either way no row will say it was copied.
            Err(error) if is_corrupt(&error) || is_not_found(&error) => {
                return Ok(Err(format!("{path}: {error}")));
            }
            Err(error) => return Err(format!("{path}: {error}")),
        };
        let Some(limit) = source.limit else {
            return Ok(Ok(bytes));
        };
        // A region of a former generation stops at its last accepted record (D17, D138).
        Ok(Ok(accepted_prefix(
            &bytes,
            limit.min(source.extent.id_last),
        )
        .map_or_else(Bytes::new, |(_, accepted)| accepted)))
    }

    /// One output for the batch, with one region per stream, then the rows that lead readers
    /// to it, then the cursor, once a checkpoint holds the rows.
    async fn run_batch(
        &self,
        class: RetentionClass,
        batch: Batch,
        forced: bool,
        report: &mut CompactReport,
    ) -> Result<(), String> {
        let mut builder = SegmentBuilder::new();
        let mut any = false;
        // Per source object, the hashes it holds of this class that were copied and those that
        // weren't.
        let mut copied: BTreeMap<ObjectId, (Vec<u64>, Vec<u64>)> = BTreeMap::new();
        for (stream, mut sources) in batch.streams {
            sources.sort_by_key(|source| source.object);
            let hash = stream_hash(stream);
            let mut pieces = Vec::with_capacity(sources.len());
            let mut corrupt = false;
            for source in &sources {
                match self.read_source(source).await? {
                    Ok(piece) => pieces.push(piece),
                    Err(error) => {
                        report.errors.push(error);
                        corrupt = true;
                    }
                }
            }
            // Left where it is, and its sources' rows say so by leaving it out. The delete job
            // keeps their hour.
            if corrupt {
                for source in &sources {
                    copied.entry(source.object).or_default().1.push(hash);
                }
                continue;
            }
            for source in &sources {
                copied.entry(source.object).or_default().0.push(hash);
            }
            pieces.retain(|piece| !piece.is_empty());
            if !pieces.is_empty() {
                builder
                    .add_extent(stream, pieces)
                    .map_err(|error| format!("building for {stream}: {error}"))?;
                any = true;
            }
        }
        let output = if any {
            let built = builder.finish().map_err(|error| error.to_string())?;
            let path = put_compacted(
                self.store.as_ref(),
                class.prefix(),
                batch.hour,
                self.generation,
                &built.parts,
            )
            .await?;
            storage::crash_point("compact.after_output");
            let id =
                ObjectId::of_path(&path).ok_or_else(|| format!("{path}: no id in the path"))?;
            report.outputs.push(path);
            Some(id)
        } else {
            None
        };
        {
            let mut table = write_table(&self.objects);
            if let Some(by) = output {
                table.update(
                    ObjectKey::compacted(class, by),
                    ObjectState::Remote,
                    |row| {
                        row.held = Some(0);
                    },
                );
            }
            for (object, held) in &batch.objects {
                let (hashes, left_out) = copied.remove(object).unwrap_or_default();
                // A stream with nothing accepted still counts as copied: the output has all of
                // it there is.
                let ranges = match output {
                    Some(_) => hash_ranges(&hashes, &left_out),
                    None => Vec::new(),
                };
                let mut replaced = 0;
                table.update(ObjectKey::sealed(*object), ObjectState::Remote, |row| {
                    // A class raised since an earlier look is held too, so the object stays
                    // until it has a copy.
                    row.held = Some(row.held.unwrap_or(0) | *held);
                    if let Some(by) = output {
                        row.replacements.retain(|known| known.class != class);
                        for (hash_first, hash_last) in &ranges {
                            row.replacements.push(Replacement {
                                class,
                                hash_first: *hash_first,
                                hash_last: *hash_last,
                                by,
                            });
                            replaced += 1;
                        }
                    }
                    // Nothing of this class to copy, or nothing it held was accepted: the class
                    // is passed with no copy needed.
                    if (output.is_none() || hashes.is_empty())
                        && left_out.is_empty()
                        && let Some(held) = &mut row.held
                    {
                        *held &= !class.bit();
                    }
                });
                if replaced > 0 {
                    report.replaced_count += 1;
                }
            }
        }
        storage::crash_point("compact.after_rows");
        self.wait_for_checkpoint(forced).await?;
        storage::crash_point("compact.before_cursor");
        if let Some(through) = batch.through {
            self.commit_cursor(class, through).await?;
        }
        Ok(())
    }

    /// Whether an expired `sealed/` object may go (section 7.4): every worker has passed its
    /// manifest, and its row says each longer class it holds has a compacted copy. An object of
    /// the manifest stream alone holds no class, and goes once the manifests in it are read.
    /// Everything here errs towards keeping it.
    fn sealed_object_passed(
        &self,
        path: &Path,
        extents: &[StreamExtent],
        manifest_ids: &HashMap<&Path, u64>,
        log_complete: bool,
        slowest_cursor: u64,
    ) -> bool {
        if self.manifests_only(extents) {
            return !self.holds_unread_manifests(extents, slowest_cursor);
        }
        self.manifest_passed(path, extents, manifest_ids, log_complete, slowest_cursor)
            && self.row_of(path).is_some_and(|row| row.is_passed())
    }

    fn manifests_only(&self, extents: &[StreamExtent]) -> bool {
        extents
            .iter()
            .all(|extent| extent.stream == self.keys.manifest)
    }

    fn holds_unread_manifests(&self, extents: &[StreamExtent], slowest_cursor: u64) -> bool {
        extents
            .iter()
            .any(|extent| extent.stream == self.keys.manifest && extent.id_last > slowest_cursor)
    }

    /// Every worker has passed the object's manifest, and the manifests it holds.
    fn manifest_passed(
        &self,
        path: &Path,
        extents: &[StreamExtent],
        manifest_ids: &HashMap<&Path, u64>,
        log_complete: bool,
        slowest_cursor: u64,
    ) -> bool {
        if self.holds_unread_manifests(extents, slowest_cursor) {
            return false;
        }
        match manifest_ids.get(path) {
            Some(manifest) => *manifest <= slowest_cursor,
            // A recorded object's manifest was saved in an object (D180). With all of the stream
            // read and no sign of it, that object has gone, which takes every worker passing it
            // (D182).
            None => log_complete && self.row_of(path).is_some(),
        }
    }

    fn row_of(&self, path: &Path) -> Option<storage::ObjectRow> {
        let id = ObjectId::of_path(path)?;
        let table = self
            .objects
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        table.get(ObjectKey::sealed(id)).cloned()
    }

    /// The delete job (section 7.4). Only whole hours under a prefix, once expired, and for
    /// `sealed/` only once compaction has passed all of the hour. It messages no stream: rows
    /// lead readers to the compacted copies, which a checkpoint held before the cursors moved.
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
        let mut going: Vec<(Option<RetentionClass>, String, Vec<Path>)> = Vec::new();
        let mut prefixes = vec![(None, SEALED_PREFIX, self.config.sealed_expiry_ms)];
        for class in RetentionClass::ALL {
            prefixes.push((
                Some(class),
                class.prefix(),
                class.length_ms() + CLASS_GRACE_MS,
            ));
        }
        let manifest_ids: HashMap<&Path, u64> = log
            .by_id
            .iter()
            .rev()
            .map(|(id, path)| (path, *id))
            .collect();
        for (class, root, expiry_ms) in prefixes {
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
                    .check_hour(root, hour, &manifest_ids, log_complete, slowest_cursor)
                    .await
                {
                    Ok(Some(paths)) => going.push((class, prefix, paths)),
                    Ok(None) => report.blocked_prefixes.push(prefix),
                    Err(error) => {
                        report.errors.push(format!("{prefix}: {error}"));
                        report.blocked_prefixes.push(prefix);
                    }
                }
            }
        }
        drop(manifest_ids);
        storage::crash_point("expire.before_delete");
        for (class, prefix, paths) in going {
            let mut all_gone = true;
            for path in &paths {
                match self.store.delete(path).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                        report.deleted_object_count += 1;
                        if let Some(id) = ObjectId::of_path(path) {
                            self.forget(class, id);
                        }
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

    /// The rows a deleted object leaves. A sealed object's row stays while it leads to a copy,
    /// since a list may still name it, and a compacted object takes the replacements naming it
    /// with it.
    fn forget(&self, class: Option<RetentionClass>, id: ObjectId) {
        let mut table = write_table(&self.objects);
        match class {
            None => {
                let keep = table
                    .get(ObjectKey::sealed(id))
                    .is_some_and(|row| !row.replacements.is_empty());
                if !keep {
                    table.remove(ObjectKey::sealed(id));
                }
            }
            Some(class) => {
                table.remove(ObjectKey::compacted(class, id));
                let naming: Vec<ObjectKey> = table
                    .rows()
                    .filter(|row| {
                        row.key.class.is_none()
                            && row
                                .replacements
                                .iter()
                                .any(|known| known.class == class && known.by == id)
                    })
                    .map(|row| row.key)
                    .collect();
                let sealed_gone_ms = self.config.sealed_expiry_ms + HOUR_MS;
                let now_ms = self.clock.now_ms();
                for key in naming {
                    let mut empty = false;
                    table.update(key, ObjectState::Remote, |row| {
                        row.replacements
                            .retain(|known| !(known.class == class && known.by == id));
                        empty = row.replacements.is_empty();
                    });
                    // Its own hour is long past, so the sealed object went before this did.
                    if empty && key.id.time_ms().saturating_add(sealed_gone_ms) <= now_ms {
                        table.remove(key);
                    }
                }
            }
        }
    }

    /// Everything in one expired hour, if it can all go.
    async fn check_hour(
        &self,
        root: &str,
        hour: u64,
        manifest_ids: &HashMap<&Path, u64>,
        log_complete: bool,
        slowest_cursor: u64,
    ) -> Result<Option<Vec<Path>>, String> {
        let objects = list_hour_objects(self.store.as_ref(), root, hour)
            .await
            .map_err(|error| error.to_string())?;
        let mut paths = Vec::with_capacity(objects.len());
        for object in objects {
            let path = object.location;
            if root == SEALED_PREFIX {
                let extents = match self.read_footer(&path).await? {
                    FooterRead::Found(extents) => extents,
                    FooterRead::Missing => Vec::new(),
                    // A sealed object nobody could read may hold what was never copied.
                    FooterRead::Corrupt(error) => return Err(error),
                };
                if !self.manifests_only(&extents)
                    && self.manifest_passed(
                        &path,
                        &extents,
                        manifest_ids,
                        log_complete,
                        slowest_cursor,
                    )
                    && self.copy_missing_classes(&path, &extents).await?
                {
                    return Ok(None);
                }
                if !self.sealed_object_passed(
                    &path,
                    &extents,
                    manifest_ids,
                    log_complete,
                    slowest_cursor,
                ) {
                    return Ok(None);
                }
            }
            paths.push(path);
        }
        Ok(Some(paths))
    }
}

#[cfg(test)]
mod tests {
    use super::hash_ranges;

    #[test]
    fn ranges_break_only_at_streams_left_out() {
        assert_eq!(hash_ranges(&[1, 3, 9], &[]), vec![(1, 9)]);
        assert_eq!(hash_ranges(&[1, 3, 9], &[5]), vec![(1, 3), (9, 9)]);
        assert_eq!(hash_ranges(&[1, 3, 9], &[0, 10]), vec![(1, 9)]);
        assert_eq!(hash_ranges(&[], &[4]), Vec::<(u64, u64)>::new());
        assert_eq!(
            hash_ranges(&[2, 4, 6, 8], &[3, 7]),
            vec![(2, 2), (4, 6), (8, 8)]
        );
    }
}
