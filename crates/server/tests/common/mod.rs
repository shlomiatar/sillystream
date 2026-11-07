//! A node with a scripted WAL and store: `sync_data` and `PUT` can be held, and what
//! was written can be read back.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, IoSlice};
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ciborium::value::Value;
use format::{RecordHeader, Records, encode_record};
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult,
};
use server::{ManualClock, Node, NodeConfig, Publish, Reply};
use storage::{ReplayedEntry, WalFile, WalVolume, encode_file_header, replay_bytes};

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

pub const START_MS: u64 = 1_800_000_000_000;
pub const AUTHOR: NonZeroU64 = NonZeroU64::new(77).unwrap();

/// Long enough that an ack that was going to arrive would have.
pub const QUIET_MS: u64 = 60;
pub const WAIT_MS: u64 = 10_000;

#[derive(Clone, Default)]
pub struct WalScript {
    pub hold_sync: Arc<AtomicBool>,
    pub fail_sync: Arc<AtomicBool>,
    pub sync_count: Arc<AtomicU64>,
    files: Arc<Mutex<BTreeMap<u64, Vec<u8>>>>,
}

impl WalScript {
    pub fn hold(&self) {
        self.hold_sync.store(true, Ordering::SeqCst);
    }

    pub fn release(&self) {
        self.hold_sync.store(false, Ordering::SeqCst);
    }

    /// Every publish written, in file order, whether synced or not. The dedupe entries a file
    /// carries from the one before aren't publishes (section 3).
    pub fn entries(&self) -> Vec<ReplayedEntry> {
        let files = self.files.lock().unwrap();
        files
            .iter()
            .flat_map(|(seq, bytes)| replay_bytes(&Bytes::copy_from_slice(bytes), *seq).entries)
            .filter(|entry| {
                format::RecordHeader::read(&entry.records).map_or(true, |header| {
                    !header.is_system() || header.options != format::ENTRY_DEDUPE_CARRY
                })
            })
            .collect()
    }

    pub fn stream_entries(&self, stream: &str) -> Vec<ReplayedEntry> {
        self.entries()
            .into_iter()
            .filter(|entry| entry.stream == key(stream))
            .collect()
    }
}

pub struct ScriptFile {
    script: WalScript,
    seq: u64,
}

impl WalFile for ScriptFile {
    fn write_vectored(&mut self, slices: &[IoSlice<'_>]) -> io::Result<usize> {
        let mut files = self.script.files.lock().unwrap();
        let file = files.entry(self.seq).or_default();
        let mut written = 0;
        for slice in slices {
            file.extend_from_slice(slice);
            written += slice.len();
        }
        Ok(written)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        while self.script.hold_sync.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(1));
        }
        if self.script.fail_sync.load(Ordering::SeqCst) {
            return Err(io::Error::other("scripted sync failure"));
        }
        self.script.sync_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl WalVolume for WalScript {
    type File = ScriptFile;

    fn create(&mut self, seq: u64) -> io::Result<ScriptFile> {
        self.files
            .lock()
            .unwrap()
            .insert(seq, encode_file_header(seq, 0).to_vec());
        Ok(ScriptFile {
            script: self.clone(),
            seq,
        })
    }

    fn remove(&mut self, _seq: u64) -> io::Result<()> {
        Ok(())
    }
}

/// A store whose `PUT`s wait while held.
#[derive(Debug)]
pub struct ScriptStore {
    inner: InMemory,
    pub hold_put: Arc<AtomicBool>,
    pub put_count: Arc<AtomicU64>,
    /// PUTs that have begun, whether or not held.
    pub put_begun_count: Arc<AtomicU64>,
    /// How many more PUTs under `checkpoints/` may succeed. Negative: no limit.
    pub checkpoint_put_budget: Arc<AtomicI64>,
    /// So many `GET`s fail with a store error that is worth retrying, whatever their path.
    pub failing_get_count: Arc<AtomicI64>,
    /// The bytes every ranged `GET` asked for.
    pub range_get_bytes: Arc<AtomicU64>,
    /// Every delete, in order.
    pub deleted: Arc<Mutex<Vec<Path>>>,
    /// `GET`s of a path ending in this wait this long, once each.
    pub slow_get_once: Arc<Mutex<Vec<(String, Duration)>>>,
    /// `PUT`s of a path ending in this wait this long, every time.
    pub slow_put: Arc<Mutex<Vec<(String, Duration)>>>,
}

impl fmt::Display for ScriptStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ScriptStore")
    }
}

impl ScriptStore {
    pub fn new() -> ScriptStore {
        ScriptStore {
            inner: InMemory::new(),
            hold_put: Arc::new(AtomicBool::new(false)),
            put_count: Arc::new(AtomicU64::new(0)),
            put_begun_count: Arc::new(AtomicU64::new(0)),
            checkpoint_put_budget: Arc::new(AtomicI64::new(-1)),
            failing_get_count: Arc::new(AtomicI64::new(0)),
            range_get_bytes: Arc::new(AtomicU64::new(0)),
            deleted: Arc::new(Mutex::new(Vec::new())),
            slow_get_once: Arc::new(Mutex::new(Vec::new())),
            slow_put: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Removes an object without it counting as one of the node's deletes.
    pub async fn inner_delete(&self, location: &Path) {
        self.inner.delete(location).await.unwrap();
    }

    pub fn hold(&self) {
        self.hold_put.store(true, Ordering::SeqCst);
    }

    pub fn release(&self) {
        self.hold_put.store(false, Ordering::SeqCst);
    }
}

#[async_trait]
impl ObjectStore for ScriptStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.put_begun_count.fetch_add(1, Ordering::SeqCst);
        while self.hold_put.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let slow = self
            .slow_put
            .lock()
            .unwrap()
            .iter()
            .find(|(end, _)| location.as_ref().ends_with(end.as_str()))
            .map(|(_, delay)| *delay);
        if let Some(delay) = slow {
            tokio::time::sleep(delay).await;
        }
        if location.as_ref().starts_with("checkpoints/") {
            let budget = &self.checkpoint_put_budget;
            if budget.load(Ordering::SeqCst) == 0 {
                return Err(object_store::Error::Generic {
                    store: "ScriptStore",
                    source: "scripted checkpoint PUT failure".into(),
                });
            }
            let _ = budget.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                (left > 0).then(|| left - 1)
            });
        }
        self.put_count.fetch_add(1, Ordering::SeqCst);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if let Some(object_store::GetRange::Bounded(range)) = &options.range {
            self.range_get_bytes
                .fetch_add((range.end - range.start) as u64, Ordering::SeqCst);
        }
        if self
            .failing_get_count
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                (left > 0).then(|| left - 1)
            })
            .is_ok()
        {
            return Err(object_store::Error::Generic {
                store: "ScriptStore",
                source: "scripted GET failure".into(),
            });
        }
        let slow = {
            let mut armed = self.slow_get_once.lock().unwrap();
            armed
                .iter()
                .position(|(end, _)| location.as_ref().ends_with(end.as_str()))
                .map(|at| armed.remove(at).1)
        };
        if let Some(delay) = slow {
            tokio::time::sleep(delay).await;
        }
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.deleted.lock().unwrap().push(location.clone());
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

pub struct Harness {
    pub node: Option<Node>,
    pub wal: WalScript,
    pub store: Arc<ScriptStore>,
    pub clock: ManualClock,
}

impl Harness {
    pub fn new(config: NodeConfig) -> Harness {
        let wal = WalScript::default();
        let store = Arc::new(ScriptStore::new());
        let clock = ManualClock::new(START_MS);
        let node =
            Node::start(config, wal.clone(), store.clone(), Arc::new(clock.clone())).unwrap();
        Harness {
            node: Some(node),
            wal,
            store,
            clock,
        }
    }

    pub fn node(&self) -> &Node {
        self.node.as_ref().unwrap()
    }

    /// Releases anything held, so the threads can finish.
    pub fn finish(mut self) -> server::NodeExit {
        self.wal.release();
        self.store.release();
        self.node.take().unwrap().shutdown()
    }
}

impl Drop for Harness {
    /// A failed test mustn't leave threads spinning on a hold.
    fn drop(&mut self) {
        self.wal.release();
        self.store.release();
    }
}

/// No timers: seals and rebalancing happen only when a test asks.
pub fn manual_config(core_count: usize) -> NodeConfig {
    let mut config = NodeConfig::new(core_count);
    config.seal_window_ms = None;
    config.rebalance_period_ms = None;
    config.upload_retry_initial_ms = 1;
    config.upload_retry_max_ms = 4;
    config.slab_bytes = 64 * 1024;
    config
}

pub fn record_with(options: u64, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    add_record(&mut out, options, data, 1);
    out
}

pub fn add_record(out: &mut Vec<u8>, options: u64, data: &[u8], count: u16) {
    let header = RecordHeader {
        id: 0,
        options,
        author: 0,
        len: data.len() as u32,
        count,
        codec: 0,
    };
    encode_record(out, &header, data).unwrap();
}

pub fn publish<'a>(stream: &'a str, records: &'a [u8], producer_seq: u64, ack: u8) -> Publish<'a> {
    Publish {
        stream: key(stream),
        handle: 9,
        producer_id: 1,
        producer_seq,
        ack_level: Some(ack),
        author: AUTHOR,
        state: None,
        records,
    }
}

/// Fails the test if nothing arrives.
pub async fn expect_reply(client: &mut server::Client) -> Reply {
    client
        .recv_within(WAIT_MS)
        .await
        .expect("a reply should have arrived")
}

pub async fn expect_ack(client: &mut server::Client) -> (u64, u64, u32) {
    match expect_reply(client).await {
        Reply::Ack {
            producer_seq,
            first_id,
            count,
            ..
        } => (producer_seq, first_id, count),
        other => panic!("expected an ack, got {other:?}"),
    }
}

pub async fn expect_quiet(client: &mut server::Client) {
    if let Some(reply) = client.recv_within(QUIET_MS).await {
        panic!("expected nothing, got {reply:?}");
    }
}

/// `(id, last_id, options, data)` of each record.
pub fn parse_records(bytes: &[u8]) -> Vec<(u64, u64, u64, Vec<u8>)> {
    Records::new(bytes)
        .map(|view| {
            let view = view.unwrap();
            (
                view.header.id,
                view.header.last_id(),
                view.header.options,
                view.data.to_vec(),
            )
        })
        .collect()
}

pub fn cbor(value: &Value) -> Bytes {
    let mut out = Vec::new();
    ciborium::ser::into_writer(value, &mut out).unwrap();
    Bytes::from(out)
}

pub fn from_cbor(bytes: &[u8]) -> Value {
    ciborium::de::from_reader(bytes).unwrap()
}

pub fn map(pairs: &[(&str, Value)]) -> Value {
    Value::Map(
        pairs
            .iter()
            .map(|(key, value)| (Value::Text((*key).to_owned()), value.clone()))
            .collect(),
    )
}

pub fn int(value: i64) -> Value {
    Value::Integer(value.into())
}

/// What one connection heard, in order.
#[derive(Default)]
pub struct Heard {
    pub events: Vec<server::Event>,
}

impl Heard {
    /// `(id, last_id, options, data)` of what `sub` received.
    pub fn records(&self, sub: u32) -> Vec<(u64, u64, u64, Vec<u8>)> {
        self.events
            .iter()
            .filter_map(|event| match event {
                server::Event::Data { sub: to, records } if *to == sub => {
                    Some(parse_records(&records.to_vec()))
                }
                _ => None,
            })
            .flatten()
            .collect()
    }

    pub fn progress(&self, sub: u32) -> Vec<u64> {
        self.events
            .iter()
            .filter_map(|event| match event {
                server::Event::Progress { sub: to, id } if *to == sub => Some(*id),
                _ => None,
            })
            .collect()
    }

    pub fn error_code(&self, sub: u32) -> Option<proto::ErrorCode> {
        self.events.iter().find_map(|event| match event {
            server::Event::Error {
                reference, code, ..
            } if *reference == sub => Some(*code),
            _ => None,
        })
    }
}

/// Listens until `done` says so. Fails the test if `WAIT_MS` passes first.
pub async fn hear_until(
    connection: &mut server::Connection,
    heard: &mut Heard,
    mut done: impl FnMut(&Heard) -> bool,
) {
    let give_up = std::time::Instant::now() + Duration::from_millis(WAIT_MS);
    while !done(heard) {
        assert!(
            std::time::Instant::now() < give_up,
            "gave up after hearing {} events: {:?}",
            heard.events.len(),
            heard
                .events
                .iter()
                .map(|e| format!("{e:?}").chars().take(300).collect::<String>())
                .collect::<Vec<_>>()
        );
        if let Some(event) = connection.recv_within(100).await {
            heard.events.push(event);
        }
    }
}

/// Listens for `quiet_ms`, keeping what comes.
pub async fn hear_for(connection: &mut server::Connection, heard: &mut Heard, quiet_ms: u64) {
    let until = std::time::Instant::now() + Duration::from_millis(quiet_ms);
    while std::time::Instant::now() < until {
        if let Some(event) = connection.recv_within(5).await {
            heard.events.push(event);
        }
    }
}

/// Waits until the subscription has answered.
pub async fn subscribed(connection: &mut server::Connection, heard: &mut Heard, sub: u32) -> u64 {
    hear_until(connection, heard, |heard| {
        heard
            .events
            .iter()
            .any(|event| matches!(event, server::Event::Subscribed { sub: to, .. } if *to == sub))
    })
    .await;
    heard
        .events
        .iter()
        .find_map(|event| match event {
            server::Event::Subscribed { sub: to, head, .. } if *to == sub => Some(*head),
            _ => None,
        })
        .unwrap()
}

/// Sums a stat over the node's cores.
pub async fn total_stat(node: &Node, stat: impl Fn(&server::CoreStats) -> u64) -> u64 {
    let mut total = 0;
    for core in 0..node.core_count() {
        total += stat(&node.stats(core).await.unwrap());
    }
    total
}

/// Makes a stream by publishing nothing into it yet.
pub async fn make_stream(client: &mut server::Client, stream: &str) {
    client
        .configure(key(stream), server::StreamConfig::default())
        .await;
}

/// Waits until no core holds a stream's batches, which means reads go to the extents.
/// A seal that holds what was just published at level 3. The publish only reaches the WAL once
/// its core has read it, and a seal holds what its WAL file holds, so a seal that came first is
/// empty, and another follows.
#[allow(dead_code)]
pub async fn seal_published(node: &Node) -> server::SealOutcome {
    for _ in 0..1_000 {
        match node.seal().await {
            server::SealOutcome::Empty { .. } => tokio::time::sleep(Duration::from_millis(5)).await,
            outcome => return outcome,
        }
    }
    panic!("nothing published reached a seal");
}

pub async fn wait_for_memory_to_empty(node: &Node, stream: &str) {
    wait_for_memory(node, stream, true).await;
}

/// A publish not yet acked may still be on its way to its core: waits until memory holds it.
pub async fn wait_for_memory_to_hold(node: &Node, stream: &str) {
    wait_for_memory(node, stream, false).await;
}

async fn wait_for_memory(node: &Node, stream: &str, empty: bool) {
    let give_up = std::time::Instant::now() + Duration::from_millis(WAIT_MS);
    loop {
        let mut held = 0;
        for core in 0..node.core_count() {
            held += node.read_memory(core, key(stream)).await.len();
        }
        if (held == 0) == empty {
            return;
        }
        assert!(
            std::time::Instant::now() < give_up,
            "{held} batches in memory"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Drops the cursors and sinks a stopped node saved as they changed, leaving what its last seal
/// saved: what a crash before the saver wrote, or a lost metadata disk, leaves (section 5.1).
/// The keys are `k | stream | name` and `n | stream | sink_id`.
pub fn forget_saved_between_seals(metadata_dir: &std::path::Path) {
    let store = storage::MetadataStore::open(metadata_dir).unwrap();
    let mut saved = store.scan_prefix(b"k").unwrap();
    saved.extend(store.scan_prefix(b"n").unwrap());
    store
        .apply(saved.iter().map(|(key, _)| (key.as_slice(), None)))
        .unwrap();
}
