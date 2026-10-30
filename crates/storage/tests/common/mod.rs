//! Shared by the storage tests. Not every test file uses every helper.
#![allow(dead_code)]

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use format::{RecordHeader, encode_record};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult,
};

/// One stamped record's bytes.
pub fn record(id: u64, options: u64, author: u64, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let header = RecordHeader {
        id,
        options,
        author,
        len: data.len() as u32,
        count: 1,
        codec: 0,
    };
    encode_record(&mut out, &header, data).unwrap();
    out
}

/// Counts every read and PUT, and can fail PUTs. All reads go through `get_opts`,
/// since the trait's other read methods default to it.
#[derive(Debug)]
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    pub get_count: AtomicU64,
    pub put_count: AtomicU64,
    pub put_attempt_count: AtomicU64,
    pub fail_next_puts: AtomicU64,
    /// Refuses PUTs with an error that says retrying can't help.
    pub reject_next_puts: AtomicU64,
    /// PUTs never finish, as a store that accepted the connection and went silent.
    pub hang_puts: AtomicBool,
    /// The next PUT blocks a thread on this until its sender is dropped, as a stalled
    /// mount does inside `LocalFileSystem`.
    pub block_next_put_on: Mutex<Option<Receiver<()>>>,
}

impl CountingStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> CountingStore {
        CountingStore {
            inner,
            get_count: AtomicU64::new(0),
            put_count: AtomicU64::new(0),
            put_attempt_count: AtomicU64::new(0),
            fail_next_puts: AtomicU64::new(0),
            reject_next_puts: AtomicU64::new(0),
            hang_puts: AtomicBool::new(false),
            block_next_put_on: Mutex::new(None),
        }
    }

    pub fn gets(&self) -> u64 {
        self.get_count.load(Ordering::SeqCst)
    }

    pub fn puts(&self) -> u64 {
        self.put_count.load(Ordering::SeqCst)
    }

    pub fn reset_gets(&self) {
        self.get_count.store(0, Ordering::SeqCst);
    }
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.put_attempt_count.fetch_add(1, Ordering::SeqCst);
        if self.hang_puts.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        let gate = self.block_next_put_on.lock().unwrap().take();
        if let Some(gate) = gate {
            tokio::task::spawn_blocking(move || gate.recv())
                .await
                .unwrap()
                .ok();
        }
        let reject = self
            .reject_next_puts
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        if reject {
            return Err(object_store::Error::NotImplemented);
        }
        let fail = self
            .fail_next_puts
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        if fail {
            return Err(object_store::Error::Generic {
                store: "CountingStore",
                source: "injected failure".into(),
            });
        }
        self.put_count.fetch_add(1, Ordering::SeqCst);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        panic!("the uploader makes one plain PUT per object");
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.get_count.fetch_add(1, Ordering::SeqCst);
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'_, object_store::Result<ObjectMeta>> {
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

/// Writes a built segment as one PUT.
pub async fn put_parts(store: &dyn ObjectStore, path: &Path, parts: &[Bytes]) {
    let payload = PutPayload::from_iter(parts.iter().cloned());
    store.put(path, payload).await.unwrap();
}

pub fn concat(parts: &[Bytes]) -> Vec<u8> {
    parts.iter().flat_map(|part| part.iter().copied()).collect()
}
