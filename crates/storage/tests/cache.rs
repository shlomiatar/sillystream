//! The local segment directory: its cap, its eviction, mapped reads and a startup's uploads.

use std::path::Path as FsPath;
use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;
use storage::{SegmentBuilder, SegmentCache, SegmentReader, upload_pending};

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

fn path(name: &str) -> Path {
    Path::from(format!("sealed/1/node/{name}.seg"))
}

fn parts(len: usize) -> Vec<Bytes> {
    vec![
        Bytes::from(vec![1u8; len / 2]),
        Bytes::from(vec![2u8; len - len / 2]),
    ]
}

/// What the uploader does with one segment, with the PUT left out.
fn pend(cache: &mut SegmentCache, object: &Path, parts: &[Bytes]) -> u64 {
    let len_bytes = parts.iter().map(|part| part.len() as u64).sum();
    cache.reserve(len_bytes).unwrap();
    cache.writer().write(object, parts).unwrap();
    len_bytes
}

fn put(cache: &mut SegmentCache, object: &Path, parts: &[Bytes]) {
    let len_bytes = pend(cache, object, parts);
    cache.uploaded(object, len_bytes).unwrap();
}

async fn is_cached(cache: &SegmentCache, path: &Path) -> bool {
    cache.store().head(path).await.is_ok()
}

fn is_pending(root: &FsPath, path: &Path) -> bool {
    root.join("pending").join(path.as_ref()).exists()
}

#[tokio::test]
async fn the_oldest_written_goes_first_whatever_was_read() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = SegmentCache::open(dir.path(), 300).unwrap();
    put(&mut cache, &path("a"), &parts(100));
    put(&mut cache, &path("b"), &parts(100));
    put(&mut cache, &path("c"), &parts(100));
    assert_eq!(cache.used_bytes(), 300);
    cache.store().get(&path("a")).await.unwrap();
    put(&mut cache, &path("d"), &parts(100));
    assert_eq!(cache.used_bytes(), 300);
    assert!(!is_cached(&cache, &path("a")).await);
    assert!(is_cached(&cache, &path("b")).await);
    // A big one pushes out several.
    put(&mut cache, &path("e"), &parts(250));
    assert!(cache.used_bytes() <= 300);
    assert!(is_cached(&cache, &path("e")).await);
    assert!(!is_cached(&cache, &path("c")).await);
}

/// A waiting segment is the only way it reaches the store, so the cap can't take it. It takes
/// `cached/` first, and then seals wait.
#[tokio::test]
async fn pending_segments_are_never_evicted_and_fill_the_cap() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = SegmentCache::open(dir.path(), 200).unwrap();
    put(&mut cache, &path("old"), &parts(100));
    pend(&mut cache, &path("a"), &parts(100));
    assert!(cache.has_room());
    assert!(is_cached(&cache, &path("old")).await);
    pend(&mut cache, &path("b"), &parts(100));
    assert!(!is_cached(&cache, &path("old")).await);
    assert!(!cache.has_room());
    // Over the cap, and still on disk. The cap isn't worth an acked record.
    pend(&mut cache, &path("c"), &parts(100));
    assert_eq!(cache.used_bytes(), 300);
    for name in ["a", "b", "c"] {
        assert!(is_pending(dir.path(), &path(name)));
    }
    cache.uploaded(&path("a"), 100).unwrap();
    assert!(!cache.has_room(), "b and c still fill it");
    cache.uploaded(&path("b"), 100).unwrap();
    assert!(cache.has_room());
}

#[tokio::test]
async fn a_segment_over_the_cap_is_still_written_and_goes_once_uploaded() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = SegmentCache::open(dir.path(), 100).unwrap();
    let big = path("big");
    pend(&mut cache, &big, &parts(101));
    assert!(is_pending(dir.path(), &big));
    cache.uploaded(&big, 101).unwrap();
    assert!(!is_pending(dir.path(), &big));
    assert!(!is_cached(&cache, &big).await);
    assert_eq!(cache.used_bytes(), 0);
}

#[tokio::test]
async fn reopening_keeps_segments_enforces_a_smaller_cap_and_clears_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut cache = SegmentCache::open(dir.path(), 1000).unwrap();
        put(&mut cache, &path("a"), &parts(100));
        // Separate modified times, which is all a reopen has to order by.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        put(&mut cache, &path("b"), &parts(100));
        pend(&mut cache, &path("c"), &parts(100));
    }
    std::fs::write(dir.path().join("tmp-3"), b"interrupted").unwrap();
    std::fs::create_dir_all(dir.path().join("sealed/1/from-an-older-layout")).unwrap();
    let cache = SegmentCache::open(dir.path(), 1000).unwrap();
    assert_eq!(cache.used_bytes(), 300);
    assert!(is_cached(&cache, &path("a")).await);
    assert!(!dir.path().join("tmp-3").exists());
    assert!(!dir.path().join("sealed").exists());
    drop(cache);
    let cache = SegmentCache::open(dir.path(), 250).unwrap();
    assert_eq!(cache.used_bytes(), 200);
    assert!(!is_cached(&cache, &path("a")).await, "oldest out first");
    assert!(is_cached(&cache, &path("b")).await);
    assert!(is_pending(dir.path(), &path("c")));
}

#[tokio::test]
async fn a_segment_reader_works_on_mapped_files_and_sees_eviction_as_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = SegmentCache::open(dir.path(), 4096).unwrap();
    let mut builder = SegmentBuilder::new();
    let mut record = Vec::new();
    let header = format::RecordHeader {
        id: 1 << 16,
        options: 1,
        author: 7,
        len: 4,
        count: 1,
        codec: 0,
    };
    format::encode_record(&mut record, &header, b"data").unwrap();
    let record = Bytes::from(record);
    builder.add_extent(key("s"), [record.clone()]).unwrap();
    let built = builder.finish().unwrap();
    let first = path("first");
    put(&mut cache, &first, &built.parts);
    let store: Arc<dyn ObjectStore> = cache.store();
    let reader = SegmentReader::new(store, first.clone());
    let read = reader.read_stream(key("s")).await.unwrap();
    assert_eq!(read, record);
    put(&mut cache, &path("big"), &parts(4096));
    assert!(matches!(
        reader.read_stream(key("s")).await,
        Err(storage::SegmentError::Store(
            object_store::Error::NotFound { .. }
        ))
    ));
    // A map outlives its file's name, so a reader holding bytes loses nothing to eviction.
    assert_eq!(read, record);
}

fn temp_files(root: &FsPath) -> Vec<String> {
    std::fs::read_dir(root)
        .unwrap()
        .map(|item| item.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.starts_with("tmp-"))
        .collect()
}

#[test]
fn a_failed_write_leaves_no_temp_file_and_a_retry_works() {
    let dir = tempfile::tempdir().unwrap();
    let cache = SegmentCache::open(dir.path(), 1 << 20).unwrap();
    // A directory at the target makes the final rename fail.
    let target = path("a");
    let in_the_way = dir.path().join("pending").join(target.as_ref());
    std::fs::create_dir_all(in_the_way.join("in-the-way")).unwrap();
    for _ in 0..5 {
        assert!(cache.writer().write(&target, &parts(400 * 1024)).is_err());
    }
    assert_eq!(temp_files(dir.path()), Vec::<String>::new());
    std::fs::remove_dir_all(&in_the_way).unwrap();
    let file = cache.writer().write(&target, &parts(100)).unwrap();
    assert_eq!(file.len(), 100);
    assert_eq!(temp_files(dir.path()), Vec::<String>::new());
}

fn count_dirs(root: &FsPath) -> usize {
    let mut count = 0;
    for item in std::fs::read_dir(root).unwrap() {
        let item = item.unwrap();
        if item.metadata().unwrap().is_dir() {
            count += 1 + count_dirs(&item.path());
        }
    }
    count
}

#[test]
fn eviction_and_uploads_remove_the_directories_they_empty() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = SegmentCache::open(dir.path(), 100).unwrap();
    for hour in 0..50 {
        let object = Path::from(format!("sealed/{hour}/stream-{hour}/a.seg"));
        put(&mut cache, &object, &parts(100));
    }
    assert_eq!(cache.used_bytes(), 100);
    // `pending` and `cached`, and `sealed`, one hour and one stream for the file that is left.
    assert_eq!(count_dirs(dir.path()), 5);
}

/// An object whose time is `first_id`'s ms after the epoch.
fn segment_path(generation: u64, first_id: u64) -> Path {
    let time_ms = storage::OBJECT_EPOCH_MS + format::id_ms(first_id);
    storage::sealed_path(storage::ObjectId::new(time_ms, generation, 0))
}

#[tokio::test]
async fn a_startup_uploads_what_is_pending_and_caches_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = SegmentCache::open(dir.path(), 1 << 20).unwrap();
    let (first, second) = (segment_path(1, 1 << 16), segment_path(1, 2 << 16));
    pend(&mut cache, &first, &parts(100));
    pend(&mut cache, &second, &parts(200));
    drop(cache);
    let store = InMemory::new();
    assert_eq!(upload_pending(dir.path(), &store).await.unwrap(), 2);
    assert_eq!(store.get(&first).await.unwrap().meta.size, 100);
    assert_eq!(store.get(&second).await.unwrap().meta.size, 200);
    assert!(!is_pending(dir.path(), &first));
    let cache = SegmentCache::open(dir.path(), 1 << 20).unwrap();
    assert!(is_cached(&cache, &first).await);
    assert_eq!(cache.used_bytes(), 300);
    assert_eq!(upload_pending(dir.path(), &store).await.unwrap(), 0);
}

/// A crash can come after the PUT and before the rename.
#[tokio::test]
async fn a_startup_counts_the_same_bytes_in_the_store_as_done() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = SegmentCache::open(dir.path(), 1 << 20).unwrap();
    let object = segment_path(1, 1 << 16);
    let bytes = parts(100);
    pend(&mut cache, &object, &bytes);
    drop(cache);
    let store = InMemory::new();
    store
        .put(&object, object_store::PutPayload::from_iter(bytes))
        .await
        .unwrap();
    assert_eq!(upload_pending(dir.path(), &store).await.unwrap(), 1);
    assert!(!is_pending(dir.path(), &object));
    assert_eq!(store.get(&object).await.unwrap().meta.size, 100);
}

/// No two objects get one id, so other bytes there are something else's, and nothing is lost by
/// leaving them and the file for someone to look at.
#[tokio::test]
async fn a_startup_refuses_a_name_held_by_other_bytes_and_keeps_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = SegmentCache::open(dir.path(), 1 << 20).unwrap();
    let object = segment_path(1, 1 << 16);
    pend(&mut cache, &object, &parts(100));
    drop(cache);
    let store = InMemory::new();
    store
        .put(&object, Bytes::from_static(b"other").into())
        .await
        .unwrap();
    assert!(upload_pending(dir.path(), &store).await.is_err());
    assert_eq!(store.get(&object).await.unwrap().meta.size, 5);
    assert!(is_pending(dir.path(), &object));
}

/// The file stays, so the next startup tries again.
#[tokio::test]
async fn a_startup_that_cannot_upload_fails_and_keeps_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = SegmentCache::open(dir.path(), 1 << 20).unwrap();
    let object = Path::from("sealed/1/no-object-id-here.seg");
    pend(&mut cache, &object, &parts(100));
    drop(cache);
    assert!(upload_pending(dir.path(), &InMemory::new()).await.is_err());
    assert!(is_pending(dir.path(), &object));
}
