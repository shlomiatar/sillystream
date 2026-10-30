//! The local segment cache and its eviction.

use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;
use storage::{SegmentBuilder, SegmentCache, SegmentReader};

fn path(name: &str) -> Path {
    Path::from(format!("sealed/1/node/{name}.seg"))
}

fn parts(len: usize) -> Vec<Bytes> {
    vec![
        Bytes::from(vec![1u8; len / 2]),
        Bytes::from(vec![2u8; len - len / 2]),
    ]
}

async fn is_cached(cache: &SegmentCache, path: &Path) -> bool {
    cache.store().head(path).await.is_ok()
}

#[tokio::test]
async fn the_least_recently_used_goes_first_and_the_cap_holds() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = SegmentCache::open(dir.path(), 300).unwrap();
    assert!(cache.put(&path("a"), &parts(100)).unwrap());
    assert!(cache.put(&path("b"), &parts(100)).unwrap());
    assert!(cache.put(&path("c"), &parts(100)).unwrap());
    assert_eq!(cache.used_bytes(), 300);
    // `a` is older but was just used, so `b` goes.
    assert!(cache.touch(&path("a")).unwrap());
    assert!(cache.put(&path("d"), &parts(100)).unwrap());
    assert_eq!(cache.used_bytes(), 300);
    assert!(is_cached(&cache, &path("a")).await);
    assert!(!is_cached(&cache, &path("b")).await);
    assert!(is_cached(&cache, &path("c")).await);
    assert!(is_cached(&cache, &path("d")).await);
    assert!(!cache.touch(&path("b")).unwrap());
    // A big one pushes out several.
    assert!(cache.put(&path("e"), &parts(250)).unwrap());
    assert!(cache.used_bytes() <= 300);
    assert!(is_cached(&cache, &path("e")).await);
    assert!(!is_cached(&cache, &path("c")).await);
}

#[tokio::test]
async fn an_object_over_the_cap_is_not_kept_and_evicts_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = SegmentCache::open(dir.path(), 100).unwrap();
    assert!(cache.put(&path("a"), &parts(100)).unwrap());
    assert!(!cache.put(&path("big"), &parts(101)).unwrap());
    assert!(is_cached(&cache, &path("a")).await);
    assert!(!is_cached(&cache, &path("big")).await);
}

#[tokio::test]
async fn overwriting_an_object_counts_its_bytes_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = SegmentCache::open(dir.path(), 1000).unwrap();
    cache.put(&path("a"), &parts(100)).unwrap();
    cache.put(&path("a"), &parts(50)).unwrap();
    assert_eq!(cache.used_bytes(), 50);
}

#[tokio::test]
async fn reopening_finds_files_enforces_a_smaller_cap_and_clears_temp_files() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut cache = SegmentCache::open(dir.path(), 1000).unwrap();
        cache.put(&path("a"), &parts(100)).unwrap();
        // Separate modified times, which is all a reopen has to order by.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        cache.put(&path("b"), &parts(100)).unwrap();
    }
    std::fs::write(dir.path().join("tmp-3"), b"interrupted").unwrap();
    let cache = SegmentCache::open(dir.path(), 1000).unwrap();
    assert_eq!(cache.used_bytes(), 200);
    assert!(is_cached(&cache, &path("a")).await);
    assert!(!dir.path().join("tmp-3").exists());
    drop(cache);
    let cache = SegmentCache::open(dir.path(), 150).unwrap();
    assert_eq!(cache.used_bytes(), 100);
    assert!(!is_cached(&cache, &path("a")).await, "oldest out first");
    assert!(is_cached(&cache, &path("b")).await);
}

#[tokio::test]
async fn a_segment_reader_works_on_the_cache_and_sees_eviction_as_not_found() {
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
    builder.add_extent("s", [record.clone()]).unwrap();
    let built = builder.finish().unwrap();
    let first = path("first");
    cache.put(&first, &built.parts).unwrap();
    let store: Arc<dyn ObjectStore> = cache.store();
    let reader = SegmentReader::new(store, first.clone());
    assert_eq!(reader.read_stream("s").await.unwrap(), record);
    cache.put(&path("big"), &parts(4096)).unwrap();
    assert!(reader.read_stream("s").await.is_err());
}
