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

fn temp_files(root: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(root)
        .unwrap()
        .map(|item| item.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.starts_with("tmp-"))
        .collect()
}

#[test]
fn a_failed_put_leaves_no_temp_file_and_the_cache_keeps_working() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = SegmentCache::open(dir.path(), 1 << 20).unwrap();
    // A directory at the target makes the final rename fail.
    let target = path("a");
    std::fs::create_dir_all(dir.path().join(target.as_ref()).join("in-the-way")).unwrap();
    for _ in 0..5 {
        assert!(cache.put(&target, &parts(400 * 1024)).is_err());
    }
    assert_eq!(temp_files(dir.path()), Vec::<String>::new());
    assert_eq!(cache.used_bytes(), 0);
    assert!(cache.put(&path("b"), &parts(100)).unwrap());
    assert_eq!(cache.used_bytes(), 100);
    assert_eq!(temp_files(dir.path()), Vec::<String>::new());
}

fn count_dirs(root: &std::path::Path) -> usize {
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
fn eviction_removes_the_directories_it_empties() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = SegmentCache::open(dir.path(), 100).unwrap();
    for hour in 0..50 {
        let object = Path::from(format!("sealed/{hour}/stream-{hour}/a.seg"));
        assert!(cache.put(&object, &parts(100)).unwrap());
    }
    assert_eq!(cache.used_bytes(), 100);
    // `sealed`, one hour and one stream, for the one file that is left.
    assert_eq!(count_dirs(dir.path()), 3);
}

#[test]
fn eviction_keeps_a_directory_that_still_has_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = SegmentCache::open(dir.path(), 200).unwrap();
    let first = Path::from("sealed/1/node/a.seg");
    let second = Path::from("sealed/1/node/b.seg");
    let third = Path::from("sealed/1/node/c.seg");
    cache.put(&first, &parts(100)).unwrap();
    cache.put(&second, &parts(100)).unwrap();
    cache.put(&third, &parts(100)).unwrap();
    assert_eq!(count_dirs(dir.path()), 3);
    assert!(dir.path().join(second.as_ref()).exists());
    assert!(dir.path().join(third.as_ref()).exists());
}

#[test]
fn reopening_removes_empty_directories_and_never_the_root() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("sealed/1/gone")).unwrap();
    std::fs::create_dir_all(dir.path().join("sealed/2/kept")).unwrap();
    std::fs::write(dir.path().join("sealed/2/kept/a.seg"), b"data").unwrap();
    let cache = SegmentCache::open(dir.path(), 1000).unwrap();
    assert_eq!(cache.used_bytes(), 4);
    assert_eq!(count_dirs(dir.path()), 3);
    drop(cache);

    let empty = tempfile::tempdir().unwrap();
    SegmentCache::open(empty.path(), 1000).unwrap();
    assert!(empty.path().is_dir());
}
