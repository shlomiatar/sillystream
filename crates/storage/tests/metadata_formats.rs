//! The bytes of stream metadata, ownership records and checkpoints (SPEC sections 7.6, 8.1).

use std::sync::Arc;

use bytes::Bytes;
use format::crc32c;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use proptest::prelude::*;
use storage::{
    CheckpointFile, CheckpointHead, CheckpointSet, MetaCursor, MetaExtent, MetaSink, Ownership,
    StreamMeta, build_checkpoint, checkpoint_path, decode_extent, decode_meta, decode_ownership,
    encode_extent, encode_meta, encode_ownership, put_checkpoint,
};

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

fn cursor() -> impl Strategy<Value = MetaCursor> {
    (".{0,8}", any::<u64>()).prop_map(|(name, id)| MetaCursor {
        name: key(&name),
        id,
    })
}

fn sink() -> impl Strategy<Value = MetaSink> {
    (
        any::<u64>(),
        ".{0,4}",
        proptest::option::of(".{0,4}"),
        proptest::option::of(".{0,4}"),
        proptest::option::of(".{0,4}"),
        ".{0,4}",
        any::<u32>(),
        any::<bool>(),
    )
        .prop_map(
            |(sink_id, kind, url, target, filter, on_expire, concurrency, paused)| MetaSink {
                sink_id,
                kind,
                url: url.clone(),
                target: target.as_deref().map(key),
                filter,
                on_expire,
                concurrency,
                paused,
                secret: url.clone(),
                interval_ms: concurrency.into(),
                budget_bytes: u64::from(concurrency) + 1,
                pause_reason: target.clone(),
                gaps: if paused {
                    vec![storage::MetaSinkGap {
                        first_id: sink_id,
                        last_id: sink_id / 2,
                    }]
                } else {
                    Vec::new()
                },
                gap_count: u64::from(paused),
            },
        )
}

fn extent() -> impl Strategy<Value = MetaExtent> {
    (".{0,8}", any::<(u64, u64, u64, u64, u32)>()).prop_map(
        |(path, (offset, len_bytes, id_first, id_last, block_count))| MetaExtent {
            path,
            offset,
            len_bytes,
            id_first,
            id_last,
            block_count,
        },
    )
}

fn meta() -> impl Strategy<Value = StreamMeta> {
    (
        (".{1,8}", any::<bool>(), any::<(u64, u64, u64, u64)>()),
        proptest::collection::vec(any::<u8>(), 0..32),
        any::<(u8, u8, u64)>(),
        proptest::collection::vec(cursor(), 0..4),
        proptest::collection::vec(sink(), 0..3),
        (
            proptest::collection::vec(extent(), 0..4),
            proptest::collection::vec(any::<u64>(), 0..4),
        ),
    )
        .prop_map(
            |(
                (name, deleted, (last_id, floor_id, applied_id, state_ver)),
                state,
                (default_ack_level, min_ack_level, retention_ms),
                cursors,
                sinks,
                (extents, dropped_extents),
            )| StreamMeta {
                stream: key(&name),
                deleted,
                last_id,
                floor_id,
                expired_through: floor_id / 2,
                applied_id,
                state_ver,
                state: Bytes::from(state),
                default_ack_level,
                min_ack_level,
                retention_ms,
                cursors,
                sinks,
                extents,
                dropped_extents,
            },
        )
}

proptest! {
    #[test]
    fn meta_round_trips(meta in meta()) {
        prop_assert_eq!(decode_meta(&encode_meta(&meta)).unwrap(), meta);
    }

    #[test]
    fn any_cut_or_flip_of_meta_is_an_error(meta in meta(), at in any::<prop::sample::Index>()) {
        let bytes = encode_meta(&meta);
        let at = at.index(bytes.len());
        prop_assert!(decode_meta(&bytes[..at]).is_err());
        let mut flipped = bytes;
        flipped[at] ^= 0x10;
        prop_assert!(decode_meta(&flipped).is_err());
    }

    #[test]
    fn random_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        let _ = decode_meta(&bytes);
        let _ = decode_extent(&bytes);
        let _ = decode_ownership(&bytes);
    }

    /// With a good CRC over junk, so the parser itself gets looked at.
    #[test]
    fn junk_with_a_good_crc_never_panics(junk in proptest::collection::vec(any::<u8>(), 0..256)) {
        let mut bytes = crc32c(&junk).to_le_bytes().to_vec();
        bytes.extend_from_slice(&junk);
        let _ = decode_meta(&bytes);
    }

    #[test]
    fn extents_round_trip(extent in extent()) {
        let mut out = Vec::new();
        encode_extent(&mut out, &extent);
        prop_assert_eq!(decode_extent(&out).unwrap(), extent);
    }

    #[test]
    fn ownership_round_trips(generation in any::<u64>(), last_valid_id in any::<u64>()) {
        let bytes = encode_ownership(generation, last_valid_id);
        prop_assert_eq!(decode_ownership(&bytes), Some((generation, last_valid_id)));
    }
}

#[test]
fn ownership_bytes_are_exact() {
    let mut want = Vec::new();
    want.extend_from_slice(b"SSOWN001");
    want.extend_from_slice(&[3, 0, 0, 0, 0, 0, 0, 0]);
    want.extend_from_slice(&[0x10, 0, 0, 0, 0, 0, 0, 0]);
    let crc = crc32c(&want);
    want.extend_from_slice(&crc.to_le_bytes());
    want.extend_from_slice(&[0, 0, 0, 0]);
    assert_eq!(encode_ownership(3, 0x10).as_slice(), want.as_slice());
    let mut flipped = want;
    flipped[9] ^= 1;
    assert_eq!(decode_ownership(&flipped), None);
}

#[test]
fn meta_bytes_are_exact() {
    let meta = StreamMeta {
        stream: key("s"),
        deleted: true,
        last_id: 2,
        floor_id: 3,
        expired_through: 20,
        applied_id: 4,
        state_ver: 5,
        state: Bytes::from_static(b"st"),
        default_ack_level: 2,
        min_ack_level: 1,
        retention_ms: 6,
        cursors: vec![MetaCursor {
            name: key("c"),
            id: 7,
        }],
        sinks: vec![MetaSink {
            sink_id: 12,
            kind: "k".to_owned(),
            url: Some("u".to_owned()),
            target: Some(key("t")),
            filter: None,
            on_expire: "e".to_owned(),
            concurrency: 13,
            paused: true,
            secret: Some("x".to_owned()),
            interval_ms: 20,
            budget_bytes: 21,
            pause_reason: Some("r".to_owned()),
            gaps: vec![storage::MetaSinkGap {
                first_id: 22,
                last_id: 23,
            }],
            gap_count: 24,
        }],
        extents: vec![MetaExtent {
            path: "p".to_owned(),
            offset: 14,
            len_bytes: 15,
            id_first: 16,
            id_last: 17,
            block_count: 18,
        }],
        dropped_extents: vec![19],
    };
    let mut body = Vec::new();
    let u32le = |out: &mut Vec<u8>, value: u32| out.extend_from_slice(&value.to_le_bytes());
    let u64le = |out: &mut Vec<u8>, value: u64| out.extend_from_slice(&value.to_le_bytes());
    let text = |out: &mut Vec<u8>, value: &[u8]| {
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value);
    };
    u32le(&mut body, 1);
    body.extend_from_slice(&key("s").0);
    for value in [2, 3, 20, 4, 5] {
        u64le(&mut body, value);
    }
    text(&mut body, b"st");
    body.extend_from_slice(&[2, 1, 0, 0, 0, 0, 0, 0]);
    u64le(&mut body, 6);
    u32le(&mut body, 1);
    body.extend_from_slice(&key("c").0);
    u64le(&mut body, 7);
    u32le(&mut body, 1);
    u64le(&mut body, 12);
    text(&mut body, b"k");
    body.push(1);
    text(&mut body, b"u");
    body.push(1);
    body.extend_from_slice(&key("t").0);
    body.push(0);
    text(&mut body, b"e");
    u32le(&mut body, 13);
    body.push(1);
    body.push(1);
    text(&mut body, b"x");
    u64le(&mut body, 20);
    u64le(&mut body, 21);
    body.push(1);
    text(&mut body, b"r");
    u32le(&mut body, 1);
    u64le(&mut body, 22);
    u64le(&mut body, 23);
    u64le(&mut body, 24);
    u32le(&mut body, 1);
    text(&mut body, b"p");
    for value in [14, 15, 16, 17] {
        u64le(&mut body, value);
    }
    u32le(&mut body, 18);
    u32le(&mut body, 1);
    u64le(&mut body, 19);
    let mut want = crc32c(&body).to_le_bytes().to_vec();
    want.extend_from_slice(&body);
    assert_eq!(encode_meta(&meta), want);
    assert_eq!(decode_meta(&want).unwrap(), meta);
}

fn stream(name: &str, last_id: u64, extent_ids: &[(u64, u64)]) -> StreamMeta {
    StreamMeta {
        stream: key(name),
        last_id,
        extents: extent_ids
            .iter()
            .map(|(id_first, id_last)| MetaExtent {
                path: format!("o/{id_first}"),
                offset: 0,
                len_bytes: 8,
                id_first: *id_first,
                id_last: *id_last,
                block_count: 1,
            })
            .collect(),
        ..StreamMeta::default()
    }
}

async fn put(
    store: &Arc<dyn ObjectStore>,
    generation: u64,
    seq: u64,
    full: bool,
    max_id: u64,
    metas: &[StreamMeta],
) -> CheckpointFile {
    let head = CheckpointHead {
        sink_streams: vec![key("with.sink")],
        full,
        max_id,
        objects: vec![format!("obj-{seq}")],
    };
    let built = build_checkpoint(&head, metas).unwrap();
    let path = checkpoint_path(generation, seq);
    put_checkpoint(store.as_ref(), &path, &built).await.unwrap();
    // The same bytes again is what a retry after a lost answer does.
    put_checkpoint(store.as_ref(), &path, &built).await.unwrap();
    CheckpointFile::new(generation, seq, built.top, head)
}

#[tokio::test]
async fn deltas_add_up_and_a_tombstone_ends_the_walk() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    put(
        &store,
        1,
        1,
        false,
        10,
        &[stream("a", 10, &[(1, 10)]), stream("gone", 5, &[(1, 5)])],
    )
    .await;
    let mut tombstone = stream("gone", 5, &[]);
    tombstone.deleted = true;
    put(
        &store,
        1,
        2,
        false,
        20,
        &[stream("a", 20, &[(11, 20)]), tombstone],
    )
    .await;
    let set = CheckpointSet::read(&store, Ownership::from_records([(1, 0)]))
        .await
        .unwrap();
    assert_eq!(set.files.len(), 2);
    assert_eq!(set.max_id(), 20);
    let a = set.lookup(&store, key("a")).await.unwrap().unwrap();
    assert_eq!(a.last_id, 20);
    let mut ids: Vec<_> = a.extents.iter().map(|extent| extent.id_first).collect();
    ids.sort_unstable();
    assert_eq!(ids, [1, 11]);
    assert!(
        set.lookup(&store, key("gone"))
            .await
            .unwrap()
            .unwrap()
            .deleted
    );
    assert_eq!(set.lookup(&store, key("nobody")).await.unwrap(), None);

    let merged = set.merge_all(&store).await.unwrap();
    assert_eq!(merged[&key("a")].extents.len(), 2);
    assert!(merged[&key("gone")].deleted);
    assert!(merged[&key("gone")].extents.is_empty());
}

#[tokio::test]
async fn a_full_file_ends_the_walk_and_older_files_are_marked_to_go() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    put(&store, 1, 1, false, 10, &[stream("a", 10, &[(1, 10)])]).await;
    put(
        &store,
        1,
        2,
        true,
        20,
        &[stream("a", 20, &[(1, 10), (11, 20)])],
    )
    .await;
    put(&store, 1, 3, false, 30, &[stream("b", 30, &[(21, 30)])]).await;
    let set = CheckpointSet::read(&store, Ownership::from_records([(1, 0)]))
        .await
        .unwrap();
    assert_eq!(set.files.len(), 2);
    assert_eq!(set.superseded, vec![checkpoint_path(1, 1)]);
    assert_eq!(
        set.lookup(&store, key("a"))
            .await
            .unwrap()
            .unwrap()
            .extents
            .len(),
        2
    );
}

#[tokio::test]
async fn a_former_generations_file_counts_up_to_its_limit() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    put(&store, 1, 1, false, 10, &[stream("a", 10, &[(1, 10)])]).await;
    // Written by generation 1 after it lost the node, with ids past where 2 began.
    put(&store, 1, 2, false, 20, &[stream("a", 20, &[(11, 20)])]).await;
    let set = CheckpointSet::read(&store, Ownership::from_records([(1, 0), (2, 10)]))
        .await
        .unwrap();
    let a = set.lookup(&store, key("a")).await.unwrap().unwrap();
    assert_eq!(a.extents.len(), 1);
    assert_eq!(a.extents[0].id_last, 10);
}

/// Compaction's update and retention's removal, through the walk and through a full file.
#[tokio::test]
async fn a_newer_entry_takes_away_what_it_drops_and_keeps_what_it_adds() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = put(
        &store,
        1,
        1,
        false,
        20,
        &[stream("s", 20, &[(1, 5), (6, 9), (10, 20)])],
    )
    .await;
    // (1, 9) replaces two, one of them with the same id_first.
    let mut replaced = stream("s", 20, &[(1, 9)]);
    replaced.dropped_extents = vec![1, 6];
    let second = put(&store, 1, 2, false, 20, &[replaced]).await;
    let mut expired = stream("s", 20, &[]);
    expired.dropped_extents = vec![10];
    let third = put(&store, 1, 3, false, 20, &[expired]).await;

    let set = CheckpointSet {
        files: vec![third, second, first],
        ..CheckpointSet::default()
    };
    let found = set.lookup(&store, key("s")).await.unwrap().unwrap();
    let ranges: Vec<(u64, u64)> = found
        .extents
        .iter()
        .map(|extent| (extent.id_first, extent.id_last))
        .collect();
    assert_eq!(ranges, [(1, 9)]);
    assert!(found.dropped_extents.is_empty());

    let merged = set.merge_all(&store).await.unwrap();
    assert_eq!(merged[&key("s")].extents, found.extents);
    assert!(merged[&key("s")].dropped_extents.is_empty());
}

#[test]
fn compacted_paths_keep_the_hour_and_generation_form() {
    let path = storage::compacted_path("7d", 500_000, "node", 3, 77);
    assert_eq!(
        path.as_ref(),
        "7d/500000/node/00000000000000000003-00000000000000000077.seg"
    );
    assert_eq!(storage::object_hour(&path), Some(500_000));
    assert_eq!(storage::object_generation(&path), Some(3));
}
