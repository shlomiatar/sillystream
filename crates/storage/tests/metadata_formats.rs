//! The bytes of stream metadata, ownership records and checkpoints (SPEC sections 7.6, 8.1).

use std::sync::Arc;

use bytes::Bytes;
use format::crc32c;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use proptest::prelude::*;
use storage::{
    CheckpointFile, CheckpointHead, CheckpointSet, MetaCursor, MetaSink, NewerObjects, ObjectId,
    ObjectKey, ObjectRow, ObjectState, ObjectTable, Ownership, Replacement, RetentionClass,
    StreamMeta, build_checkpoint, checkpoint_path, decode_ids, decode_meta, decode_ownership,
    decode_rows, encode_ids, encode_meta, encode_ownership, encode_rows, put_checkpoint,
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
            |(sink_id, kind, url, target, filter, on_expire, budget, paused)| MetaSink {
                sink_id,
                kind,
                url: url.clone(),
                target: target.as_deref().map(key),
                filter,
                on_expire,
                paused,
                secret: url.clone(),
                budget_bytes: u64::from(budget) + 1,
                pause_reason: target.clone(),
                last_gap: paused.then_some(storage::MetaSinkGap {
                    first_id: sink_id,
                    last_id: sink_id / 2,
                }),
                gap_count: u64::from(paused),
                compression: paused.then(|| "zstd".to_owned()),
            },
        )
}

/// A sorted list of distinct ids, as a stream's list is.
fn ids() -> impl Strategy<Value = Vec<ObjectId>> {
    proptest::collection::vec(any::<u64>(), 0..6).prop_map(|mut raw| {
        raw.sort_unstable();
        raw.dedup();
        raw.into_iter().map(ObjectId).collect()
    })
}

fn class() -> impl Strategy<Value = RetentionClass> {
    prop::sample::select(RetentionClass::ALL.to_vec())
}

fn row() -> impl Strategy<Value = ObjectRow> {
    (
        proptest::option::of(class()),
        any::<u64>(),
        prop::sample::select(vec![
            ObjectState::Local,
            ObjectState::Cached,
            ObjectState::Remote,
        ]),
        proptest::option::of(0u8..8),
        proptest::collection::vec((class(), any::<(u64, u64, u64)>()), 0..3),
    )
        .prop_map(|(class, id, state, held, replacements)| ObjectRow {
            key: ObjectKey {
                class,
                id: ObjectId(id),
            },
            state,
            held,
            replacements: replacements
                .into_iter()
                .map(|(class, (hash_first, hash_last, by))| Replacement {
                    class,
                    hash_first,
                    hash_last,
                    by: ObjectId(by),
                })
                .collect(),
        })
}

fn meta() -> impl Strategy<Value = StreamMeta> {
    (
        (".{1,8}", any::<bool>(), any::<(u64, u64, u64, u64)>()),
        proptest::collection::vec(any::<u8>(), 0..32),
        any::<(u8, u8, u64)>(),
        proptest::collection::vec(cursor(), 0..4),
        proptest::collection::vec(sink(), 0..3),
        ids(),
    )
        .prop_map(
            |(
                (name, deleted, (last_id, floor_id, applied_id, state_ver)),
                state,
                (default_ack_level, min_ack_level, retention_ms),
                cursors,
                sinks,
                objects,
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
                objects,
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
        let _ = decode_ids(&bytes);
        let _ = decode_rows(&bytes);
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
    fn ids_round_trip(ids in ids()) {
        let mut out = Vec::new();
        encode_ids(&mut out, &ids);
        prop_assert_eq!(decode_ids(&out).unwrap(), ids);
    }

    #[test]
    fn rows_round_trip(rows in proptest::collection::vec(row(), 0..4)) {
        let mut out = Vec::new();
        encode_rows(&mut out, &rows);
        let (decoded, taken) = decode_rows(&out).unwrap();
        prop_assert_eq!(taken, out.len());
        prop_assert_eq!(decoded, rows);
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
            paused: true,
            secret: Some("x".to_owned()),
            budget_bytes: 21,
            pause_reason: Some("r".to_owned()),
            last_gap: Some(storage::MetaSinkGap {
                first_id: 22,
                last_id: 23,
            }),
            gap_count: 24,
            compression: Some("gzip".to_owned()),
        }],
        objects: vec![ObjectId(5), ObjectId(300)],
    };
    let mut body = Vec::new();
    let u32le = |out: &mut Vec<u8>, value: u32| out.extend_from_slice(&value.to_le_bytes());
    let u64le = |out: &mut Vec<u8>, value: u64| out.extend_from_slice(&value.to_le_bytes());
    let text = |out: &mut Vec<u8>, value: &[u8]| {
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value);
    };
    u32le(&mut body, 4);
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
    body.push(1);
    body.push(1);
    text(&mut body, b"x");
    u64le(&mut body, 21);
    body.push(1);
    text(&mut body, b"r");
    body.push(1);
    u64le(&mut body, 22);
    u64le(&mut body, 23);
    u64le(&mut body, 24);
    body.push(1);
    text(&mut body, b"gzip");
    // The list: a count, then each id's distance from the one before as a varint.
    u32le(&mut body, 2);
    body.extend_from_slice(&[5, 0xA7, 0x02]);
    let mut want = crc32c(&body).to_le_bytes().to_vec();
    want.extend_from_slice(&body);
    assert_eq!(encode_meta(&meta), want);
    assert_eq!(decode_meta(&want).unwrap(), meta);
}

/// Bytes from before the version field, whose word there was `flags`, or from a later layout,
/// are refused by name rather than misread.
#[test]
fn another_format_version_is_refused_with_both_numbers() {
    let meta = StreamMeta {
        stream: key("s"),
        ..StreamMeta::default()
    };
    for found in [0u32, 1, storage::META_VERSION + 1] {
        let mut bytes = encode_meta(&meta);
        bytes[4..8].copy_from_slice(&found.to_le_bytes());
        let crc = crc32c(&bytes[4..]);
        bytes[..4].copy_from_slice(&crc.to_le_bytes());
        let error = decode_meta(&bytes).unwrap_err().to_string();
        assert!(error.contains(&format!("version {found}")), "{error}");
        assert!(
            error.contains(&storage::META_VERSION.to_string()),
            "{error}"
        );

        let mut sink = storage::encode_sink(&MetaSink {
            sink_id: 1,
            kind: "archive".to_owned(),
            url: None,
            target: None,
            filter: None,
            on_expire: "stop".to_owned(),
            paused: false,
            secret: None,
            budget_bytes: 0,
            pause_reason: None,
            last_gap: None,
            gap_count: 0,
            compression: None,
        });
        sink[..4].copy_from_slice(&found.to_le_bytes());
        let error = storage::decode_sink(&sink).unwrap_err().to_string();
        assert!(error.contains(&format!("version {found}")), "{error}");
    }
}

#[test]
fn a_list_out_of_order_is_refused() {
    let mut out = Vec::new();
    encode_ids(&mut out, &[ObjectId(5), ObjectId(9)]);
    // The second delta made 0: the same id twice.
    out[5] = 0;
    assert!(decode_ids(&out).is_err());
    // A varint that runs past 64 bits.
    let mut long = 1u32.to_le_bytes().to_vec();
    long.extend_from_slice(&[0xFF; 10]);
    long.push(1);
    assert!(decode_ids(&long).is_err());
}

#[test]
fn a_sealed_id_leads_to_its_copy_for_its_class_and_hash() {
    let sealed = ObjectId::new(storage::OBJECT_EPOCH_MS + 10, 1, 0);
    let copy = ObjectId::compacted(sealed.hour(), 1, 0);
    let mut table = ObjectTable::new();
    table.update(ObjectKey::sealed(sealed), ObjectState::Remote, |row| {
        row.held = Some(RetentionClass::Days7.bit());
        row.replacements.push(Replacement {
            class: RetentionClass::Days7,
            hash_first: 10,
            hash_last: 20,
            by: copy,
        });
    });
    let days7 = Some(RetentionClass::Days7);
    assert_eq!(
        table.resolve(sealed, days7, 15),
        ObjectKey::compacted(RetentionClass::Days7, copy)
    );
    // Outside the range, another class, or 1d: the sealed object itself.
    assert_eq!(table.resolve(sealed, days7, 21), ObjectKey::sealed(sealed));
    assert_eq!(
        table.resolve(sealed, Some(RetentionClass::Days30), 15),
        ObjectKey::sealed(sealed)
    );
    assert_eq!(table.resolve(sealed, None, 15), ObjectKey::sealed(sealed));
    // A compacted id in a list is its class's.
    assert_eq!(
        table.resolve(copy, days7, 15),
        ObjectKey::compacted(RetentionClass::Days7, copy)
    );
    assert!(copy.is_compacted() && !sealed.is_compacted());
    assert!(table.get(ObjectKey::sealed(sealed)).unwrap().is_passed());
    let (changed, removed) = table.take_changed();
    assert_eq!((changed.len(), removed.len()), (1, 0));
    table.remove(ObjectKey::sealed(sealed));
    assert_eq!(
        table.take_changed(),
        (Vec::new(), vec![ObjectKey::sealed(sealed)])
    );
}

fn object(n: u64) -> ObjectId {
    ObjectId::new(storage::OBJECT_EPOCH_MS + n * 1000, 1, 0)
}

fn stream(name: &str, last_id: u64, objects: &[u64]) -> StreamMeta {
    StreamMeta {
        stream: key(name),
        last_id,
        objects: objects.iter().map(|n| object(*n)).collect(),
        ..StreamMeta::default()
    }
}

fn sealed_row(n: u64) -> ObjectRow {
    ObjectRow::new(ObjectKey::sealed(object(n)), ObjectState::Remote)
}

async fn put(
    store: &Arc<dyn ObjectStore>,
    generation: u64,
    seq: u64,
    head: CheckpointHead,
    metas: &[StreamMeta],
) -> CheckpointFile {
    let head = CheckpointHead {
        sink_streams: vec![key("with.sink")],
        ..head
    };
    let built = build_checkpoint(&head, metas).unwrap();
    let path = checkpoint_path(generation, seq);
    put_checkpoint(store.as_ref(), &path, &built).await.unwrap();
    // The same bytes again is what a retry after a lost answer does.
    put_checkpoint(store.as_ref(), &path, &built).await.unwrap();
    CheckpointFile::new(generation, seq, built.top, head)
}

fn head(full: bool, max_id: u64, rows: Vec<ObjectRow>, removed: Vec<ObjectKey>) -> CheckpointHead {
    CheckpointHead {
        full,
        max_id,
        rows,
        removed,
        sink_streams: Vec::new(),
    }
}

/// What `NewerObjects::read` would find in each newer object's last ids.
fn newer(objects: &[(u64, &[(&str, u64)])]) -> NewerObjects {
    let mut newer = NewerObjects::default();
    for (n, last_ids) in objects {
        let mut last_ids: Vec<_> = last_ids.iter().map(|(name, id)| (key(name), *id)).collect();
        last_ids.sort_unstable();
        newer.add(object(*n), last_ids);
    }
    newer
}

/// The full file's list and last id, then each object recorded since, with the newest fields.
#[tokio::test]
async fn a_list_is_the_full_files_and_the_newer_objects() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    put(
        &store,
        1,
        1,
        head(true, 10, vec![sealed_row(1)], Vec::new()),
        &[stream("a", 10, &[1]), stream("gone", 5, &[1])],
    )
    .await;
    let mut tombstone = stream("gone", 5, &[]);
    tombstone.deleted = true;
    // An entry since the full file holds fields only: no list, and the last id it had then.
    let mut a = stream("a", 12, &[]);
    a.state_ver = 3;
    put(
        &store,
        1,
        2,
        head(false, 20, vec![sealed_row(2), sealed_row(3)], Vec::new()),
        &[a, tombstone],
    )
    .await;
    let set = CheckpointSet::read(&store, Ownership::from_records([(1, 0)]))
        .await
        .unwrap();
    assert_eq!(set.files.len(), 2);
    assert_eq!(set.max_id(), 20);
    assert_eq!(set.newer_objects(), vec![object(2), object(3)]);
    assert_eq!(set.rows().len(), 3);
    let newer = newer(&[(2, &[("a", 15)]), (3, &[("a", 20), ("b", 18)])]);

    let a = set.lookup(&store, key("a"), &newer).await.unwrap().unwrap();
    assert_eq!(a.last_id, 20);
    assert_eq!(a.state_ver, 3);
    assert_eq!(a.objects, vec![object(1), object(2), object(3)]);
    let gone = set
        .lookup(&store, key("gone"), &newer)
        .await
        .unwrap()
        .unwrap();
    assert!(gone.deleted && gone.objects.is_empty());
    // Only ever appended to since the full file: no entry, and a list all the same.
    let b = set.lookup(&store, key("b"), &newer).await.unwrap().unwrap();
    assert_eq!((b.last_id, b.objects.clone()), (18, vec![object(3)]));
    assert_eq!(
        set.lookup(&store, key("nobody"), &newer).await.unwrap(),
        None
    );

    let merged = set.merge_all(&store, &newer).await.unwrap();
    assert_eq!(merged[&key("a")], a);
    assert_eq!(merged[&key("b")], b);
    assert!(merged[&key("gone")].deleted);
}

#[tokio::test]
async fn a_full_file_ends_the_walk_and_older_files_are_marked_to_go() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    put(
        &store,
        1,
        1,
        head(false, 10, vec![sealed_row(1)], Vec::new()),
        &[stream("a", 10, &[])],
    )
    .await;
    put(
        &store,
        1,
        2,
        head(true, 20, vec![sealed_row(1), sealed_row(2)], Vec::new()),
        &[stream("a", 20, &[1, 2])],
    )
    .await;
    put(
        &store,
        1,
        3,
        head(false, 30, Vec::new(), vec![ObjectKey::sealed(object(1))]),
        &[stream("b", 30, &[])],
    )
    .await;
    let set = CheckpointSet::read(&store, Ownership::from_records([(1, 0)]))
        .await
        .unwrap();
    assert_eq!(set.files.len(), 2);
    assert_eq!(set.superseded, vec![checkpoint_path(1, 1)]);
    // A later file's removal takes a row away.
    assert_eq!(
        set.rows().into_keys().collect::<Vec<_>>(),
        vec![ObjectKey::sealed(object(2))]
    );
    let a = set
        .lookup(&store, key("a"), &NewerObjects::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.objects, vec![object(1), object(2)]);
}

#[tokio::test]
async fn a_former_generations_object_counts_up_to_its_limit() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    put(
        &store,
        1,
        1,
        head(true, 10, Vec::new(), Vec::new()),
        &[stream("a", 10, &[])],
    )
    .await;
    let set = CheckpointSet::read(&store, Ownership::from_records([(1, 0), (2, 12)]))
        .await
        .unwrap();
    // Generation 1's object holds ids past where 2 began.
    let newer = newer(&[(2, &[("a", 20)])]);
    let a = set.lookup(&store, key("a"), &newer).await.unwrap().unwrap();
    assert_eq!(a.last_id, 12);
    assert_eq!(a.objects, vec![object(2)]);
}

/// A fenced-off generation's full file is no base, even as the only file: what it says above
/// its limit is what the next owner never accepted.
#[tokio::test]
async fn a_fenced_off_full_file_is_no_base_for_a_lookup() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    put(
        &store,
        1,
        1,
        head(true, 20, vec![sealed_row(5)], Vec::new()),
        &[stream("a", 20, &[5])],
    )
    .await;
    // The next owner's own entry, below the limit.
    put(
        &store,
        2,
        2,
        head(false, 10, Vec::new(), Vec::new()),
        &[stream("a", 10, &[])],
    )
    .await;
    let set = CheckpointSet::read(&store, Ownership::from_records([(1, 0), (2, 12)]))
        .await
        .unwrap();
    let a = set
        .lookup(&store, key("a"), &NewerObjects::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.last_id, 10);
    assert!(!a.objects.contains(&object(5)), "{:?}", a.objects);
}

/// A stream deleted and made again: the full file's list was the old stream's.
#[tokio::test]
async fn a_floor_since_the_full_file_leaves_its_list_out() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    put(
        &store,
        1,
        1,
        head(true, 10, Vec::new(), Vec::new()),
        &[stream("a", 10, &[1])],
    )
    .await;
    let mut again = stream("a", 10, &[]);
    again.floor_id = 10;
    put(
        &store,
        1,
        2,
        head(false, 30, Vec::new(), Vec::new()),
        &[again],
    )
    .await;
    let set = CheckpointSet::read(&store, Ownership::from_records([(1, 0)]))
        .await
        .unwrap();
    let newer = newer(&[(2, &[("a", 9)]), (3, &[("a", 30)])]);
    let a = set.lookup(&store, key("a"), &newer).await.unwrap().unwrap();
    assert_eq!(a.objects, vec![object(3)]);
    assert_eq!(a.last_id, 30);
}
