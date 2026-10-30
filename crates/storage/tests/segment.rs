//! Segments: round trips, read counts, two-section streams, collisions and CRCs.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use common::{CountingStore, concat, put_parts, record};
use format::{Clause, Filter, RecordHeader, Records, crc32c};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use proptest::prelude::*;
use storage::{
    BLOCK_ENTRY_BYTES, SegmentBuilder, SegmentError, SegmentReader, TRAILER_BYTES,
    decode_block_table, decode_page, decode_top_index, stream_hash,
};

const FIRST_ID: u64 = 1_800_000_000_000 << 16;

fn stores() -> Vec<(Arc<dyn ObjectStore>, Option<tempfile::TempDir>)> {
    let dir = tempfile::tempdir().unwrap();
    let local = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    vec![
        (Arc::new(InMemory::new()), None),
        (Arc::new(local), Some(dir)),
    ]
}

async fn write(store: &dyn ObjectStore, builder: SegmentBuilder) -> (Path, storage::BuiltSegment) {
    let built = builder.finish().unwrap();
    let path = Path::from("segment.seg");
    put_parts(store, &path, &built.parts).await;
    (path, built)
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Any streams, any record sizes, any limits: every stream reads back exactly, the
    /// footer lists exactly them, and the object is what the parts said.
    #[test]
    fn round_trip(
        streams in prop::collection::btree_map(
            "[a-z0-9.]{1,12}",
            prop::collection::vec((any::<u64>(), 0u64..3, prop::collection::vec(any::<u8>(), 0..200)), 1..30),
            0..30,
        ),
        block_bytes in 32usize..2048,
        page_bytes in 64usize..2048,
    ) {
        runtime().block_on(async move {
            for (store, _dir) in stores() {
                let mut builder = SegmentBuilder::with_limits(block_bytes, page_bytes);
                let mut expected = BTreeMap::new();
                for (stream, records) in &streams {
                    let mut bytes = Vec::new();
                    for (index, (options, author, data)) in records.iter().enumerate() {
                        bytes.extend(record(FIRST_ID + index as u64, *options, *author, data));
                    }
                    let bytes = Bytes::from(bytes);
                    builder.add_extent(stream, [bytes.clone()]).unwrap();
                    expected.insert(stream.clone(), bytes);
                }
                let (path, built) = write(store.as_ref(), builder).await;
                let reader = SegmentReader::new(Arc::clone(&store), path.clone());
                let whole = store.get(&path).await.unwrap().bytes().await.unwrap();
                assert_eq!(whole.len() as u64, built.len_bytes);
                assert_eq!(&whole[..], &concat(&built.parts)[..]);
                let top = reader.read_top_index().await.unwrap();
                let all = reader.all_extents(&top).await.unwrap();
                assert_eq!(all, built.extents);
                assert_eq!(all.len(), expected.len());
                for (stream, bytes) in &expected {
                    assert_eq!(&reader.read_stream(stream).await.unwrap(), bytes);
                    let extents = reader.lookup(&top, stream).await.unwrap();
                    assert_eq!(extents.len(), 1);
                    let table = reader.read_table(&extents[0]).await.unwrap();
                    for block in &table.blocks {
                        // Blocks stay near the target; only a lone bigger record overruns.
                        let records = block.summary.bytes as usize;
                        assert!(records <= block_bytes || block.summary.count == 1);
                    }
                }
                let missing = reader.lookup(&top, "missing/never").await.unwrap();
                assert!(missing.is_empty());
            }
        });
    }

    #[test]
    fn decoders_never_panic_on_random_bytes(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        let _ = decode_block_table(&bytes);
        let _ = decode_page(&bytes, Some("s"));
        let _ = decode_page(&bytes, None);
        let _ = decode_top_index(&bytes);
    }
}

/// `decode_*` also see valid CRCs over nonsense, the case a CRC doesn't catch.
#[test]
fn decoders_refuse_nonsense_that_has_a_valid_crc() {
    let mut page = vec![0u8; 8];
    page[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
    let crc = crc32c(&page[4..]);
    page[..4].copy_from_slice(&crc.to_le_bytes());
    assert!(matches!(
        decode_page(&page, None),
        Err(SegmentError::Corrupt(_))
    ));
    let mut table = vec![0u8; 8];
    table[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
    let crc = crc32c(&table[4..]);
    table[..4].copy_from_slice(&crc.to_le_bytes());
    assert!(matches!(
        decode_block_table(&table),
        Err(SegmentError::Corrupt(_))
    ));
}

/// The acceptance figure: at most one top index read, one page read and two data reads,
/// across 1M streams.
#[tokio::test(flavor = "multi_thread")]
async fn lookup_across_a_million_streams_makes_the_promised_reads() {
    let dir = tempfile::tempdir().unwrap();
    let local = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let inners: Vec<Arc<dyn ObjectStore>> = vec![Arc::new(InMemory::new()), Arc::new(local)];

    let mut builder = SegmentBuilder::new();
    let payload = record(FIRST_ID, 1, 7, b"synthetic");
    let payload = Bytes::from(payload);
    for stream in 0..1_000_000u32 {
        builder
            .add_extent(&format!("logs.{stream}"), [payload.clone()])
            .unwrap();
    }
    let built = builder.finish().unwrap();
    assert_eq!(built.extents.len(), 1_000_000);

    for inner in inners {
        let counting = Arc::new(CountingStore::new(inner));
        let path = Path::from("sealed/million.seg");
        put_parts(counting.as_ref(), &path, &built.parts).await;
        let store: Arc<dyn ObjectStore> = counting.clone();
        let reader = SegmentReader::new(store, path);

        for stream in [0u32, 1, 499_999, 999_999] {
            let name = format!("logs.{stream}");
            counting.reset_gets();
            let top = reader.read_top_index().await.unwrap();
            assert_eq!(counting.gets(), 1, "one top index read");
            assert!(top.page_count() > 100, "the footer really has pages");

            let extents = reader.lookup(&top, &name).await.unwrap();
            assert_eq!(counting.gets(), 2, "one page read");
            assert_eq!(extents.len(), 1);

            let bytes = reader.read_extent(&extents[0]).await.unwrap();
            assert_eq!(counting.gets(), 4, "two data reads");
            assert_eq!(bytes, payload);

            // With the top index cached, the top index read goes away.
            counting.reset_gets();
            let extents = reader.lookup(&top, &name).await.unwrap();
            reader.read_extent(&extents[0]).await.unwrap();
            assert_eq!(counting.gets(), 3, "cached top index: page and two data");

            // With a stored extent, the whole footer goes away.
            counting.reset_gets();
            assert_eq!(reader.read_extent(&extents[0]).await.unwrap(), payload);
            assert_eq!(counting.gets(), 2, "stored extent: two data reads");
        }
        counting.reset_gets();
        let top = reader.read_top_index().await.unwrap();
        assert!(reader.lookup(&top, "logs.absent").await.unwrap().is_empty());
        assert_eq!(counting.gets(), 2, "a missing stream costs no data read");
    }
}

#[tokio::test]
async fn a_stream_in_two_sections_returns_both_entries() {
    for (store, _dir) in stores() {
        // Tiny pages, so that the run has to be kept together on purpose.
        let mut builder = SegmentBuilder::with_limits(1024, 64);
        let early = Bytes::from(record(FIRST_ID, 1, 7, b"on the old core"));
        let late = Bytes::from(record(FIRST_ID + 1, 1, 7, b"on the new core"));
        for other in 0..50 {
            builder
                .add_extent(&format!("other.{other}"), [early.clone()])
                .unwrap();
        }
        builder.add_extent("moved", [early.clone()]).unwrap();
        builder.add_extent("zz", [early.clone()]).unwrap();
        builder.add_extent("moved", [late.clone()]).unwrap();
        let (path, _built) = write(store.as_ref(), builder).await;
        let reader = SegmentReader::new(store, path);
        let top = reader.read_top_index().await.unwrap();
        let extents = reader.lookup(&top, "moved").await.unwrap();
        assert_eq!(extents.len(), 2);
        assert_eq!(reader.read_extent(&extents[0]).await.unwrap(), early);
        assert_eq!(reader.read_extent(&extents[1]).await.unwrap(), late);
        let mut both = early.to_vec();
        both.extend_from_slice(&late);
        assert_eq!(reader.read_stream("moved").await.unwrap(), both);
    }
}

/// A real 64-bit collision can't be found on demand, so this writes the bytes of
/// section 7.3 by hand, independently of the builder, with `b` claiming `a`'s hash.
#[tokio::test]
async fn hash_collisions_resolve_by_name() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let hash = stream_hash("a");

    let record_a = record(FIRST_ID, 1, 7, b"aaaa");
    let record_b = record(FIRST_ID, 2, 7, b"bbbb");
    let mut object = Vec::new();
    let mut extent = |stream_records: &[u8]| {
        let header = RecordHeader::read(stream_records).unwrap();
        let offset = object.len() as u64;
        let mut table = vec![0u8; 8];
        table[4..8].copy_from_slice(&1u32.to_le_bytes());
        for field in [
            header.options,
            header.options,
            header.id,
            header.id,
            1,
            stream_records.len() as u64,
            header.author,
        ] {
            table.extend_from_slice(&field.to_le_bytes());
        }
        table.extend_from_slice(&0u32.to_le_bytes());
        table.extend_from_slice(&crc32c(stream_records).to_le_bytes());
        let crc = crc32c(&table[4..]);
        table[..4].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(table.len(), 8 + BLOCK_ENTRY_BYTES);
        object.extend_from_slice(&table);
        object.extend_from_slice(stream_records);
        (
            offset,
            (table.len() + stream_records.len()) as u64,
            header.id,
        )
    };
    let a = extent(&record_a);
    let b = extent(&record_b);

    let mut page = vec![0u8; 8];
    page[4..8].copy_from_slice(&2u32.to_le_bytes());
    // `b` first, so that a lookup for `a` has to skip a matching hash.
    for (name, (offset, len, id)) in [("b", b), ("a", a)] {
        for field in [hash, offset, len, id, id] {
            page.extend_from_slice(&field.to_le_bytes());
        }
        page.extend_from_slice(&1u32.to_le_bytes());
        page.extend_from_slice(&(name.len() as u16).to_le_bytes());
        page.extend_from_slice(&[0, 0]);
        page.extend_from_slice(name.as_bytes());
        page.resize(page.len() + 7, 0);
    }
    let crc = crc32c(&page[4..]);
    page[..4].copy_from_slice(&crc.to_le_bytes());
    let page_offset = object.len() as u64;
    object.extend_from_slice(&page);

    let top_offset = object.len() as u64;
    let mut footer = Vec::new();
    footer.extend_from_slice(&1u32.to_le_bytes());
    footer.extend_from_slice(&[0; 4]);
    footer.extend_from_slice(&hash.to_le_bytes());
    footer.extend_from_slice(&page_offset.to_le_bytes());
    footer.extend_from_slice(&(page.len() as u32).to_le_bytes());
    footer.extend_from_slice(&[0; 4]);
    let top_len = footer.len() as u32;
    footer.extend_from_slice(&storage::SEGMENT_MAGIC.to_le_bytes());
    footer.extend_from_slice(&top_offset.to_le_bytes());
    footer.extend_from_slice(&top_len.to_le_bytes());
    let crc = crc32c(&footer);
    footer.extend_from_slice(&crc.to_le_bytes());
    assert_eq!(footer.len(), top_len as usize + TRAILER_BYTES);
    object.extend_from_slice(&footer);

    let path = Path::from("collide.seg");
    store.put(&path, object.into()).await.unwrap();
    let reader = SegmentReader::new(store, path);
    let top = reader.read_top_index().await.unwrap();
    let found = reader.lookup(&top, "a").await.unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(&*found[0].stream, "a");
    assert_eq!(
        reader.read_extent(&found[0]).await.unwrap()[..],
        record_a[..]
    );
    // `b` hashes elsewhere in reality, so it finds no page entry with its own hash.
    assert!(reader.lookup(&top, "b").await.unwrap().is_empty());
    let page_only_b = decode_page(&page, Some("a")).unwrap();
    assert_eq!(page_only_b.len(), 1);
    assert_eq!(decode_page(&page, None).unwrap().len(), 2);
}

/// Reads everything a reader can read, returning the first failure.
async fn read_everything(
    store: Arc<dyn ObjectStore>,
    path: &Path,
    streams: &[&str],
) -> Result<(), SegmentError> {
    let reader = SegmentReader::new(store, path.clone());
    let top = reader.read_top_index().await?;
    reader.all_extents(&top).await?;
    for stream in streams {
        reader.read_stream(stream).await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_flipped_bit_anywhere_fails() {
    let mut builder = SegmentBuilder::with_limits(128, 128);
    let mut bytes = Vec::new();
    for index in 0..4 {
        bytes.extend(record(FIRST_ID + index, index, 7, b"some data here"));
    }
    let bytes = Bytes::from(bytes);
    let streams = ["alpha", "beta", "gamma"];
    for stream in streams {
        builder.add_extent(stream, [bytes.clone()]).unwrap();
    }
    let object = concat(&builder.finish().unwrap().parts);
    let path = Path::from("flip.seg");

    let clean: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    clean.put(&path, object.clone().into()).await.unwrap();
    read_everything(clean, &path, &streams).await.unwrap();

    let mut crc_failures = 0;
    for bit in 0..object.len() * 8 {
        let mut flipped = object.clone();
        flipped[bit / 8] ^= 1 << (bit % 8);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store.put(&path, flipped.into()).await.unwrap();
        let error = read_everything(store, &path, &streams)
            .await
            .expect_err(&format!("bit {bit} went unnoticed"));
        if matches!(error, SegmentError::Crc(_)) {
            crc_failures += 1;
        }
    }
    // A trailer flip may instead fail the magic or a range check. All else is a CRC.
    let non_crc = object.len() * 8 - crc_failures;
    assert!(non_crc <= TRAILER_BYTES * 8, "{non_crc} non-CRC failures");
}

#[tokio::test]
async fn a_flipped_bit_in_a_block_is_named_as_a_crc_failure() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut builder = SegmentBuilder::new();
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"data"));
    builder.add_extent("s", [bytes.clone()]).unwrap();
    let built = builder.finish().unwrap();
    let mut object = concat(&built.parts);
    let table_bytes = 8 + BLOCK_ENTRY_BYTES;
    object[table_bytes + 33] ^= 4;
    let path = Path::from("x.seg");
    store.put(&path, object.into()).await.unwrap();
    let error = SegmentReader::new(store, path)
        .read_stream("s")
        .await
        .unwrap_err();
    assert!(matches!(error, SegmentError::Crc("block")), "{error}");
}

#[tokio::test]
async fn record_bytes_in_a_segment_equal_the_wire_bytes() {
    for (store, _dir) in stores() {
        // Random-looking bytes, including a compressed-looking data section the
        // builder has no business touching.
        let mut wire = Vec::new();
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        for index in 0..2_000u64 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let data = state.to_le_bytes().repeat((state % 40) as usize);
            wire.extend(record(
                FIRST_ID + index,
                state,
                7,
                &data[..data.len() - (data.len() % 5)],
            ));
        }
        let wire = Bytes::from(wire);
        let mut builder = SegmentBuilder::new();
        // Split the references at arbitrary record boundaries, as publishes would be.
        let mut pieces = Vec::new();
        let mut offset = 0;
        let mut piece_start = 0;
        for view in Records::new(&wire) {
            offset += view.unwrap().header.total_bytes();
            if offset - piece_start > 1000 {
                pieces.push(wire.slice(piece_start..offset));
                piece_start = offset;
            }
        }
        pieces.push(wire.slice(piece_start..offset));
        builder.add_extent("s", pieces).unwrap();
        let (path, built) = write(store.as_ref(), builder).await;
        assert!(
            built.extents[0].block_count > 1,
            "the test needs many blocks"
        );
        let reader = SegmentReader::new(Arc::clone(&store), path.clone());
        assert_eq!(reader.read_stream("s").await.unwrap(), wire);
        // In the object itself: table, then the wire bytes as one run.
        let whole = store.get(&path).await.unwrap().bytes().await.unwrap();
        let start = 8 + built.extents[0].block_count as usize * BLOCK_ENTRY_BYTES;
        assert_eq!(&whole[start..start + wire.len()], &wire[..]);
    }
}

#[test]
fn the_builder_does_not_copy_record_bytes() {
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"pointer check"));
    let mut builder = SegmentBuilder::new();
    builder.add_extent("s", [bytes.clone()]).unwrap();
    let built = builder.finish().unwrap();
    assert!(
        built
            .parts
            .iter()
            .any(|part| part.as_ptr() == bytes.as_ptr() && part.len() == bytes.len()),
        "a part must be the original memory"
    );
}

#[test]
fn the_builder_refuses_what_is_not_stamped_records_in_order() {
    let id_zero = Bytes::from(record(0, 1, 7, b"x"));
    assert!(matches!(
        SegmentBuilder::new().add_extent("s", [id_zero]),
        Err(SegmentError::OutOfOrder(_))
    ));
    let late = record(FIRST_ID + 1, 1, 7, b"x");
    let early = record(FIRST_ID, 1, 7, b"x");
    assert!(matches!(
        SegmentBuilder::new().add_extent("s", [Bytes::from([late, early].concat())]),
        Err(SegmentError::OutOfOrder(_))
    ));
    let mut cut = record(FIRST_ID, 1, 7, b"xxxxxxxx");
    cut.truncate(cut.len() - 8);
    assert!(matches!(
        SegmentBuilder::new().add_extent("s", [Bytes::from(cut)]),
        Err(SegmentError::Records(_))
    ));
}

#[tokio::test]
async fn an_empty_segment_reads_as_empty() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (path, built) = write(store.as_ref(), SegmentBuilder::new()).await;
    assert_eq!(built.first_id, None);
    let reader = SegmentReader::new(store, path);
    let top = reader.read_top_index().await.unwrap();
    assert_eq!(top.page_count(), 0);
    assert!(reader.lookup(&top, "s").await.unwrap().is_empty());
}

#[tokio::test]
async fn a_rare_filter_reads_only_the_blocks_that_can_match() {
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let mut builder = SegmentBuilder::with_limits(1024, 4096);
    let mut wire = Vec::new();
    let total = 1000u64;
    for index in 0..total {
        // One rare record, in the middle.
        let options = u64::from(index == 500) << 63;
        wire.extend(record(FIRST_ID + index, options, 7, &[0x55; 64]));
    }
    builder.add_extent("s", [Bytes::from(wire)]).unwrap();
    let built = builder.finish().unwrap();
    let path = Path::from("filter.seg");
    put_parts(counting.as_ref(), &path, &built.parts).await;
    let store: Arc<dyn ObjectStore> = counting.clone();
    let reader = SegmentReader::new(store, path);
    let extent = &built.extents[0];
    let table = reader.read_table(extent).await.unwrap();
    assert!(table.blocks.len() > 10);
    counting.reset_gets();
    let filter = Filter::new(&[Clause::Eq {
        mask: 1 << 63,
        value: 1 << 63,
    }])
    .unwrap();
    let blocks = reader
        .read_matching_blocks(extent, &table, &filter)
        .await
        .unwrap();
    assert_eq!(counting.gets(), 1);
    assert_eq!(blocks.len(), 1);
    let matched: Vec<u64> = Records::new(&blocks[0])
        .map(|view| view.unwrap().header)
        .filter(|header| filter.matches(header))
        .map(|header| header.id)
        .collect();
    assert_eq!(matched, [FIRST_ID + 500]);
    assert!(blocks[0].len() <= 1024, "only the one block was read");
}

#[tokio::test]
async fn a_big_top_index_takes_a_second_read_and_still_works() {
    // Over 256 KiB of top index: one page per stream, past what the tail read holds.
    let counting = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let mut builder = SegmentBuilder::with_limits(1024, 1);
    let bytes = Bytes::from(record(FIRST_ID, 1, 7, b"x"));
    for stream in 0..11_000 {
        builder
            .add_extent(&format!("s{stream}"), [bytes.clone()])
            .unwrap();
    }
    let built = builder.finish().unwrap();
    let path = Path::from("big.seg");
    put_parts(counting.as_ref(), &path, &built.parts).await;
    let store: Arc<dyn ObjectStore> = counting.clone();
    let reader = SegmentReader::new(store, path);
    let top = reader.read_top_index().await.unwrap();
    assert_eq!(top.page_count(), 11_000);
    assert_eq!(counting.gets(), 2);
    assert_eq!(reader.read_stream("s10999").await.unwrap(), bytes);
}

#[test]
fn a_small_segment_matches_its_byte_exact_vector() {
    // Built by an independent implementation of section 7.3 and D10, D52.
    let vector = include_str!("vectors/segment_small.hex").trim();
    let expected: Vec<u8> = (0..vector.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&vector[at..at + 2], 16).unwrap())
        .collect();
    let mut builder = SegmentBuilder::new();
    let bytes = Bytes::from(record(0x0001_0000_0000_0005, 0x10, 7, b"hello"));
    builder.add_extent("s", [bytes]).unwrap();
    assert_eq!(concat(&builder.finish().unwrap().parts), expected);
}
