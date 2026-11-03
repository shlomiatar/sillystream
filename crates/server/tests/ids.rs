//! Ids across publishes (SPEC section 3): consecutive within one, in the order the SDK
//! sorted them, increasing across them, with no gaps inside and none reused.

mod common;

use common::*;
use format::Records;
use proptest::prelude::*;

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

/// `(options, data, count)` of one record.
type Item = (u64, Vec<u8>, u16);

/// What the SDK does: a stable sort by options.
fn sdk_records(items: &[Item]) -> Vec<u8> {
    let mut sorted: Vec<&Item> = items.iter().collect();
    sorted.sort_by_key(|(options, _, _)| *options);
    let mut out = Vec::new();
    for (options, data, count) in sorted {
        add_record(&mut out, *options, data, *count);
    }
    out
}

fn arb_item() -> impl Strategy<Value = Item> {
    (
        0u64..4,
        proptest::collection::vec(any::<u8>(), 0..40),
        prop_oneof![Just(1u16), 1u16..50, Just(u16::MAX)],
    )
}

async fn run_publishes(publishes: Vec<(usize, Vec<Item>)>) {
    let streams = ["a", "b", "c"];
    let harness = Harness::new(manual_config(3));
    let mut client = harness.node().client(0).await;
    let mut expected: Vec<Vec<Vec<u8>>> = vec![Vec::new(); streams.len()];
    let mut ranges: Vec<Vec<(u64, u32)>> = vec![Vec::new(); streams.len()];
    let mut last: Vec<u64> = vec![0; streams.len()];

    for (seq, (stream, items)) in publishes.into_iter().enumerate() {
        if items.is_empty() {
            continue;
        }
        let records = sdk_records(&items);
        client
            .publish(publish(streams[stream], &records, seq as u64 + 1, 1))
            .await
            .unwrap();
        let (_, first_id, count) = expect_ack(&mut client).await;
        let items_total: u64 = items.iter().map(|(_, _, count)| u64::from(*count)).sum();
        assert_eq!(u64::from(count), items_total);
        assert!(first_id > last[stream], "ids go up across publishes");
        last[stream] = first_id + items_total - 1;
        ranges[stream].push((first_id, count));
        expected[stream].push(records);
    }

    // Wait for the WAL: a level 2 publish after the others is durable after them.
    for (stream, name) in streams.iter().enumerate() {
        if ranges[stream].is_empty() {
            continue;
        }
        let empty = record_with(0, b"");
        client
            .publish(publish(name, &empty, 1 << 40, 2))
            .await
            .unwrap();
        expect_ack(&mut client).await;
        let entries = harness.wal.stream_entries(name);
        assert_eq!(entries.len(), ranges[stream].len() + 1);
        for (at, unstamped) in expected[stream].iter().enumerate() {
            let (first_id, _) = ranges[stream][at];
            let mut next_id = first_id;
            let stored = &entries[at].records;
            let want = Records::new(unstamped);
            let got = Records::new(stored);
            let mut record_count = 0;
            for (want, got) in want.zip(got) {
                let (want, got) = (want.unwrap(), got.unwrap());
                // Consecutive, in the sorted order, and nothing else changed.
                assert_eq!(got.header.id, next_id);
                assert_eq!(got.header.author, AUTHOR.get());
                assert_eq!(got.header.options, want.header.options);
                assert_eq!(got.header.count, want.header.count);
                assert_eq!(got.data, want.data);
                next_id += u64::from(got.header.count);
                record_count += 1;
            }
            assert_eq!(record_count, Records::new(unstamped).count());
            assert_eq!(stored.len(), unstamped.len());
        }
    }
    harness.finish();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn ids_are_consecutive_in_sdk_order_over_many_publishes(
        publishes in proptest::collection::vec(
            (0usize..3, proptest::collection::vec(arb_item(), 0..8)),
            1..40,
        ),
    ) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run_publishes(publishes));
    }
}

#[tokio::test]
async fn id_is_time_with_a_frozen_clock_borrowing_forward() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let mut records = Vec::new();
    add_record(&mut records, 1, b"", u16::MAX);
    add_record(&mut records, 1, b"", u16::MAX);
    client.publish(publish("s", &records, 1, 1)).await.unwrap();
    let (_, first_id, count) = expect_ack(&mut client).await;
    assert_eq!(format::id_ms(first_id), START_MS);
    assert_eq!(count, 2 * u32::from(u16::MAX));
    // The counter ran out, so the next one has borrowed from the next ms.
    client.publish(publish("s", &records, 2, 1)).await.unwrap();
    let (_, next_id, _) = expect_ack(&mut client).await;
    assert_eq!(next_id, first_id + u64::from(count));
    assert_eq!(format::id_ms(next_id), START_MS + 1);
    harness.finish();
}

#[tokio::test]
async fn a_publish_with_more_items_than_an_ack_can_count_is_rejected_whole() {
    // 65,537 records of 65,535 items is exactly u32::MAX (D34).
    let mut records = Vec::new();
    for _ in 0..65_537 {
        add_record(&mut records, 1, b"", u16::MAX);
    }
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;

    // The system record would be item u32::MAX + 1, so this one can't be counted.
    let mut with_state = publish("s", &records, 1, 2);
    with_state.state = Some(server::StateChange {
        expect_ver: 0,
        op: server::StateOp::Set,
        doc: cbor(&int(1)),
    });
    client.publish(with_state).await.unwrap();
    match expect_reply(&mut client).await {
        server::Reply::Nack { code, .. } => assert_eq!(code, proto::ErrorCode::Malformed),
        other => panic!("expected a nack, got {other:?}"),
    }
    assert_eq!(
        client.snapshot(key("s")).await,
        Err(proto::ErrorCode::NotFound)
    );

    // Without it, the same records fit exactly.
    client.publish(publish("s", &records, 1, 1)).await.unwrap();
    let (_, _, count) = expect_ack(&mut client).await;
    assert_eq!(count, u32::MAX);
    harness.finish();
}

#[tokio::test]
async fn malformed_publishes_are_rejected_whole() {
    let harness = Harness::new(manual_config(1));
    let mut client = harness.node().client(0).await;
    let malformed = |reply: server::Reply| {
        assert!(
            matches!(
                reply,
                server::Reply::Nack {
                    code: proto::ErrorCode::Malformed,
                    ..
                }
            ),
            "{reply:?}"
        );
    };
    // Neither records nor state (D6a).
    client.publish(publish("s", &[], 1, 2)).await.unwrap();
    malformed(expect_reply(&mut client).await);
    // Lengths that don't add up.
    let truncated = &record_with(1, b"hello")[..20];
    client.publish(publish("s", truncated, 1, 2)).await.unwrap();
    malformed(expect_reply(&mut client).await);
    assert_eq!(
        client.snapshot(key("s")).await,
        Err(proto::ErrorCode::NotFound)
    );
    harness.finish();
}
