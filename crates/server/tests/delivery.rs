//! Live delivery: the filter table, shared bytes, visibility, linger, `PROGRESS`, `BEHIND`
//! and fair sharing (SPEC section 5.2).

mod common;

use std::collections::BTreeMap;

use common::*;
use format::{Clause, Filter, RecordHeader, Records};
use proptest::prelude::*;
use proto::ErrorCode;
use server::{Event, Reply, StateOp, StateOpRequest, Subscription};

/// A stream id or cursor name's key hash under the all-zero key that `NodeConfig::new` gives.
fn key(name: &str) -> format::KeyHash {
    format::IdentityKey::new([0; 32]).hash(name)
}

const STREAM: &str = "live.stream";

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .unwrap()
}

fn clause_strategy() -> impl Strategy<Value = Clause> {
    prop_oneof![
        (any::<u8>(), any::<u8>()).prop_map(|(mask, value)| Clause::Eq {
            mask: u64::from(mask),
            value: u64::from(value & mask),
        }),
        any::<u8>().prop_map(|mask| Clause::Any {
            mask: u64::from(mask),
        }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(5))]

    #[test]
    fn ten_thousand_subscribers_over_three_filters_check_each_record_three_times(
        clauses in proptest::collection::vec(clause_strategy(), 3),
        options in proptest::collection::vec(any::<u8>(), 1..24),
        author_filter in any::<bool>(),
    ) {
        runtime().block_on(async move {
            let mut config = manual_config(4);
            config.max_subscriptions_per_connection = 20_000;
            let harness = Harness::new(config);
            let node = harness.node();
            let mut client = node.client(0).await;
            make_stream(&mut client, STREAM).await;

            // Three filters that can't be equal, whatever the clauses are.
            let filters: Vec<(Vec<Clause>, Option<u64>, u64)> = vec![
                (vec![clauses[0]], None, 0),
                (vec![clauses[1], Clause::Eq { mask: 1 << 40, value: 0 }], None, 1),
                (vec![clauses[2]], author_filter.then_some(AUTHOR.get()), 2),
            ];
            let mut connections = Vec::new();
            for home in 0..4 {
                connections.push((node.connection(home).await.unwrap(), Heard::default()));
            }
            for index in 0..10_000u32 {
                let (clauses, author, _) = &filters[index as usize % 3];
                let mut subscription = Subscription::new(index, key(STREAM), u64::MAX);
                subscription.clauses = clauses;
                subscription.author = *author;
                connections[index as usize % 4].0.subscribe(subscription);
            }
            for (connection, heard) in &mut connections {
                hear_until(connection, heard, |heard| {
                    heard.events.iter().filter(|event| matches!(event, Event::Subscribed { .. })).count() == 2_500
                }).await;
                heard.events.clear();
            }

            let data = [0x5A; 13];
            for (seq, one) in options.iter().enumerate() {
                let records = record_with(u64::from(*one), &data);
                client.publish(publish(STREAM, &records, seq as u64 + 1, 2)).await.unwrap();
                expect_ack(&mut client).await;
            }

            // What a naive check of each record picks, from the bytes in the WAL.
            let mut stored = Vec::new();
            for entry in harness.wal.stream_entries(STREAM) {
                stored.extend_from_slice(&entry.records);
            }
            let naive: Vec<Vec<u8>> = filters.iter().map(|(clauses, author, _)| {
                let mut filter = Filter::new(clauses).unwrap();
                filter.author = *author;
                let mut picked = Vec::new();
                for view in Records::new(&stored) {
                    let view = view.unwrap();
                    if filter.matches(&view.header) {
                        let mut one = Vec::new();
                        format::encode_record(&mut one, &view.header, view.data).unwrap();
                        picked.extend_from_slice(&one);
                    }
                }
                picked
            }).collect();
            let expected_bytes: u64 = (0..10_000usize).map(|index| naive[index % 3].len() as u64).sum();

            let mut received: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
            let mut received_bytes = 0;
            let give_up = std::time::Instant::now() + std::time::Duration::from_millis(30_000);
            'hearing: for (home, (connection, _)) in connections.iter_mut().enumerate() {
                let want: u64 = (0..10_000usize)
                    .filter(|index| index % 4 == home)
                    .map(|index| naive[index % 3].len() as u64)
                    .sum();
                let mut got = 0;
                while got < want {
                    if std::time::Instant::now() > give_up {
                        break 'hearing;
                    }
                    if let Some(Event::Data { sub, records }) = connection.recv_within(100).await {
                        got += records.len_bytes();
                        received.entry(sub).or_default().extend_from_slice(&records.to_vec());
                    }
                }
                received_bytes += got;
            }
            assert_eq!(received_bytes, expected_bytes);
            for index in 0..10_000u32 {
                let got = received.get(&index).map_or(&[][..], Vec::as_slice);
                assert_eq!(got, &naive[index as usize % 3][..], "subscriber {index}");
            }

            let checks = total_stat(node, |stats| stats.filter_check_count).await;
            assert_eq!(checks, options.len() as u64 * 3);
            drop(connections);
            drop(client);
            harness.finish();
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn received_bytes_are_the_stored_bytes_and_the_same_memory() {
    let harness = Harness::new(manual_config(2));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut near = node.connection(0).await.unwrap();
    let mut far = node.connection(1).await.unwrap();
    let (mut near_heard, mut far_heard) = (Heard::default(), Heard::default());
    near.subscribe(Subscription::new(1, key(STREAM), u64::MAX));
    far.subscribe(Subscription::new(2, key(STREAM), u64::MAX));
    subscribed(&mut near, &mut near_heard, 1).await;
    subscribed(&mut far, &mut far_heard, 2).await;

    let mut records = Vec::new();
    add_record(&mut records, 7, b"first, with odd length", 1);
    add_record(&mut records, 8, b"second", 3);
    client
        .publish(publish(STREAM, &records, 1, 2))
        .await
        .unwrap();
    let (_, first_id, count) = expect_ack(&mut client).await;
    assert_eq!(count, 4);

    let frame = |heard: &Heard, sub| {
        heard.events.iter().find_map(|event| match event {
            Event::Data { sub: to, records } if *to == sub => Some(records.clone()),
            _ => None,
        })
    };
    hear_until(&mut near, &mut near_heard, |heard| {
        frame(heard, 1).is_some()
    })
    .await;
    hear_until(&mut far, &mut far_heard, |heard| frame(heard, 2).is_some()).await;
    let near_frame = frame(&near_heard, 1).unwrap();
    let far_frame = frame(&far_heard, 2).unwrap();

    // Byte for byte what the WAL holds, and what went in, with only id and author stamped.
    let stored = harness.wal.stream_entries(STREAM);
    assert_eq!(near_frame.to_vec(), &stored[0].records[..]);
    assert_eq!(far_frame.to_vec(), &stored[0].records[..]);
    let parsed = parse_records(&near_frame.to_vec());
    assert_eq!(parsed[0].0, first_id);
    assert_eq!(parsed[1].1, first_id + 3);
    assert_eq!(parsed[0].3, b"first, with odd length");

    // The very memory the core's own log holds, on either home core: no copy anywhere.
    let mut held = Vec::new();
    for core in 0..2 {
        held.extend(node.read_memory(core, key(STREAM)).await);
    }
    assert_eq!(held.len(), 1);
    assert_eq!(near_frame.parts().len(), 1);
    assert_eq!(near_frame.parts()[0].as_bytes().as_ptr(), held[0].as_ptr());
    assert_eq!(far_frame.parts()[0].as_bytes().as_ptr(), held[0].as_ptr());
    drop((near, far, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn level_2_waits_for_the_fsync_and_a_level_1_behind_it_waits_too() {
    let harness = Harness::new(manual_config(1));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, key(STREAM), u64::MAX));
    subscribed(&mut connection, &mut heard, 1).await;

    // Level 1 on its own is visible at once, with the WAL held.
    harness.wal.hold();
    let records = record_with(1, b"level 1, alone");
    client
        .publish(publish(STREAM, &records, 1, 1))
        .await
        .unwrap();
    expect_ack(&mut client).await;
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() == 1
    })
    .await;

    let records = record_with(2, b"level 2");
    client
        .publish(publish(STREAM, &records, 2, 2))
        .await
        .unwrap();
    let records = record_with(3, b"level 1, behind it");
    client
        .publish(publish(STREAM, &records, 3, 1))
        .await
        .unwrap();
    // The level 1 is acked, since it asked for memory only, and still not delivered.
    let (seq, _, _) = expect_ack(&mut client).await;
    assert_eq!(seq, 3);
    hear_for(&mut connection, &mut heard, QUIET_MS).await;
    assert_eq!(
        heard.records(1).len(),
        1,
        "nothing passes a pending level 2"
    );
    expect_quiet(&mut client).await;

    harness.wal.release();
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() == 3
    })
    .await;
    let got = heard.records(1);
    assert_eq!(got[1].3, b"level 2");
    assert_eq!(got[2].3, b"level 1, behind it");
    assert!(got[0].0 < got[1].0 && got[1].0 < got[2].0);
    expect_ack(&mut client).await;
    drop((connection, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn level_3_is_delivered_before_the_upload_finishes() {
    let harness = Harness::new(manual_config(1));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, key(STREAM), u64::MAX));
    subscribed(&mut connection, &mut heard, 1).await;

    harness.store.hold();
    let records = record_with(1, b"level 3");
    client
        .publish(publish(STREAM, &records, 1, 3))
        .await
        .unwrap();
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() == 1
    })
    .await;
    // It is in the WAL and in the subscriber's hands, and not in the store.
    assert_eq!(
        harness
            .store
            .put_count
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    expect_quiet(&mut client).await;

    let seal = {
        let node_seal = node.seal();
        harness.store.release();
        node_seal.await
    };
    assert!(matches!(seal, server::SealOutcome::Uploaded { .. }));
    expect_ack(&mut client).await;
    drop((connection, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn linger_holds_records_until_its_timer_or_a_full_frame() {
    let mut config = manual_config(1);
    config.max_frame_bytes = 1024;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let mut subscription = Subscription::new(1, key(STREAM), u64::MAX);
    subscription.linger_ms = 200;
    connection.subscribe(subscription);
    subscribed(&mut connection, &mut heard, 1).await;
    heard.events.clear();

    let started = std::time::Instant::now();
    for seq in 1..=3 {
        let records = record_with(1, b"small");
        client
            .publish(publish(STREAM, &records, seq, 1))
            .await
            .unwrap();
        expect_ack(&mut client).await;
    }
    hear_for(&mut connection, &mut heard, 50).await;
    assert!(heard.events.is_empty(), "the timer hasn't fired");
    hear_until(&mut connection, &mut heard, |heard| {
        !heard.events.is_empty()
    })
    .await;
    assert!(started.elapsed().as_millis() >= 150);
    assert_eq!(heard.events.len(), 1, "one frame");
    assert_eq!(heard.records(1).len(), 3);

    // A queue reaching the frame size doesn't wait for the timer.
    heard.events.clear();
    let started = std::time::Instant::now();
    let big = vec![9; 400];
    for seq in 4..=7 {
        let records = record_with(1, &big);
        client
            .publish(publish(STREAM, &records, seq, 1))
            .await
            .unwrap();
        expect_ack(&mut client).await;
    }
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(1).len() >= 2
    })
    .await;
    assert!(
        started.elapsed().as_millis() < 150,
        "{:?}",
        started.elapsed()
    );
    drop((connection, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn progress_follows_skipped_records_at_most_once_per_interval_and_loses_nothing() {
    let mut config = manual_config(1);
    config.progress_interval_ms = 200;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    let rare = [Clause::Eq {
        mask: 0xFF,
        value: 0x42,
    }];
    let mut subscription = Subscription::new(1, key(STREAM), u64::MAX);
    subscription.clauses = &rare;
    connection.subscribe(subscription);
    subscribed(&mut connection, &mut heard, 1).await;

    let started = std::time::Instant::now();
    let mut seq = 0;
    let mut last_id = 0;
    while started.elapsed().as_millis() < 1_000 {
        seq += 1;
        let records = record_with(1, b"not for it");
        client
            .publish(publish(STREAM, &records, seq, 1))
            .await
            .unwrap();
        last_id = expect_ack(&mut client).await.1;
        hear_for(&mut connection, &mut heard, 5).await;
    }
    hear_for(&mut connection, &mut heard, 300).await;
    let elapsed_ms = started.elapsed().as_millis() as u64;
    let progress = heard.progress(1);
    assert!(!progress.is_empty(), "skipped records earn a PROGRESS");
    assert!(
        progress.len() as u64 <= elapsed_ms / 200 + 1,
        "{} in {elapsed_ms} ms",
        progress.len()
    );
    assert!(progress.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(
        *progress.last().unwrap(),
        last_id,
        "ends at the head, in the end"
    );
    assert!(heard.records(1).is_empty());

    // A match after it, and resuming from it, misses nothing.
    let resume_from = progress[progress.len() / 2];
    let records = record_with(0x42, b"for it");
    client
        .publish(publish(STREAM, &records, seq + 1, 1))
        .await
        .unwrap();
    let match_id = expect_ack(&mut client).await.1;
    let mut again = node.connection(0).await.unwrap();
    let mut heard_again = Heard::default();
    let mut subscription = Subscription::new(2, key(STREAM), resume_from);
    subscription.clauses = &rare;
    again.subscribe(subscription);
    hear_until(&mut again, &mut heard_again, |heard| {
        heard.records(2).len() == 1
    })
    .await;
    assert_eq!(heard_again.records(2)[0].0, match_id);
    drop((connection, again, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_subscriber_gets_behind_and_resumes_and_nobody_else_notices() {
    let mut config = manual_config(2);
    config.subscriber_queue_bytes = 64 * 1024;
    config.connection_window_bytes = 16 * 1024;
    config.fair_share_bytes = 4 * 1024;
    config.catch_up_read_bytes = 8 * 1024;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut slow = node.connection(1).await.unwrap();
    let mut fast = node.connection(1).await.unwrap();
    let (mut slow_heard, mut fast_heard) = (Heard::default(), Heard::default());
    slow.subscribe(Subscription::new(1, key(STREAM), u64::MAX));
    fast.subscribe(Subscription::new(2, key(STREAM), u64::MAX));
    subscribed(&mut slow, &mut slow_heard, 1).await;
    subscribed(&mut fast, &mut fast_heard, 2).await;

    // The slow connection isn't read from here on.
    let total_count = 512u64;
    let data = vec![7u8; 1024];
    let mut ids = Vec::new();
    let started = std::time::Instant::now();
    for seq in 1..=total_count {
        let records = record_with(seq, &data);
        client
            .publish(publish(STREAM, &records, seq, 2))
            .await
            .unwrap();
        ids.push(expect_ack(&mut client).await.1);
        hear_until(&mut fast, &mut fast_heard, |heard| {
            heard.records(2).len() as u64 >= seq
        })
        .await;
    }
    assert!(
        started.elapsed().as_secs() < 8,
        "every publish was acked and delivered while one client wasn't reading"
    );
    let fast_ids: Vec<u64> = fast_heard
        .records(2)
        .iter()
        .map(|record| record.0)
        .collect();
    assert_eq!(fast_ids, ids);
    assert!(
        fast_heard
            .events
            .iter()
            .all(|event| !matches!(event, Event::Behind { .. }))
    );
    assert_eq!(total_stat(node, |stats| stats.behind_count).await, 1);

    // Now it reads: what it was given, BEHIND, and then everything after that.
    hear_until(&mut slow, &mut slow_heard, |heard| {
        heard
            .records(1)
            .last()
            .is_some_and(|last| last.0 == *ids.last().unwrap())
    })
    .await;
    let got: Vec<u64> = slow_heard
        .records(1)
        .iter()
        .map(|record| record.0)
        .collect();
    assert_eq!(got, ids, "no loss and no repeat");
    let resume_from = slow_heard
        .events
        .iter()
        .find_map(|event| match event {
            Event::Behind { resume_from, .. } => Some(*resume_from),
            _ => None,
        })
        .unwrap();
    let mut before_behind = true;
    for event in &slow_heard.events {
        match event {
            Event::Behind { .. } => before_behind = false,
            Event::Data { records, .. } => {
                let parsed = parse_records(&records.to_vec());
                assert_eq!(parsed.iter().all(|one| one.0 <= resume_from), before_behind);
            }
            _ => {}
        }
    }

    // It is live again.
    let records = record_with(1, b"after");
    client
        .publish(publish(STREAM, &records, total_count + 1, 2))
        .await
        .unwrap();
    expect_ack(&mut client).await;
    hear_until(&mut slow, &mut slow_heard, |heard| {
        heard.records(1).len() as u64 == total_count + 1
    })
    .await;
    drop((slow, fast, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_heavy_subscription_does_not_starve_a_light_one_on_its_connection() {
    let share_bytes = 8 * 1024;
    let window_bytes = 32 * 1024;
    let mut config = manual_config(1);
    config.subscriber_queue_bytes = 64 * 1024 * 1024;
    config.connection_window_bytes = window_bytes;
    config.fair_share_bytes = share_bytes;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, "heavy").await;
    make_stream(&mut client, "light").await;
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, key("heavy"), u64::MAX));
    connection.subscribe(Subscription::new(2, key("light"), u64::MAX));
    subscribed(&mut connection, &mut heard, 1).await;
    subscribed(&mut connection, &mut heard, 2).await;
    heard.events.clear();

    // With the connection stalled, the heavy one queues 4 MiB and the light one 64 KiB.
    let data = vec![1u8; 1024];
    for seq in 1..=4096u64 {
        let records = record_with(1, &data);
        client
            .publish(publish("heavy", &records, seq, 1))
            .await
            .unwrap();
        expect_ack(&mut client).await;
    }
    for seq in 1..=64u64 {
        let records = record_with(1, &data);
        client
            .publish(publish("light", &records, seq, 1))
            .await
            .unwrap();
        expect_ack(&mut client).await;
    }

    let light_total = 64 * (1024 + 32);
    let mut light_bytes = 0u64;
    let mut heavy_bytes = 0u64;
    while light_bytes < light_total {
        match connection
            .recv_within(WAIT_MS)
            .await
            .expect("frames should arrive")
        {
            Event::Data { sub: 1, records } => heavy_bytes += records.len_bytes(),
            Event::Data { sub: 2, records } => light_bytes += records.len_bytes(),
            other => panic!("{other:?}"),
        }
        // What was in flight before the light one queued, plus an even share after that.
        assert!(
            heavy_bytes <= window_bytes + light_bytes + 2 * share_bytes,
            "heavy {heavy_bytes}, light {light_bytes}"
        );
    }
    assert!(
        heavy_bytes < 4 * 1024 * 1024 / 8,
        "{heavy_bytes} of 4 MiB went first"
    );
    drop((connection, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_refuses_what_it_should() {
    let mut config = manual_config(1);
    config.max_subscriptions_per_connection = 1;
    let harness = Harness::new(config);
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let mut connection = node.connection(0).await.unwrap();
    let mut heard = Heard::default();

    connection.subscribe(Subscription::new(1, key("no.such.stream"), 0));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.error_code(1).is_some()
    })
    .await;
    assert_eq!(heard.error_code(1), Some(ErrorCode::NotFound));

    let too_many = [Clause::Any { mask: 1 }; 5];
    let mut subscription = Subscription::new(2, key(STREAM), 0);
    subscription.clauses = &too_many;
    connection.subscribe(subscription);
    hear_until(&mut connection, &mut heard, |heard| {
        heard.error_code(2).is_some()
    })
    .await;
    assert_eq!(heard.error_code(2), Some(ErrorCode::Malformed));

    connection.subscribe(Subscription::new(3, key(STREAM), u64::MAX));
    subscribed(&mut connection, &mut heard, 3).await;
    connection.subscribe(Subscription::new(3, key(STREAM), u64::MAX));
    connection.subscribe(Subscription::new(4, key(STREAM), u64::MAX));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.error_code(4).is_some()
    })
    .await;
    assert_eq!(heard.error_code(4), Some(ErrorCode::TooManySubscriptions));
    assert_eq!(heard.error_code(3), Some(ErrorCode::Malformed));

    // An unsubscribed subscription gets nothing more.
    connection.unsubscribe(3);
    connection.subscribe(Subscription::new(5, key(STREAM), u64::MAX));
    subscribed(&mut connection, &mut heard, 5).await;
    let records = record_with(1, b"x");
    client
        .publish(publish(STREAM, &records, 1, 1))
        .await
        .unwrap();
    expect_ack(&mut client).await;
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(5).len() == 1
    })
    .await;
    assert!(heard.records(3).is_empty());
    drop((connection, client));
    harness.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn system_records_come_only_if_asked_and_state_flags_do_what_they_say() {
    let harness = Harness::new(manual_config(2));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let doc = cbor(&map(&[("a", int(1))]));
    client
        .state_op(StateOpRequest {
            stream: key(STREAM),
            req: 1,
            author: AUTHOR,
            expect_ver: 0,
            op: StateOp::Set,
            doc: doc.clone(),
        })
        .await
        .unwrap();
    let Reply::StateResult { ok: true, ver, .. } = expect_reply(&mut client).await else {
        panic!("expected a state result");
    };

    let mut connection = node.connection(1).await.unwrap();
    let mut heard = Heard::default();
    connection.subscribe(Subscription::new(1, key(STREAM), 0));
    let mut subscription = Subscription::new(2, key(STREAM), 0);
    subscription.include_system = true;
    subscription.with_state = true;
    connection.subscribe(subscription);
    let mut subscription = Subscription::new(3, key(STREAM), u64::MAX);
    subscription.state_only = true;
    connection.subscribe(subscription);
    for sub in 1..=3 {
        subscribed(&mut connection, &mut heard, sub).await;
    }
    let state_of = |heard: &Heard, sub| {
        heard.events.iter().find_map(|event| match event {
            Event::Subscribed {
                sub: to,
                state_ver,
                state,
                ..
            } if *to == sub => Some((*state_ver, state.clone())),
            _ => None,
        })
    };
    assert_eq!(state_of(&heard, 1).unwrap(), (ver, bytes::Bytes::new()));
    assert_eq!(state_of(&heard, 2).unwrap(), (ver, doc.clone()));
    assert_eq!(state_of(&heard, 3).unwrap(), (ver, doc));
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(2).len() == 1
    })
    .await;
    assert_eq!(heard.records(2)[0].0, ver);

    let new_doc = cbor(&map(&[("a", int(2))]));
    client
        .state_op(StateOpRequest {
            stream: key(STREAM),
            req: 2,
            author: AUTHOR,
            expect_ver: ver,
            op: StateOp::Set,
            doc: new_doc.clone(),
        })
        .await
        .unwrap();
    let Reply::StateResult {
        ok: true,
        ver: new_ver,
        ..
    } = expect_reply(&mut client).await
    else {
        panic!("expected a state result");
    };
    let records = record_with(1, b"record");
    client
        .publish(publish(STREAM, &records, 1, 2))
        .await
        .unwrap();
    expect_ack(&mut client).await;
    hear_until(&mut connection, &mut heard, |heard| {
        heard.records(2).len() == 3
            && heard.records(1).len() == 1
            && heard
                .events
                .iter()
                .any(|event| matches!(event, Event::State { .. }))
    })
    .await;
    assert!(heard.records(1).iter().all(|record| record.3 == b"record"));
    assert!(heard.records(3).is_empty(), "STATE_ONLY gets no records");
    let (state_ver, state_doc) = heard
        .events
        .iter()
        .find_map(|event| match event {
            Event::State { sub: 3, ver, doc } => Some((*ver, doc.clone())),
            _ => None,
        })
        .unwrap();
    assert_eq!((state_ver, state_doc), (new_ver, new_doc));
    let system = RecordHeader::read(&heard_system_bytes(&heard)).unwrap();
    assert!(system.is_system());
    drop((connection, client));
    harness.finish();
}

fn heard_system_bytes(heard: &Heard) -> Vec<u8> {
    heard
        .events
        .iter()
        .find_map(|event| match event {
            Event::Data { sub: 2, records } => Some(records.to_vec()),
            _ => None,
        })
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_move_keeps_live_delivery_in_order_with_nothing_lost() {
    let harness = Harness::new(manual_config(3));
    let node = harness.node();
    let mut client = node.client(0).await;
    make_stream(&mut client, STREAM).await;
    let shard = server::shard_of(key(STREAM), node.virtual_shard_count());
    let mut connections = Vec::new();
    for home in 0..3 {
        let mut connection = node.connection(home).await.unwrap();
        let mut heard = Heard::default();
        connection.subscribe(Subscription::new(1, key(STREAM), u64::MAX));
        subscribed(&mut connection, &mut heard, 1).await;
        connections.push((connection, heard));
    }
    // The WAL held across moves: what waits for it has to be delivered ahead of the rest.
    harness.wal.hold();
    let mut count = 0u64;
    for round in 0..9usize {
        for _ in 0..4 {
            count += 1;
            let records = record_with(count, &count.to_le_bytes());
            client
                .publish(publish(STREAM, &records, count, (count % 3) as u8 + 1))
                .await
                .unwrap();
        }
        assert!(node.move_shard(shard, round % 3).await);
        if round == 4 {
            harness.wal.release();
        }
    }
    harness.wal.release();
    for (connection, heard) in &mut connections {
        hear_until(connection, heard, |heard| {
            heard.records(1).len() as u64 == count
        })
        .await;
        let got = heard.records(1);
        assert!(got.windows(2).all(|pair| pair[0].0 < pair[1].0), "id order");
        for (index, record) in got.iter().enumerate() {
            assert_eq!(record.2, index as u64 + 1, "publish order");
        }
    }
    // A seal after moves still works, and is what the level 3 acks wait for. The stream has
    // stays on several cores and a seal takes one per core, so this can take a few.
    let mut acked = 0;
    while acked < count {
        assert!(matches!(
            node.seal().await,
            server::SealOutcome::Uploaded { .. } | server::SealOutcome::Empty { .. }
        ));
        while client.recv_within(20).await.is_some() {
            acked += 1;
        }
    }
    connections.clear();
    drop(client);
    harness.finish();
}
