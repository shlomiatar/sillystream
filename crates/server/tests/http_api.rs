//! Every route of section 11.2, D21 and D26, for success and each error it can return.

mod api;

use std::sync::Arc;

use api::*;
use proto::{AckLevel, ClientFrame};
use server::net::{Server, ServerConfig};
use storage::{WalFile, WalVolume};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const RECORDS: &str = "application/x-sillystream-records";
const STREAM: &str = "/v1/stream/s";

async fn seed(server: &TestServer, stream: &str, data: &[&[u8]]) -> Vec<u64> {
    let text = token(ALL);
    let mut ids = Vec::new();
    for bytes in data {
        let path = format!("/v1/stream/{stream}/publish");
        let reply = server
            .http(&Http::new("POST", &path, &text).body(*bytes))
            .await;
        assert_eq!(reply.status, 200, "{}", reply.text());
        ids.push(reply.field("first_id").unwrap().parse().unwrap());
    }
    ids
}

fn assert_code(reply: &HttpReply, status: u16, code: u16) {
    assert_eq!(
        (reply.status, reply.code()),
        (status, Some(code)),
        "{}",
        reply.text()
    );
}

/// For a route: no token, a bad token, an expired one, a wrong pattern and a missing
/// permission all fail with code 1 and leave nothing behind, and the right permission passes.
async fn auth_is_checked(
    server: &TestServer,
    method: &str,
    path_of: impl Fn(&str) -> String,
    needs: u8,
    body: &[u8],
) {
    let path = path_of("exists");
    let without_token = Http {
        method,
        path: &path,
        token: None,
        headers: Vec::new(),
        body: body.to_vec(),
    };
    assert_code(&server.http(&without_token).await, 401, 1);
    let cases = [
        ("garbage".to_owned(), 401),
        (
            auth::Token {
                expiry_ms: START_MS + 1000,
                author: std::num::NonZeroU64::new(1).unwrap(),
                grants: vec![auth::Grant {
                    pattern: "*".to_owned(),
                    perms: ALL,
                }],
            }
            .sign(b"somebody else's secret, long enough")
            .unwrap(),
            401,
        ),
        (token_at(START_MS, AUTHOR, &[("*", ALL)]), 401),
        (token_at(START_MS + 1000, AUTHOR, &[("other.*", ALL)]), 403),
        (token(ALL & !needs), 403),
    ];
    for (text, status) in cases {
        let reply = server
            .http(&Http::new(method, &path, &text).body(body.to_vec()))
            .await;
        assert_code(&reply, status, 1);
    }
    // Each single permission that isn't enough.
    for perm in [
        auth::PERM_READ,
        auth::PERM_PUBLISH,
        auth::PERM_STATE,
        auth::PERM_ADMIN,
    ] {
        if perm & needs != needs {
            let text = token(perm);
            let reply = server
                .http(&Http::new(method, &path, &text).body(body.to_vec()))
                .await;
            assert_code(&reply, 403, 1);
        }
    }
    let text = token(needs);
    let reply = server
        .http(&Http::new(method, &path, &text).body(body.to_vec()))
        .await;
    assert!(
        (200..300).contains(&reply.status),
        "{method} {path} with its permission: {} {}",
        reply.status,
        reply.text()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn every_route_checks_its_permission() {
    let server = start();
    seed(&server, "exists", &[b"x"]).await;
    let stream = |suffix: &'static str| move |name: &str| format!("/v1/stream/{name}{suffix}");
    auth_is_checked(&server, "GET", stream(""), auth::PERM_READ, b"").await;
    auth_is_checked(
        &server,
        "POST",
        stream("/publish"),
        auth::PERM_PUBLISH,
        b"x",
    )
    .await;
    auth_is_checked(&server, "GET", stream("/read"), auth::PERM_READ, b"").await;
    auth_is_checked(&server, "GET", stream("/state"), auth::PERM_READ, b"").await;
    let doc = cbor_map(&[("a", 1)]);
    auth_is_checked(&server, "PATCH", stream("/state"), auth::PERM_STATE, &doc).await;
    auth_is_checked(&server, "PUT", stream("/cursor/c"), auth::PERM_READ, b"1").await;
    auth_is_checked(&server, "PUT", stream(""), auth::PERM_ADMIN, b"{}").await;
    let sink = br#"{"kind":"archive"}"#;
    auth_is_checked(
        &server,
        "PUT",
        stream("/sink/1"),
        auth::PERM_ADMIN | auth::PERM_READ,
        sink,
    )
    .await;
    auth_is_checked(
        &server,
        "POST",
        stream("/sink/1/pause"),
        auth::PERM_ADMIN,
        b"",
    )
    .await;
    auth_is_checked(
        &server,
        "POST",
        stream("/sink/1/resume"),
        auth::PERM_ADMIN,
        b"",
    )
    .await;
    auth_is_checked(
        &server,
        "POST",
        stream("/sink/1/reset"),
        auth::PERM_ADMIN,
        b"",
    )
    .await;
    auth_is_checked(&server, "DELETE", stream("/sink/1"), auth::PERM_ADMIN, b"").await;
    auth_is_checked(&server, "DELETE", stream(""), auth::PERM_ADMIN, b"").await;
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_info_says_head_config_and_not_found() {
    let server = start();
    let text = token(ALL);
    let missing = server.http(&Http::new("GET", STREAM, &text)).await;
    assert_code(&missing, 404, 2);
    let ids = seed(&server, "s", &[b"one", b"two"]).await;
    let reply = server.http(&Http::new("GET", STREAM, &text)).await;
    assert_eq!(reply.status, 200);
    assert_eq!(reply.field("stream").unwrap(), "s");
    assert_eq!(reply.field("head").unwrap(), ids[1].to_string());
    assert_eq!(reply.field("retention").unwrap(), "1d");
    assert_eq!(reply.field("default_ack").unwrap(), "2");
    server.server.shutdown();
}

/// The form section 11.2 shows, header for header.
#[tokio::test(flavor = "multi_thread")]
async fn the_simple_publish_form_works_with_curl_equivalent_headers() {
    let server = start();
    let text = token(auth::PERM_PUBLISH | auth::PERM_READ);
    let request = Http::new("POST", "/v1/stream/logs.app/publish?ack=2", &text)
        .header("X-Options", "0x8000000000000000")
        .header("X-Producer-Id", "7")
        .header("X-Producer-Seq", "42")
        .header("Content-Type", "application/octet-stream")
        .body(&b"a line of a log"[..]);
    let reply = server.http(&request).await;
    assert_eq!(reply.status, 200, "{}", reply.text());
    let first_id: u64 = reply.field("first_id").unwrap().parse().unwrap();
    assert_eq!(reply.field("count").unwrap(), "1");
    assert_eq!(reply.field("producer_seq").unwrap(), "42");

    // A retry is the original ack, not a new record.
    let again = server.http(&request).await;
    assert_eq!(again.field("first_id").unwrap(), first_id.to_string());

    let read = server
        .http(&Http::new("GET", "/v1/stream/logs.app/read", &text))
        .await;
    assert_eq!(read.status, 200);
    assert_eq!(read.header("content-type"), Some(RECORDS));
    assert_eq!(
        parse_records(&read.body),
        [(first_id, 1 << 63, AUTHOR, b"a line of a log".to_vec())]
    );
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn x_options_reads_hex_and_decimal_and_refuses_the_rest() {
    let server = start();
    let text = token(ALL);
    let path = "/v1/stream/opts/publish";
    let send = async |value: &str| {
        server
            .http(
                &Http::new("POST", path, &text)
                    .header("X-Options", value)
                    .body(&b"d"[..]),
            )
            .await
    };
    assert_eq!(send("0xFF").await.status, 200);
    assert_eq!(send("255").await.status, 200);
    assert_code(&send("0xZZ").await, 400, 8);
    assert_code(&send("nope").await, 400, 8);
    let read = server
        .http(&Http::new("GET", "/v1/stream/opts/read", &text))
        .await;
    let options: Vec<u64> = parse_records(&read.body).iter().map(|r| r.1).collect();
    assert_eq!(options, [255, 255]);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn binary_publish_takes_records_and_rejects_what_section_3_rejects() {
    let server = start();
    let text = token(ALL);
    let path = "/v1/stream/bin/publish?ack=1";
    let send = async |body: Vec<u8>| {
        server
            .http(
                &Http::new("POST", path, &text)
                    .header("Content-Type", RECORDS)
                    .body(body),
            )
            .await
    };
    let mut two = record(1, b"a");
    two.extend(record(2, b"bb"));
    let reply = send(two).await;
    assert_eq!(reply.status, 200, "{}", reply.text());
    assert_eq!(reply.field("count").unwrap(), "2");

    let mut with_author = record(0, b"x");
    with_author[16] = 9;
    assert_code(&send(with_author).await, 400, 8);
    let mut with_id = record(0, b"x");
    with_id[0] = 9;
    assert_code(&send(with_id).await, 400, 8);
    let mut truncated = record(0, b"xxxxxxxxxxxxxxxx");
    truncated.truncate(40);
    assert_code(&send(truncated).await, 400, 8);
    assert_code(&send(Vec::new()).await, 400, 8);

    // Bad query and headers.
    let get = async |path: &str, headers: Vec<(&'static str, &'static str)>| {
        let mut request = Http::new("POST", path, &text).body(&b"d"[..]);
        request.headers = headers;
        server.http(&request).await
    };
    assert_code(&get("/v1/stream/bin/publish?ack=9", vec![]).await, 400, 8);
    assert_code(
        &get("/v1/stream/bin/publish", vec![("X-Producer-Id", "1")]).await,
        400,
        8,
    );
    assert_code(
        &get(
            "/v1/stream/bin/publish",
            vec![("X-Producer-Id", "x"), ("X-Producer-Seq", "1")],
        )
        .await,
        400,
        8,
    );
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_over_max_frame_is_error_6() {
    let mut node = node_config(1);
    node.max_frame_bytes = 4096;
    let server = start_node(node);
    let text = token(ALL);
    let path = "/v1/stream/big/publish";
    let fits = server
        .http(&Http::new("POST", path, &text).body(vec![1u8; 2000]))
        .await;
    assert_eq!(fits.status, 200);
    let over_body = server
        .http(&Http::new("POST", path, &text).body(vec![1u8; 5000]))
        .await;
    assert_code(&over_body, 413, 6);
    // The body fits, and its record with a header doesn't.
    let over_record = server
        .http(&Http::new("POST", path, &text).body(vec![1u8; 4090]))
        .await;
    assert_code(&over_record, 413, 6);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn level_0_is_answered_without_an_ack() {
    let server = start();
    let text = token(ALL);
    let reply = server
        .http(&Http::new("POST", "/v1/stream/zero/publish?ack=0", &text).body(&b"x"[..]))
        .await;
    assert_eq!(reply.status, 202);
    // A stream whose default is 0 must not leave a publish waiting for an ack that never comes.
    let config = server
        .http(&Http::new("PUT", "/v1/stream/zero", &text).body(&br#"{"default_ack":0}"#[..]))
        .await;
    assert_eq!(config.status, 200, "{}", config.text());
    let reply = server
        .http(&Http::new("POST", "/v1/stream/zero/publish", &text).body(&b"y"[..]))
        .await;
    assert_eq!(reply.status, 202);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_read_route_filters_limits_and_says_how_far_it_got() {
    let server = start();
    let text = token(ALL);
    let path = "/v1/stream/r/publish";
    let mut ids = Vec::new();
    for (options, data) in [(1u64, &b"a"[..]), (2, b"b"), (3, b"c"), (2, b"d")] {
        let value = options.to_string();
        let reply = server
            .http(
                &Http::new("POST", path, &text)
                    .header("X-Options", &value)
                    .body(data),
            )
            .await;
        ids.push(reply.field("first_id").unwrap().parse::<u64>().unwrap());
    }
    let read = async |query: &str| {
        let path = format!("/v1/stream/r/read{query}");
        server.http(&Http::new("GET", &path, &text)).await
    };
    let datas = |reply: &HttpReply| -> Vec<Vec<u8>> {
        parse_records(&reply.body)
            .into_iter()
            .map(|r| r.3)
            .collect()
    };

    let all = read("").await;
    assert_eq!(datas(&all).len(), 4);
    assert_eq!(all.header("x-head").unwrap(), ids[3].to_string());
    assert_eq!(all.header("x-count").unwrap(), "4");

    let limited = read("?limit=2").await;
    assert_eq!(datas(&limited), [b"a".to_vec(), b"b".to_vec()]);
    assert_eq!(limited.header("x-through").unwrap(), ids[1].to_string());

    let after = read(&format!("?from={}", ids[1])).await;
    assert_eq!(datas(&after), [b"c".to_vec(), b"d".to_vec()]);

    let eq = read("?filter=eq:0xFF:2").await;
    assert_eq!(datas(&eq), [b"b".to_vec(), b"d".to_vec()]);
    let any = read("?filter=any:1").await;
    assert_eq!(datas(&any), [b"a".to_vec(), b"c".to_vec()]);
    let either = read("?filter=eq:0xFF:1,eq:0xFF:3").await;
    assert_eq!(datas(&either), [b"a".to_vec(), b"c".to_vec()]);
    let author = read(&format!("?filter=author:{AUTHOR}")).await;
    assert_eq!(datas(&author).len(), 4);
    let nobody = read("?filter=author:12345").await;
    assert_eq!(datas(&nobody).len(), 0);
    assert_eq!(nobody.status, 200);

    // A read that matches nothing still says it looked as far as the head.
    assert_eq!(nobody.header("x-through").unwrap(), ids[3].to_string());

    let at_head = read(&format!("?from={}", ids[3])).await;
    assert_eq!(datas(&at_head).len(), 0);
    let live_only = read(&format!("?from={}", u64::MAX)).await;
    assert_eq!(datas(&live_only).len(), 0);

    assert_code(&read("?from=x").await, 400, 8);
    assert_code(&read("?limit=0").await, 400, 8);
    assert_code(&read("?filter=wat").await, 400, 8);
    let missing = server
        .http(&Http::new("GET", "/v1/stream/none/read", &text))
        .await;
    assert_code(&missing, 404, 2);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_before_retention_is_error_7() {
    let server = start();
    let text = token_at(u64::MAX, AUTHOR, &[("*", ALL)]);
    let ids = seed(&server, "old", &[b"gone"]).await;
    server.clock.advance(3 * 86_400_000);
    let path = format!("/v1/stream/old/read?from={}", ids[0] - 1);
    let reply = server.http(&Http::new("GET", &path, &text)).await;
    assert_code(&reply, 410, 7);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn state_get_and_patch_with_if_match() {
    let server = start();
    let text = token(ALL);
    let path = "/v1/stream/st/state";
    let missing = server.http(&Http::new("GET", path, &text)).await;
    assert_code(&missing, 404, 2);

    // A state write creates the stream (section 13).
    let reply = server
        .http(
            &Http::new("PATCH", path, &text)
                .header("X-State-Op", "set")
                .body(cbor_map(&[("a", 1)])),
        )
        .await;
    assert_eq!(reply.status, 200, "{}", reply.text());
    assert_eq!(reply.field("ok").unwrap(), "true");
    let version = reply.field("ver").unwrap();

    let got = server.http(&Http::new("GET", path, &text)).await;
    assert_eq!(got.status, 200);
    assert_eq!(got.header("content-type"), Some("application/cbor"));
    assert_eq!(got.header("x-state-ver").unwrap(), version);
    assert_eq!(got.header("x-head").unwrap(), version);
    assert_eq!(got.body, cbor_map(&[("a", 1)]));

    // Merge is what PATCH does without saying.
    let quoted_version = format!("\"{version}\"");
    let reply = server
        .http(
            &Http::new("PATCH", path, &text)
                .header("If-Match", &quoted_version)
                .body(cbor_map(&[("b", 2)])),
        )
        .await;
    assert_eq!(reply.status, 200, "{}", reply.text());
    let second = reply.field("ver").unwrap();

    // The first version is out of date now.
    let conflict = server
        .http(
            &Http::new("PATCH", path, &text)
                .header("If-Match", &version)
                .body(cbor_map(&[("c", 3)])),
        )
        .await;
    assert_code(&conflict, 409, 3);
    assert_eq!(conflict.field("ok").unwrap(), "false");
    let got = server.http(&Http::new("GET", path, &text)).await;
    assert_eq!(
        got.header("x-state-ver").unwrap(),
        second,
        "nothing changed"
    );
    assert_eq!(got.body, cbor_map(&[("a", 1), ("b", 2)]));

    // Incr, by name and by number.
    use ciborium::value::Value;
    let incr = cbor(&Value::Map(vec![
        (
            Value::Text("path".into()),
            Value::Array(vec![Value::Text("a".into())]),
        ),
        (Value::Text("n".into()), Value::Integer(5.into())),
    ]));
    for name in ["incr", "3"] {
        let reply = server
            .http(
                &Http::new("PATCH", path, &text)
                    .header("X-State-Op", name)
                    .body(incr.clone()),
            )
            .await;
        assert_eq!(reply.status, 200, "{}", reply.text());
    }
    let got = server.http(&Http::new("GET", path, &text)).await;
    assert_eq!(got.body, cbor_map(&[("a", 11), ("b", 2)]));

    // And what it refuses.
    let bad_op = server
        .http(
            &Http::new("PATCH", path, &text)
                .header("X-State-Op", "swap")
                .body(cbor_map(&[])),
        )
        .await;
    assert_code(&bad_op, 400, 8);
    let bad_match = server
        .http(
            &Http::new("PATCH", path, &text)
                .header("If-Match", "soon")
                .body(cbor_map(&[])),
        )
        .await;
    assert_code(&bad_match, 400, 8);
    let not_cbor = server
        .http(&Http::new("PATCH", path, &text).body(vec![0xFF, 0xFF]))
        .await;
    assert_code(&not_cbor, 400, 8);
    let over = server
        .http(
            &Http::new("PATCH", path, &text)
                .header("X-State-Op", "set")
                .body(cbor(&Value::Bytes(vec![0; 70_000]))),
        )
        .await;
    assert_code(&over, 400, 8);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reset_above_the_head_is_refused() {
    let server = start();
    let text = token(ALL);
    let ids = seed(&server, "c", &[b"1"]).await;
    let path = "/v1/stream/c/cursor/mine?reset=true";
    let reply = server
        .http(&Http::new("PUT", path, &text).body((ids[0] + 1).to_string().into_bytes()))
        .await;
    assert_eq!(reply.status, 400, "{}", reply.text());
    server.server.shutdown();
}

/// `read` can see a sink's health, not what its errors say about the destination (D188).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_admin_sees_a_sinks_error_text() {
    let mut node = node_config(2);
    node.sink.pool_thread_count = 2;
    let server = start_node(node);
    let text = token(ALL);
    let reader = token(auth::PERM_READ);
    seed(&server, "h", &[b"1"]).await;
    // Nothing listens here, and the worker's report is what puts the text in the status.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let body = format!(
        r#"{{"kind":"webhook.deliver","url":"http://{address}/x","secret":"s","start":"earliest"}}"#
    );
    let created = server
        .http(&Http::new("PUT", "/v1/stream/h/sink/1", &text).body(body.into_bytes()))
        .await;
    assert_eq!(created.status, 200, "{}", created.text());
    let started = std::time::Instant::now();
    let info = loop {
        let info = server
            .http(&Http::new("GET", "/v1/stream/h", &text))
            .await
            .text();
        if !info.contains("\"last_error\":null") || !info.contains("\"pause_reason\":null") {
            break info;
        }
        assert!(started.elapsed().as_secs() < 10, "{info}");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    };
    assert!(!info.contains("\"hidden\""), "{info}");
    let seen = server
        .http(&Http::new("GET", "/v1/stream/h", &reader))
        .await
        .text();
    assert!(seen.contains("\"hidden\""), "{seen}");
    assert!(!seen.contains(&address.ip().to_string()), "{seen}");
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursors_move_forward_and_reset_goes_back() {
    let server = start();
    let text = token(ALL);
    let ids = seed(&server, "c", &[b"1", b"2", b"3"]).await;
    let put = async |path: &str, body: String| {
        server
            .http(&Http::new("PUT", path, &text).body(body.into_bytes()))
            .await
    };
    let path = "/v1/stream/c/cursor/mine";
    let reply = put(path, ids[1].to_string()).await;
    assert_eq!(reply.status, 200, "{}", reply.text());
    assert_eq!(reply.field("id").unwrap(), ids[1].to_string());
    // Lower is ignored, and it says where the cursor stayed.
    let reply = put(path, format!("{{\"id\":\"{}\"}}", ids[0])).await;
    assert_eq!(reply.field("id").unwrap(), ids[1].to_string());
    // Reset goes back.
    let reply = put("/v1/stream/c/cursor/mine?reset=true", ids[0].to_string()).await;
    assert_eq!(reply.field("id").unwrap(), ids[0].to_string());
    // A subscriber named `mine` starts there.
    let mut ws = server.ws(&text).await;
    ws.send(&ClientFrame::Subscribe(proto::Subscribe {
        sub: 1,
        from: 0,
        linger_ms: 0,
        author: None,
        clauses: proto::WireClauses::default(),
        stream: "c",
        name: Some("mine"),
        with_state: false,
        state_only: false,
        include_system: false,
    }))
    .await;
    assert!(matches!(ws.recv_owned().await, Owned::Subscribed { .. }));
    let Owned::Data { records, .. } = ws.recv_owned().await else {
        panic!();
    };
    assert_eq!(parse_records(&records)[0].0, ids[1]);

    // Above the head, not a number, a sink's own name, and a stream that isn't there.
    assert_code(&put(path, (ids[2] + 100).to_string()).await, 400, 8);
    assert_code(&put(path, "soon".to_owned()).await, 400, 8);
    assert_code(
        &put("/v1/stream/c/cursor/sink:1", "1".to_owned()).await,
        400,
        8,
    );
    assert_code(
        &put("/v1/stream/none/cursor/x", "1".to_owned()).await,
        404,
        2,
    );
    drop(ws);
    server.server.shutdown();
}

/// A read-only token can't grow a node's memory without limit (D218).
#[tokio::test(flavor = "multi_thread")]
async fn a_read_token_is_limited_in_cursors_and_their_names() {
    let server = start();
    let ids = seed(&server, "c", &[b"1"]).await;
    let text = token(auth::PERM_READ);
    let put = async |name: &str| {
        server
            .http(
                &Http::new("PUT", &format!("/v1/stream/c/cursor/{name}"), &text)
                    .body(ids[0].to_string().into_bytes()),
            )
            .await
    };
    let longest = "n".repeat(server::CURSOR_NAME_MAX_BYTES);
    assert_eq!(put(&longest).await.status, 200);
    assert_code(&put(&format!("{longest}n")).await, 400, 8);
    for at in 1..server::NAMED_CURSOR_CAP_COUNT {
        let reply = put(&format!("c{at}")).await;
        assert_eq!(reply.status, 200, "{at}: {}", reply.text());
    }
    assert_code(&put("one_more").await, 429, 5);
    // One already there still moves.
    assert_eq!(put(&longest).await.status, 200);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_put_creates_and_changes_config() {
    let server = start();
    let text = token(ALL);
    let path = "/v1/stream/cfg";
    let put = async |body: &str| {
        server
            .http(&Http::new("PUT", path, &text).body(body.as_bytes().to_vec()))
            .await
    };
    let created = put("").await;
    assert_eq!(created.status, 200, "{}", created.text());
    assert_eq!(created.field("retention").unwrap(), "1d");
    let info = server.http(&Http::new("GET", path, &text)).await;
    assert_eq!(info.status, 200, "explicit creation");

    let changed = put(r#"{"retention":"30d","min_ack":1}"#).await;
    assert_eq!(changed.field("retention").unwrap(), "30d");
    assert_eq!(changed.field("min_ack").unwrap(), "1");
    assert_eq!(
        changed.field("default_ack").unwrap(),
        "2",
        "what isn't set stays"
    );

    assert_code(&put(r#"{"retention":"2d"}"#).await, 400, 8);
    assert_code(&put(r#"{"min_ack":9}"#).await, 400, 8);
    assert_code(&put(r#"{"nope":1}"#).await, 400, 8);
    assert_code(&put("not json").await, 400, 8);
    assert_code(&put("[1]").await, 400, 8);

    // A minimum takes effect: a level 0 publish is raised, and it is acked through the WS.
    let mut ws = server.ws(&text).await;
    ws.open(1, "cfg").await;
    ws.publish(1, 1, AckLevel::None, &record(0, b"raised"))
        .await;
    assert!(matches!(ws.recv_owned().await, Owned::Ack { .. }));
    drop(ws);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_delete_ends_subscribers_and_a_new_stream_starts_above() {
    let server = start();
    let text = token(ALL);
    let path = "/v1/stream/del";
    assert_code(
        &server.http(&Http::new("DELETE", path, &text)).await,
        404,
        2,
    );

    let ids = seed(&server, "del", &[b"old"]).await;
    let mut reader = server.ws(&text).await;
    reader.subscribe(1, "del", u64::MAX).await;
    assert!(matches!(
        reader.recv_owned().await,
        Owned::Subscribed { .. }
    ));

    let deleted = server.http(&Http::new("DELETE", path, &text)).await;
    assert_eq!(deleted.status, 200);
    assert_eq!(
        reader.recv_owned().await,
        Owned::Error {
            reference: 1,
            code: proto::ErrorCode::NotFound
        }
    );
    assert_code(&server.http(&Http::new("GET", path, &text)).await, 404, 2);

    // The clock hasn't moved, so only the rule keeps these apart.
    let new_ids = seed(&server, "del", &[b"new"]).await;
    assert!(new_ids[0] > ids[0], "{new_ids:?} {ids:?}");
    let read = server
        .http(&Http::new("GET", "/v1/stream/del/read?from=0", &text))
        .await;
    let datas: Vec<Vec<u8>> = parse_records(&read.body).into_iter().map(|r| r.3).collect();
    assert_eq!(datas, [b"new".to_vec()], "old records can't be read");

    // A subscriber on the new stream hears it live, past the old one's numbering.
    reader.subscribe(2, "del", u64::MAX).await;
    assert!(matches!(
        reader.recv_owned().await,
        Owned::Subscribed { sub: 2, .. }
    ));
    seed(&server, "del", &[b"newer"]).await;
    loop {
        if let Owned::Data { sub: 2, records } = reader.recv_owned().await {
            assert_eq!(parse_records(&records)[0].3, b"newer");
            break;
        }
    }
    drop(reader);
    server.server.shutdown();
}

/// With the pool running: what a route creates delivers, and the status shows, secret unshown.
#[tokio::test(flavor = "multi_thread")]
async fn a_sink_made_over_http_runs_and_reports_why_it_paused() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut bytes = [0u8; 4096];
                let _ = socket.read(&mut bytes).await;
                let _ = socket
                    .write_all(b"HTTP/1.1 410 Gone\r\nContent-Length: 0\r\n\r\n")
                    .await;
            });
        }
    });
    let mut node = node_config(2);
    node.sink.pool_thread_count = 2;
    node.sink.allow_private_addresses = true;
    let server = start_node(node);
    let text = token(ALL);
    let ids = seed(&server, "h", &[b"1"]).await;
    let body = format!(
        r#"{{"kind":"webhook.deliver","url":"http://{address}/x","secret":"never shown",
             "start":"earliest"}}"#
    );
    let created = server
        .http(&Http::new("PUT", "/v1/stream/h/sink/1", &text).body(body.into_bytes()))
        .await;
    assert_eq!(created.status, 200, "{}", created.text());
    let started = std::time::Instant::now();
    loop {
        let info = server
            .http(&Http::new("GET", "/v1/stream/h", &text))
            .await
            .text();
        if info.contains("\"paused\":true") {
            assert!(info.contains("\"pause_reason\":\"status 410\""), "{info}");
            assert!(info.contains("\"failure_count\":1"), "{info}");
            assert!(!info.contains("never shown"), "{info}");
            assert!(info.contains("\"cursor\":\"0\""), "{info}");
            break;
        }
        assert!(started.elapsed().as_secs() < 10, "{info}");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let _ = ids;
    server.server.shutdown();
}

/// `admin` is over the stream in the URL. A forward also writes to its target, which takes
/// `publish` on that stream, as publishing there directly would.
#[tokio::test(flavor = "multi_thread")]
async fn a_forward_sink_needs_publish_on_its_target() {
    let mut node = node_config(2);
    node.sink.pool_thread_count = 2;
    let server = start_node(node);
    seed(&server, "victim", &[b"original"]).await;
    seed(&server, "mine", &[b"injected"]).await;
    let exp = START_MS + 1000;
    let attacker = token_at(exp, 77, &[("mine", ALL)]);
    let body = br#"{"kind":"forward","target":"victim","start":"earliest"}"#;
    let refused = server
        .http(&Http::new("PUT", "/v1/stream/mine/sink/42", &attacker).body(body.to_vec()))
        .await;
    assert_code(&refused, 403, 1);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let read = server
        .http(&Http::new(
            "GET",
            "/v1/stream/victim/read?from=0",
            &token(ALL),
        ))
        .await;
    let has = read
        .body
        .windows(b"injected".len())
        .any(|window| window == b"injected");
    assert!(
        !has,
        "a record reached a stream its sender can't publish to"
    );

    let allowed = token_at(exp, 77, &[("mine", ALL), ("victim", auth::PERM_PUBLISH)]);
    let created = server
        .http(&Http::new("PUT", "/v1/stream/mine/sink/42", &allowed).body(body.to_vec()))
        .await;
    assert_eq!(created.status, 200, "{}", created.text());
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_sink_lifecycle_is_recorded_on_the_stream() {
    let server = start();
    let text = token(ALL);
    let info = async || server.http(&Http::new("GET", "/v1/stream/k", &text)).await;
    let call = async |method: &str, path: &str, body: &str| {
        server
            .http(&Http::new(method, path, &text).body(body.as_bytes().to_vec()))
            .await
    };
    let sink = r#"{"kind":"webhook.deliver","url":"https://example.invalid/x","filter":"any:1",
                   "secret":"s","on_expire":"skip","concurrency":2,"start":"now"}"#;
    assert_code(&call("PUT", "/v1/stream/k/sink/1", sink).await, 404, 2);

    let ids = seed(&server, "k", &[b"1", b"2"]).await;
    let no_key = r#"{"kind":"webhook.deliver","url":"http://example.invalid/x","secret":""}"#;
    assert_code(&call("PUT", "/v1/stream/k/sink/1", no_key).await, 400, 8);
    let created = call("PUT", "/v1/stream/k/sink/1", sink).await;
    assert_eq!(created.status, 200, "{}", created.text());
    let text_info = info().await.text();
    assert!(
        text_info.contains("\"kind\":\"webhook.deliver\""),
        "{text_info}"
    );
    assert!(text_info.contains("\"paused\":false"), "{text_info}");
    assert!(
        text_info.contains(&format!("\"cursor\":\"{}\"", ids[1])),
        "cursor at now"
    );

    let earliest = call(
        "PUT",
        "/v1/stream/k/sink/2",
        r#"{"kind":"forward","target":"other","start":"earliest"}"#,
    )
    .await;
    assert_eq!(earliest.status, 200);
    let at_id = call(
        "PUT",
        "/v1/stream/k/sink/3",
        &format!(r#"{{"kind":"archive","start":"{}"}}"#, ids[0]),
    )
    .await;
    assert_eq!(at_id.status, 200);
    let text_info = info().await.text();
    assert!(text_info.contains("\"cursor\":\"0\""), "{text_info}");
    assert!(
        text_info.contains(&format!("\"cursor\":\"{}\"", ids[0])),
        "{text_info}"
    );

    assert_eq!(
        call("POST", "/v1/stream/k/sink/1/pause", "").await.status,
        200
    );
    assert!(info().await.text().contains("\"paused\":true"));
    // An update keeps the cursor and the pause.
    assert_eq!(call("PUT", "/v1/stream/k/sink/1", sink).await.status, 200);
    let text_info = info().await.text();
    assert!(text_info.contains("\"paused\":true"), "{text_info}");
    assert!(
        text_info.contains(&format!("\"cursor\":\"{}\"", ids[1])),
        "{text_info}"
    );
    assert_eq!(
        call("POST", "/v1/stream/k/sink/1/resume", "").await.status,
        200
    );
    assert!(!info().await.text().contains("\"paused\":true"));
    let reset = call(
        "POST",
        "/v1/stream/k/sink/1/reset",
        r#"{"start":"earliest"}"#,
    )
    .await;
    assert_eq!(reset.status, 200);
    assert_eq!(call("DELETE", "/v1/stream/k/sink/3", "").await.status, 200);

    for (path, body) in [
        ("/v1/stream/k/sink/x", r#"{"kind":"archive"}"#),
        ("/v1/stream/k/sink/9", r#"{"kind":"nope"}"#),
        ("/v1/stream/k/sink/9", r#"{"kind":"forward"}"#),
        (
            "/v1/stream/k/sink/9",
            r#"{"kind":"forward","target":"a/b"}"#,
        ),
        ("/v1/stream/k/sink/9", r#"{"kind":"webhook.notify"}"#),
        (
            "/v1/stream/k/sink/9",
            r#"{"kind":"webhook.deliver","url":"https://example.invalid/x"}"#,
        ),
        (
            "/v1/stream/k/sink/9",
            r#"{"kind":"forward","target":"other","concurrency":2}"#,
        ),
        (
            "/v1/stream/k/sink/0",
            r#"{"kind":"forward","target":"other"}"#,
        ),
        ("/v1/stream/k/sink/9", r#"{"kind":"forward","target":"k"}"#),
        (
            "/v1/stream/k/sink/9",
            r#"{"kind":"webhook.notify","url":"ftp://x"}"#,
        ),
        (
            "/v1/stream/k/sink/9",
            r#"{"kind":"archive","filter":"wat"}"#,
        ),
        (
            "/v1/stream/k/sink/9",
            r#"{"kind":"archive","on_expire":"maybe"}"#,
        ),
        (
            "/v1/stream/k/sink/9",
            r#"{"kind":"archive","concurrency":0}"#,
        ),
        (
            "/v1/stream/k/sink/9",
            r#"{"kind":"archive","start":"soon"}"#,
        ),
        ("/v1/stream/k/sink/9", r#"{"kind":"archive","extra":1}"#),
        ("/v1/stream/k/sink/9", "not json"),
    ] {
        assert_code(&call("PUT", path, body).await, 400, 8);
    }
    for action in ["pause", "resume", "reset"] {
        let path = format!("/v1/stream/k/sink/9/{action}");
        assert_code(&call("POST", &path, "").await, 404, 2);
    }
    assert_code(
        &call("POST", "/v1/stream/k/sink/1/reset", r#"{"start":"soon"}"#).await,
        400,
        8,
    );
    assert_code(
        &call("POST", "/v1/stream/k/sink/1/explode", "").await,
        404,
        2,
    );
    assert_code(&call("DELETE", "/v1/stream/k/sink/9", "").await, 404, 2);
    server.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_routes_wrong_methods_and_bad_requests() {
    let server = start();
    let text = token(ALL);
    let send = async |method: &str, path: &str| server.http(&Http::new(method, path, &text)).await;
    assert_code(&send("GET", "/nope").await, 404, 2);
    assert_code(&send("GET", "/v1/stream/a/nope").await, 404, 2);
    assert_code(&send("POST", "/v1/stream/a").await, 405, 8);
    assert_code(&send("GET", "/v1/stream/a/publish").await, 405, 8);
    assert_code(&send("POST", "/v1/stream/a/read").await, 405, 8);
    assert_code(&send("DELETE", "/v1/stream/a/state").await, 405, 8);
    assert_code(&send("GET", "/v1/stream/a/cursor/c").await, 405, 8);
    assert_code(&send("GET", "/v1/stream/a/sink/1").await, 405, 8);
    assert_code(&send("GET", "/v1/stream/a/sink/1/pause").await, 405, 8);
    assert_code(&send("GET", "/v1/stream/a%2Fb").await, 400, 8);
    assert_code(&send("GET", "/v1/stream/%FF").await, 400, 8);

    // Not HTTP at all, and a body shape this server won't read.
    let mut tls = server.tls().await;
    tls.write_all(b"this is not a request\r\n\r\n")
        .await
        .unwrap();
    assert_code(&read_http_reply(&mut tls).await, 400, 8);
    let mut tls = server.tls().await;
    tls.write_all(b"POST /v1/stream/a/publish HTTP/1.1\r\ntransfer-encoding: chunked\r\n\r\n")
        .await
        .unwrap();
    assert_code(&read_http_reply(&mut tls).await, 400, 8);
    // A head too long.
    let mut tls = server.tls().await;
    let long = format!("GET /{} HTTP/1.1\r\n", "a".repeat(200 * 1024));
    tls.write_all(long.as_bytes()).await.unwrap();
    assert_code(&read_http_reply(&mut tls).await, 400, 8);
    // An upgrade that isn't one.
    assert_eq!(send("GET", "/v1/ws").await.status, 426);
    server.server.shutdown();
}

/// Several requests on one socket, one body running into the next head, and the `100` that
/// `curl` waits for. Nothing is held between them.
#[tokio::test(flavor = "multi_thread")]
async fn keep_alive_and_expect_continue() {
    let server = start();
    let text = token(ALL);
    let mut tls = server.tls().await;
    let one = Http::new("POST", "/v1/stream/ka/publish", &text).body(&b"one"[..]);
    let two = Http::new("POST", "/v1/stream/ka/publish", &text).body(&b"two"[..]);
    let mut both = one.bytes();
    both.extend(two.bytes());
    tls.write_all(&both).await.unwrap();
    assert_eq!(read_http_reply(&mut tls).await.status, 200);
    assert_eq!(read_http_reply(&mut tls).await.status, 200);

    let head = format!(
        "POST /v1/stream/ka/publish HTTP/1.1\r\nauthorization: Bearer {text}\r\n\
         expect: 100-continue\r\ncontent-length: 5\r\n\r\n"
    );
    tls.write_all(head.as_bytes()).await.unwrap();
    let mut buffer = [0u8; 256];
    let read = tls.read(&mut buffer).await.unwrap();
    assert!(buffer[..read].starts_with(b"HTTP/1.1 100"));
    tls.write_all(b"three").await.unwrap();
    assert_eq!(read_http_reply(&mut tls).await.status, 200);

    let read_back = Http::new("GET", "/v1/stream/ka/read", &text);
    tls.write_all(&read_back.bytes()).await.unwrap();
    let reply = read_http_reply(&mut tls).await;
    let datas: Vec<Vec<u8>> = parse_records(&reply.body)
        .into_iter()
        .map(|r| r.3)
        .collect();
    assert_eq!(datas, [b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]);

    // A refused body that fits the drain leaves the connection usable.
    let refused = Http {
        method: "POST",
        path: "/v1/stream/ka/publish",
        token: Some("bad"),
        headers: Vec::new(),
        body: b"xxxx".to_vec(),
    };
    tls.write_all(&refused.bytes()).await.unwrap();
    assert_eq!(read_http_reply(&mut tls).await.status, 401);
    tls.write_all(&read_back.bytes()).await.unwrap();
    assert_eq!(read_http_reply(&mut tls).await.status, 200);
    assert_eq!(server.server.io_stats().held_buffer_count, 0);
    drop(tls);
    server.server.shutdown();
}

/// Error 4, which only a stopped WAL gives (D79, Q22).
struct FailingVolume;
struct FailingFile;

impl WalFile for FailingFile {
    fn write_vectored(&mut self, slices: &[std::io::IoSlice<'_>]) -> std::io::Result<usize> {
        Ok(slices.iter().map(|slice| slice.len()).sum())
    }
    fn sync_data(&mut self) -> std::io::Result<()> {
        Err(std::io::Error::other("scripted sync failure"))
    }
}

impl WalVolume for FailingVolume {
    type File = FailingFile;
    fn create(&mut self, _seq: u64) -> std::io::Result<FailingFile> {
        Ok(FailingFile)
    }
    fn remove(&mut self, _seq: u64) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_wal_is_error_4_on_publish_and_state() {
    let cert = test_cert();
    let manual = Arc::new(server::ManualClock::new(START_MS));
    let server = TestServer {
        server: Server::start(
            ServerConfig {
                node: node_config(1),
                listen: "127.0.0.1:0".parse().unwrap(),
                cert_pem: cert.cert_pem.clone(),
                key_pem: cert.key_pem.clone(),
                token_secret: SECRET.to_vec(),
            },
            FailingVolume,
            Arc::new(object_store::memory::InMemory::new()),
            manual.clone(),
        )
        .unwrap(),
        clock: manual,
        connector: tokio_rustls::TlsConnector::from(Arc::new(tls_client_config(
            &cert.der,
            &[&rustls::version::TLS13],
        ))),
        cert_der: cert.der,
        _wal: tempfile::tempdir().unwrap(),
    };
    let text = token(ALL);
    // The first write hits the failing sync. It may get an error for that, and what follows
    // gets code 4 once the core has heard.
    let mut last = None;
    for _ in 0..50 {
        let reply = server
            .http(&Http::new("POST", "/v1/stream/w/publish", &text).body(&b"x"[..]))
            .await;
        last = Some(reply);
        if last.as_ref().is_some_and(|reply| reply.code() == Some(4)) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_code(&last.unwrap(), 429, 4);
    let state = server
        .http(&Http::new("PATCH", "/v1/stream/w/state", &text).body(cbor_map(&[("a", 1)])))
        .await;
    assert_code(&state, 429, 4);
    drop(server);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_short_token_secret_is_refused_at_the_start() {
    let cert = test_cert();
    let wal = tempfile::tempdir().unwrap();
    let result = Server::start(
        ServerConfig {
            node: node_config(1),
            listen: "127.0.0.1:0".parse().unwrap(),
            cert_pem: cert.cert_pem,
            key_pem: cert.key_pem,
            token_secret: b"short".to_vec(),
        },
        storage::DirVolume::new(wal.path(), 1024 * 1024),
        Arc::new(object_store::memory::InMemory::new()),
        Arc::new(server::SystemClock),
    );
    assert_eq!(
        result.err().unwrap().kind(),
        std::io::ErrorKind::InvalidInput
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bad_certificate_is_refused_at_the_start() {
    let wal = tempfile::tempdir().unwrap();
    let result = Server::start(
        ServerConfig {
            node: node_config(1),
            listen: "127.0.0.1:0".parse().unwrap(),
            cert_pem: b"not a certificate".to_vec(),
            key_pem: b"not a key".to_vec(),
            token_secret: SECRET.to_vec(),
        },
        storage::DirVolume::new(wal.path(), 1024 * 1024),
        Arc::new(object_store::memory::InMemory::new()),
        Arc::new(server::SystemClock),
    );
    assert!(result.is_err());
}

/// Level 0 has no ack to wait for, but it can't be told it was accepted when the owner is going
/// to refuse it. Section 3 says a rejected publish commits nothing and gets a `NACK`.
#[tokio::test(flavor = "multi_thread")]
async fn level_0_is_refused_what_any_other_level_is_refused() {
    let server = start();
    let all = token(ALL);
    let bad_length = vec![1u8, 2, 3];
    let mut bad_padding = record(0, b"abc");
    bad_padding[35] = 1;
    let mut has_id = record(0, b"abc");
    has_id[0] = 1;
    let cases: [(&str, &[u8]); 4] = [
        ("too short", &bad_length),
        ("padding", &bad_padding),
        ("id", &has_id),
        ("empty", b""),
    ];
    for (name, body) in cases {
        for ack in [0, 2] {
            let path = format!("/v1/stream/g.{ack}/publish?ack={ack}");
            let reply = server
                .http(
                    &Http::new("POST", &path, &all)
                        .header("content-type", "application/x-sillystream-records")
                        .body(body),
                )
                .await;
            assert_eq!(reply.status, 400, "{name} at level {ack}: {}", reply.text());
            assert_eq!(reply.code(), Some(8), "{name} at level {ack}");
        }
    }
    // What is fine still goes, and answers as level 0 does, in both forms.
    for (content_type, body) in [
        ("application/x-sillystream-records", record(0, b"abc")),
        ("application/octet-stream", b"abc".to_vec()),
    ] {
        let reply = server
            .http(
                &Http::new("POST", "/v1/stream/g.0/publish?ack=0", &all)
                    .header("content-type", content_type)
                    .body(body),
            )
            .await;
        assert_eq!(reply.status, 202, "{}", reply.text());
    }
    server.server.shutdown();
}

/// Sinks last as long as their stream, and `admin` on one stream is enough to make them, so what
/// one can hold is limited as named cursors are (D218, D222).
#[tokio::test]
async fn a_sinks_text_fields_are_limited_in_size() {
    let server = start();
    let text = token(ALL);
    seed(&server, "k", &[b"1"]).await;
    let put = async |body: String| {
        server
            .http(&Http::new("PUT", "/v1/stream/k/sink/1", &text).body(body.into_bytes()))
            .await
    };
    let url_of = |len: usize| format!("http://example.invalid/{}", "u".repeat(len - 23));
    let at_limit = server::SINK_TEXT_MAX_BYTES;
    let over = at_limit + 1;
    let fits = [
        format!(
            r#"{{"kind":"webhook.deliver","url":"{}","secret":"{}"}}"#,
            url_of(at_limit),
            "s".repeat(at_limit)
        ),
        format!(
            r#"{{"kind":"forward","target":"{}"}}"#,
            "t".repeat(at_limit)
        ),
    ];
    for body in fits {
        let reply = put(body).await;
        assert_eq!(reply.status, 200, "{}", reply.text());
    }
    let too_long = [
        format!(
            r#"{{"kind":"webhook.deliver","url":"http://x/","secret":"{}"}}"#,
            "s".repeat(over)
        ),
        format!(
            r#"{{"kind":"webhook.deliver","url":"{}","secret":"s"}}"#,
            url_of(over)
        ),
        format!(r#"{{"kind":"forward","target":"{}"}}"#, "t".repeat(over)),
        // A field its kind doesn't use is stored too.
        format!(r#"{{"kind":"archive","secret":"{}"}}"#, "s".repeat(over)),
        format!(r#"{{"kind":"archive","url":"{}"}}"#, "u".repeat(over)),
        format!(r#"{{"kind":"archive","target":"{}"}}"#, "t".repeat(over)),
        format!(
            r#"{{"kind":"archive","filter":"any:1{}"}}"#,
            " ".repeat(over)
        ),
    ];
    for body in too_long {
        assert_code(&put(body).await, 400, 8);
    }
}

#[tokio::test]
async fn a_stream_holds_a_limited_number_of_sinks() {
    let server = start();
    let text = token(ALL);
    seed(&server, "k", &[b"1"]).await;
    let call = async |method: &str, id: usize| {
        let path = format!("/v1/stream/k/sink/{id}");
        server
            .http(&Http::new(method, &path, &text).body(br#"{"kind":"archive"}"#.to_vec()))
            .await
    };
    for id in 1..=server::SINK_CAP_COUNT {
        let reply = call("PUT", id).await;
        assert_eq!(reply.status, 200, "sink {id}: {}", reply.text());
    }
    let extra = server::SINK_CAP_COUNT + 1;
    assert_code(&call("PUT", extra).await, 429, 5);
    // One it already has can still change, and deleting one makes room.
    assert_eq!(call("PUT", 1).await.status, 200);
    assert_eq!(call("DELETE", 1).await.status, 200);
    assert_eq!(call("PUT", extra).await.status, 200);
    assert_code(&call("PUT", 1).await, 429, 5);
}

/// D6a: a publish below the minimum is raised to it, and that takes in what the caller is told.
#[tokio::test]
async fn an_http_publish_below_the_minimum_waits_for_the_minimum() {
    let server = start();
    let text = token(ALL);
    let make = async |stream: &str, min_ack: u8| {
        let path = format!("/v1/stream/{stream}");
        let body = format!(r#"{{"min_ack":{min_ack}}}"#);
        let reply = server
            .http(&Http::new("PUT", &path, &text).body(body.into_bytes()))
            .await;
        assert_eq!(reply.status, 200, "{}", reply.text());
    };
    let publish = async |stream: &str, ack: u8| {
        let path = format!("/v1/stream/{stream}/publish?ack={ack}");
        server
            .http(&Http::new("POST", &path, &text).body(&b"hello"[..]))
            .await
    };

    // With no minimum, level 0 is what it always was.
    make("none", 0).await;
    assert_eq!(publish("none", 0).await.status, 202);
    assert_eq!(publish("no.such.stream.yet", 0).await.status, 202);

    make("one", 1).await;
    let raised = publish("one", 0).await;
    assert_eq!(raised.status, 200, "{}", raised.text());
    assert!(raised.field("first_id").is_some(), "{}", raised.text());

    // Level 3 needs an upload, and nothing seals until this test says so.
    make("three", 3).await;
    let waiting = publish("three", 0);
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut waiting)
            .await
            .is_err(),
        "answered before the record could be in the object store"
    );
    let seal = server.server.node().seal().await;
    assert!(
        matches!(seal, server::SealOutcome::Uploaded { .. }),
        "{seal:?}"
    );
    let done = waiting.await;
    assert_eq!(done.status, 200, "{}", done.text());
    assert!(done.field("first_id").is_some(), "{}", done.text());
}

/// A read from below the head whose start is already at the head has nothing to wait for. On a
/// stream made again after a delete, every record below the head is below its floor.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_with_nothing_to_catch_up_on_doesnt_wait() {
    let server = start();
    let text = token(ALL);
    let publish = Http::new("POST", "/v1/stream/d/publish", &text).body(b"x".to_vec());
    assert_eq!(server.http(&publish).await.status, 200);
    let path = "/v1/stream/d";
    assert_eq!(
        server.http(&Http::new("DELETE", path, &text)).await.status,
        200
    );
    let made = Http::new("PUT", path, &text).body(b"{}".to_vec());
    assert_eq!(server.http(&made).await.status, 200);

    let started = std::time::Instant::now();
    let read = Http::new("GET", "/v1/stream/d/read?from=0", &text);
    let reply = server.http(&read).await;
    assert_eq!(reply.status, 200, "{}", reply.text());
    assert_eq!(reply.header("x-count"), Some("0"));
    assert!(
        started.elapsed() < std::time::Duration::from_millis(1_000),
        "{:?}",
        started.elapsed()
    );
}

/// A sink hands a stream's records to whoever made it, so making one takes what a read takes.
#[tokio::test(flavor = "multi_thread")]
async fn making_a_sink_needs_read_on_its_source() {
    let mut node = node_config(2);
    node.sink.pool_thread_count = 2;
    let server = start_node(node);
    seed(&server, "secret", &[b"TOPSECRET"]).await;
    seed(&server, "mine", &[b"mine"]).await;
    let exp = START_MS + 1000;
    let forward = br#"{"kind":"forward","target":"mine","start":"earliest"}"#;
    let path = "/v1/stream/secret/sink/5";

    let admin_only = token_at(
        exp,
        AUTHOR,
        &[
            ("secret", auth::PERM_ADMIN),
            ("mine", auth::PERM_READ | auth::PERM_PUBLISH),
        ],
    );
    let refused = server
        .http(&Http::new("PUT", path, &admin_only).body(forward.to_vec()))
        .await;
    assert_code(&refused, 403, 1);
    let archive = server
        .http(&Http::new("PUT", path, &admin_only).body(br#"{"kind":"archive"}"#.to_vec()))
        .await;
    assert_code(&archive, 403, 1);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let read = server
        .http(&Http::new(
            "GET",
            "/v1/stream/mine/read?from=0",
            &token(ALL),
        ))
        .await;
    assert!(
        !read.body.windows(9).any(|window| window == b"TOPSECRET"),
        "a token with no read permission got a stream's records"
    );

    // Read and admin may come from two grants that both match.
    let both = token_at(
        exp,
        AUTHOR,
        &[
            ("secret", auth::PERM_ADMIN),
            ("sec*", auth::PERM_READ),
            ("mine", auth::PERM_PUBLISH),
        ],
    );
    let created = server
        .http(&Http::new("PUT", path, &both).body(forward.to_vec()))
        .await;
    assert_eq!(created.status, 200, "{}", created.text());
    server.server.shutdown();
}

/// Such a cursor would hide every later record while the sink's status stays clean, so this is
/// refused as `SUBSCRIBE` and `COMMIT` refuse it (D230).
#[tokio::test(flavor = "multi_thread")]
async fn a_sink_start_above_the_head_is_refused() {
    let server = start();
    let ids = seed(&server, "k", &[b"1"]).await;
    let text = token(ALL);
    let put = |id: u64, start: u64| {
        format!("/v1/stream/k/sink/{id}|{{\"kind\":\"archive\",\"start\":{start}}}")
    };
    let call = async |method: &'static str, target: String| {
        let (path, body) = target.split_once('|').unwrap();
        server
            .http(&Http::new(method, path, &text).body(body.as_bytes().to_vec()))
            .await
    };
    let head = ids[0];
    assert_code(&call("PUT", put(1, 1 << 63)).await, 400, 8);
    assert_code(&call("PUT", put(1, head + 1)).await, 400, 8);
    let info = server
        .http(&Http::new("GET", "/v1/stream/k", &text))
        .await
        .text();
    assert!(info.contains("\"sinks\":[]"), "{info}");

    assert_eq!(call("PUT", put(1, head)).await.status, 200);
    // The same id section 5.1 reads as "live only" still means that.
    assert_eq!(call("PUT", put(2, u64::MAX)).await.status, 200);
    let reset = |start: u64| format!("/v1/stream/k/sink/1/reset|{{\"start\":{start}}}");
    assert_code(&call("POST", reset(1 << 63)).await, 400, 8);
    assert_eq!(call("POST", reset(head)).await.status, 200);
    server.server.shutdown();
}
