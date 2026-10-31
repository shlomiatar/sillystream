//! Store configuration. Real S3 is never contacted by any test in this workspace.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::path::Path;
use storage::{
    ObjectStoreConfig, S3Config, UploadEvent, Uploader, UploaderConfig, build_store,
    dedicated_path, seal_path,
};

fn fake_s3(endpoint: String) -> ObjectStoreConfig {
    ObjectStoreConfig::S3(S3Config {
        bucket: "not-a-real-bucket".to_owned(),
        region: "us-east-1".to_owned(),
        endpoint: Some(endpoint),
        access_key_id: Some("fake-key-id".to_owned()),
        secret_access_key: Some("fake-secret".to_owned()),
        allow_http: true,
    })
}

#[test]
fn the_s3_config_builds_a_client_without_connecting() {
    // A listener nobody should ever reach: it is the configured endpoint, and it
    // would hold any attempt in its accept queue.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let store = build_store(&fake_s3(endpoint)).unwrap();
    assert!(store.to_string().contains("not-a-real-bucket"));
    match listener.accept() {
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock),
        Ok((_, peer)) => panic!("building the client connected, from {peer}"),
    }
}

#[test]
fn the_s3_config_doesnt_show_its_keys_in_debug() {
    let text = format!("{:?}", fake_s3("http://127.0.0.1:1".to_owned()));
    assert!(text.contains("not-a-real-bucket"));
    assert!(!text.contains("fake-secret"));
    assert!(!text.contains("fake-key-id"));
}

async fn round_trip(store: Arc<dyn ObjectStore>) {
    let path = Path::from("a/b.seg");
    store.put(&path, vec![1, 2, 3].into()).await.unwrap();
    let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(&bytes[..], [1, 2, 3]);
}

#[tokio::test]
async fn memory_and_local_config_build_working_stores() {
    round_trip(build_store(&ObjectStoreConfig::Memory).unwrap()).await;
    let dir = tempfile::tempdir().unwrap();
    let local = ObjectStoreConfig::Local {
        root: dir.path().to_owned(),
    };
    round_trip(build_store(&local).unwrap()).await;
}

#[test]
fn paths_follow_section_7_3() {
    let first_id = 7_200_000u64 << 16;
    assert_eq!(
        seal_path("node-1", 2, first_id).to_string(),
        format!("sealed/2/node-1/{:020}-{first_id:020}.seg", 2)
    );
    assert_eq!(
        dedicated_path("logs.a", 2, first_id).to_string(),
        format!("sealed/2/logs.a/{:020}-{first_id:020}.seg", 2)
    );
}

#[test]
fn a_hostile_stream_id_stays_one_path_part() {
    for stream in ["..", ".", "a b", "%2F", "x\ny"] {
        let path = dedicated_path(stream, 1, 1 << 16);
        assert_eq!(path.parts().count(), 4, "{stream}");
        assert!(!path.parts().any(|part| part.as_ref() == ".."), "{stream}");
    }
}

/// Answers exactly one HTTP request and returns its head and body length. This is not
/// S3: it only shows that the S3 client can run on the uploader's own runtime, which an
/// in-memory store can't, since that needs no network driver.
fn answer_one_put(listener: TcpListener) -> std::thread::JoinHandle<(String, usize)> {
    std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut request = Vec::new();
        let mut buffer = [0u8; 4096];
        let head_end = loop {
            let read = socket.read(&mut buffer).unwrap();
            assert!(read > 0, "the client hung up before the end of its headers");
            request.extend_from_slice(&buffer[..read]);
            if let Some(at) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                break at + 4;
            }
        };
        let head = String::from_utf8_lossy(&request[..head_end]).to_ascii_lowercase();
        let content_length: usize = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length: "))
            .map_or(0, |value| value.trim().parse().unwrap());
        while request.len() < head_end + content_length {
            let read = socket.read(&mut buffer).unwrap();
            assert!(read > 0, "the client hung up before the end of its body");
            request.extend_from_slice(&buffer[..read]);
        }
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nETag: \"stub\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
        (head, content_length)
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn the_s3_client_runs_on_the_uploader_runtime_against_a_local_stub() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = answer_one_put(listener);
    let store = build_store(&fake_s3(endpoint)).unwrap();
    let (events_tx, mut events) = tokio::sync::mpsc::unbounded_channel();
    let (sender, uploader) = Uploader::<bytes::Bytes>::start(UploaderConfig {
        store,
        node: "node-1".to_owned(),
        generation: 1,
        section_count: 1,
        channel_capacity: 4,
        retry_initial_ms: 1,
        retry_max_ms: 4,
        put_timeout_min_ms: 60_000,
        cache: None,
        events: vec![events_tx],
    })
    .unwrap();
    let mut wire = Vec::new();
    let header = format::RecordHeader {
        id: 1 << 32,
        options: 1,
        author: 7,
        len: 4,
        count: 1,
        codec: 0,
    };
    format::encode_record(&mut wire, &header, b"data").unwrap();
    sender
        .try_send_dedicated(Arc::from("s"), 0, vec![bytes::Bytes::from(wire)])
        .unwrap();
    let len_bytes = match events.recv().await.unwrap() {
        UploadEvent::Uploaded { len_bytes, .. } => len_bytes,
        other => panic!("unexpected event: {other:?}"),
    };
    let (head, content_length) = server.join().unwrap();
    assert!(head.starts_with("put /not-a-real-bucket/sealed/"), "{head}");
    assert!(head.contains("if-none-match: *"), "{head}");
    assert_eq!(content_length as u64, len_bytes);
    drop(sender);
    uploader.finish();
}
