//! Helpers the pattern examples share: connecting from the `silly` environment variables,
//! and the state doc as a `ciborium::Value`.
#![allow(dead_code)]

use ciborium::Value;
use sdk::http::HttpClient;
use sdk::{Client, ClientConfig};

pub fn env() -> (String, Vec<u8>, String) {
    let url = std::env::var("SILLYSTREAM_URL").expect("SILLYSTREAM_URL");
    let ca = std::fs::read(std::env::var("SILLYSTREAM_CA").expect("SILLYSTREAM_CA")).unwrap();
    let token = std::env::var("SILLYSTREAM_TOKEN").expect("SILLYSTREAM_TOKEN");
    (url, ca, token)
}

pub async fn connect() -> (Client, HttpClient) {
    let (url, ca, token) = env();
    let client = Client::connect(ClientConfig::new(&url, &ca, &token))
        .await
        .unwrap();
    (client, HttpClient::new(&url, &ca, &token).unwrap())
}

/// `GET /state`: the doc, its version and the stream's head, in one consistent step.
pub async fn state_get(http: &HttpClient, stream: &str) -> (Value, u64, u64) {
    let reply = http
        .request("GET", &format!("/v1/stream/{stream}/state"), &[], b"")
        .await
        .unwrap();
    let number = |name| reply.header(name).and_then(|v| v.parse().ok()).unwrap_or(0);
    // A stream that doesn't exist yet is a 404: no state, version 0.
    let doc = if reply.status == 404 || reply.body.is_empty() {
        Value::Map(vec![])
    } else {
        ciborium::from_reader(&reply.body[..]).unwrap()
    };
    (doc, number("x-state-ver"), number("x-head"))
}

pub fn to_cbor(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    ciborium::into_writer(value, &mut out).unwrap();
    out
}

/// `doc[key]` for a text key.
pub fn get<'a>(doc: &'a Value, key: &str) -> Option<&'a Value> {
    doc.as_map()?
        .iter()
        .find(|(k, _)| k.as_text() == Some(key))
        .map(|(_, v)| v)
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
