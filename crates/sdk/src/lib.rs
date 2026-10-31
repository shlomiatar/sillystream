//! The Rust client: batching, per-record zstd, cursors, retries with `(producer_id, producer_seq)`,
//! the wait on `expect_ver`, skipping by id and reconnection (SPEC sections 3, 5 and 11).

mod batch;
mod client;
pub mod http;

pub use batch::{BatchConfig, CODEC_RAW, CODEC_ZSTD, Item, build_records, decode_records};
pub use client::{
    Ack, AckWait, Client, ClientConfig, Event, Publisher, PublisherConfig, StateChange, Subscribe,
    Subscription,
};
pub use proto::{AckLevel, ErrorCode, StateOp, WireClause};

/// `from` for live only (section 5.1).
pub const FROM_LIVE: u64 = u64::MAX;
/// `from` for the start of retention.
pub const FROM_START: u64 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SdkError {
    /// The server refused this, with one of section 11.1's codes.
    Server { code: ErrorCode, msg: String },
    /// `expect_ver` didn't match. `ver` is the version now, or 0 where a `NACK` didn't say.
    VersionConflict { ver: u64 },
    /// The connection went while this was unanswered, and it can't be retried safely for you.
    Disconnected,
    /// The client has stopped.
    Closed,
    /// Over the maximum frame, or a size field's range.
    TooLarge,
    /// A record that doesn't decode.
    Codec,
    /// A record of a subscription that can't be opened: bad data, or `too_large` for
    /// `max_decompressed_bytes`. Only those ids are lost, and the subscription carries on.
    BadRecord {
        first_id: u64,
        last_id: u64,
        too_large: bool,
    },
    /// Reaching the server: the socket, TLS or the WebSocket upgrade.
    Connect(String),
    /// A bad setting.
    Config(String),
}

impl std::fmt::Display for SdkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SdkError::Server { code, msg } => {
                write!(f, "server error {}: {msg}", code.to_wire())
            }
            SdkError::VersionConflict { ver } => write!(f, "version conflict, now at {ver}"),
            SdkError::Disconnected => f.write_str("disconnected with the operation unanswered"),
            SdkError::Closed => f.write_str("the client has stopped"),
            SdkError::TooLarge => f.write_str("too large for a frame"),
            SdkError::Codec => f.write_str("a record didn't decode"),
            SdkError::BadRecord {
                first_id,
                last_id,
                too_large,
            } => {
                let why = if *too_large {
                    "too large"
                } else {
                    "undecodable"
                };
                write!(f, "records {first_id} to {last_id} are {why}")
            }
            SdkError::Connect(text) => write!(f, "connect: {text}"),
            SdkError::Config(text) => write!(f, "config: {text}"),
        }
    }
}

impl std::error::Error for SdkError {}
