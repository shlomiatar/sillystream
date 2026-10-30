//! WebSocket frames (SPEC section 11.1): one WebSocket message is one frame,
//! `type u8 | flags u8 | body`, little-endian.
//!
//! Decoding borrows from the input and allocates nothing, and never reads inside
//! `records`. Decoders are strict: unknown types and flags, trailing bytes and
//! out-of-range enum values are errors (D39), so every frame has exactly one
//! encoding and anything accepted re-encodes to the same bytes.
#![forbid(unsafe_code)]

mod client;
mod error;
mod server;
mod wire;

pub use client::{
    AckLevel, ClientFrame, FLAG_HAS_AUTHOR, FLAG_HAS_STATE, FLAG_INCLUDE_SYSTEM, FLAG_NAMED_CURSOR,
    FLAG_STATE_ONLY, FLAG_WITH_STATE, Publish, PublishState, StateOp, Subscribe, TYPE_COMMIT,
    TYPE_HELLO, TYPE_OPEN, TYPE_PING, TYPE_PUBLISH, TYPE_STATE_OP, TYPE_SUBSCRIBE,
    TYPE_UNSUBSCRIBE, WireClause, WireClauses, decode_client,
};
pub use error::{ErrorCode, ProtoError};
pub use server::{
    DATA_PREFIX_BYTES, ServerFrame, TYPE_ACK, TYPE_BEHIND, TYPE_DATA, TYPE_ERROR, TYPE_NACK,
    TYPE_PONG, TYPE_PROGRESS, TYPE_REDIRECT, TYPE_STATE, TYPE_STATE_RESULT, TYPE_SUBSCRIBED,
    TYPE_WELCOME, data_prefix, decode_server,
};
pub use wire::{DEFAULT_MAX_FRAME_BYTES, PROTOCOL_VERSION, is_valid_stream_id};
