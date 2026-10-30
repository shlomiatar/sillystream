//! Server to client frames.

use crate::wire::{
    encode_with, put_bytes, put_str, put_stream, put_u16, put_u32, put_u64, require_flags,
    split_frame,
};
use crate::{ErrorCode, ProtoError};

pub const TYPE_WELCOME: u8 = 0x81;
pub const TYPE_SUBSCRIBED: u8 = 0x82;
pub const TYPE_DATA: u8 = 0x83;
pub const TYPE_ACK: u8 = 0x84;
pub const TYPE_STATE: u8 = 0x85;
pub const TYPE_BEHIND: u8 = 0x86;
pub const TYPE_PROGRESS: u8 = 0x87;
pub const TYPE_STATE_RESULT: u8 = 0x88;
pub const TYPE_REDIRECT: u8 = 0x89;
pub const TYPE_NACK: u8 = 0x8A;
pub const TYPE_ERROR: u8 = 0x8E;
pub const TYPE_PONG: u8 = 0x8F;

/// `type | flags | sub | records length`.
pub const DATA_PREFIX_BYTES: usize = 10;

/// Everything in a `DATA` frame before the records, so that a sender can pass the
/// original record bytes to the socket without copying them (section 5.2). `None`
/// if the length doesn't fit a `bytes` field.
pub fn data_prefix(sub: u32, records_bytes: usize) -> Option<[u8; DATA_PREFIX_BYTES]> {
    let records_len = u32::try_from(records_bytes).ok()?;
    let mut prefix = [0u8; DATA_PREFIX_BYTES];
    prefix[0] = TYPE_DATA;
    prefix[2..6].copy_from_slice(&sub.to_le_bytes());
    prefix[6..10].copy_from_slice(&records_len.to_le_bytes());
    Some(prefix)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerFrame<'a> {
    Welcome {
        session: u64,
        max_frame_bytes: u32,
        server_ms: u64,
    },
    Subscribed {
        sub: u32,
        head: u64,
        state_ver: u64,
        state: &'a [u8],
    },
    Data {
        sub: u32,
        records: &'a [u8],
    },
    Ack {
        handle: u32,
        producer_seq: u64,
        first_id: u64,
        count: u32,
    },
    State {
        sub: u32,
        ver: u64,
        doc: &'a [u8],
    },
    Behind {
        sub: u32,
        resume_from: u64,
    },
    Progress {
        sub: u32,
        id: u64,
    },
    StateResult {
        req: u32,
        ok: bool,
        ver: u64,
    },
    /// The wire format only: nothing sends it until multiple nodes (section 8.2).
    Redirect {
        stream: &'a str,
        host: &'a str,
    },
    Nack {
        handle: u32,
        producer_seq: u64,
        code: ErrorCode,
        msg: &'a str,
    },
    Error {
        reference: u32,
        code: ErrorCode,
        msg: &'a str,
    },
    Pong {
        nonce: u32,
    },
}

fn error_code(code: u16) -> Result<ErrorCode, ProtoError> {
    ErrorCode::from_wire(code).ok_or(ProtoError::UnknownErrorCode)
}

/// Decodes one WebSocket message from a server. Never panics.
pub fn decode_server(frame: &[u8], max_frame_bytes: usize) -> Result<ServerFrame<'_>, ProtoError> {
    let (frame_type, flags, mut body) = split_frame(frame, max_frame_bytes)?;
    // No server frame defines a flag. Unknown types are reported as such below.
    if matches!(
        frame_type,
        TYPE_WELCOME..=TYPE_NACK | TYPE_ERROR | TYPE_PONG
    ) {
        require_flags(flags, 0)?;
    }
    let decoded = match frame_type {
        TYPE_WELCOME => ServerFrame::Welcome {
            session: body.u64()?,
            max_frame_bytes: body.u32()?,
            server_ms: body.u64()?,
        },
        TYPE_SUBSCRIBED => ServerFrame::Subscribed {
            sub: body.u32()?,
            head: body.u64()?,
            state_ver: body.u64()?,
            state: body.bytes()?,
        },
        TYPE_DATA => ServerFrame::Data {
            sub: body.u32()?,
            records: body.bytes()?,
        },
        TYPE_ACK => ServerFrame::Ack {
            handle: body.u32()?,
            producer_seq: body.u64()?,
            first_id: body.u64()?,
            count: body.u32()?,
        },
        TYPE_STATE => ServerFrame::State {
            sub: body.u32()?,
            ver: body.u64()?,
            doc: body.bytes()?,
        },
        TYPE_BEHIND => ServerFrame::Behind {
            sub: body.u32()?,
            resume_from: body.u64()?,
        },
        TYPE_PROGRESS => ServerFrame::Progress {
            sub: body.u32()?,
            id: body.u64()?,
        },
        TYPE_STATE_RESULT => ServerFrame::StateResult {
            req: body.u32()?,
            ok: match body.u8()? {
                0 => false,
                1 => true,
                _ => return Err(ProtoError::InvalidOk),
            },
            ver: body.u64()?,
        },
        TYPE_REDIRECT => ServerFrame::Redirect {
            stream: body.stream()?,
            host: body.str()?,
        },
        TYPE_NACK => ServerFrame::Nack {
            handle: body.u32()?,
            producer_seq: body.u64()?,
            code: error_code(body.u16()?)?,
            msg: body.str()?,
        },
        TYPE_ERROR => ServerFrame::Error {
            reference: body.u32()?,
            code: error_code(body.u16()?)?,
            msg: body.str()?,
        },
        TYPE_PONG => ServerFrame::Pong { nonce: body.u32()? },
        _ => return Err(ProtoError::UnknownType),
    };
    body.finish()?;
    Ok(decoded)
}

impl ServerFrame<'_> {
    /// Appends the frame to `out`, or leaves `out` unchanged on error. This copies
    /// `records`; see [`data_prefix`] for sending `DATA` without a copy.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), ProtoError> {
        encode_with(out, |out| self.encode_inner(out))
    }

    fn encode_inner(&self, out: &mut Vec<u8>) -> Result<(), ProtoError> {
        match self {
            ServerFrame::Welcome {
                session,
                max_frame_bytes,
                server_ms,
            } => {
                out.extend_from_slice(&[TYPE_WELCOME, 0]);
                put_u64(out, *session);
                put_u32(out, *max_frame_bytes);
                put_u64(out, *server_ms);
                Ok(())
            }
            ServerFrame::Subscribed {
                sub,
                head,
                state_ver,
                state,
            } => {
                out.extend_from_slice(&[TYPE_SUBSCRIBED, 0]);
                put_u32(out, *sub);
                put_u64(out, *head);
                put_u64(out, *state_ver);
                put_bytes(out, state)
            }
            ServerFrame::Data { sub, records } => {
                let prefix = data_prefix(*sub, records.len()).ok_or(ProtoError::BytesTooLong)?;
                out.extend_from_slice(&prefix);
                out.extend_from_slice(records);
                Ok(())
            }
            ServerFrame::Ack {
                handle,
                producer_seq,
                first_id,
                count,
            } => {
                out.extend_from_slice(&[TYPE_ACK, 0]);
                put_u32(out, *handle);
                put_u64(out, *producer_seq);
                put_u64(out, *first_id);
                put_u32(out, *count);
                Ok(())
            }
            ServerFrame::State { sub, ver, doc } => {
                out.extend_from_slice(&[TYPE_STATE, 0]);
                put_u32(out, *sub);
                put_u64(out, *ver);
                put_bytes(out, doc)
            }
            ServerFrame::Behind { sub, resume_from } => {
                out.extend_from_slice(&[TYPE_BEHIND, 0]);
                put_u32(out, *sub);
                put_u64(out, *resume_from);
                Ok(())
            }
            ServerFrame::Progress { sub, id } => {
                out.extend_from_slice(&[TYPE_PROGRESS, 0]);
                put_u32(out, *sub);
                put_u64(out, *id);
                Ok(())
            }
            ServerFrame::StateResult { req, ok, ver } => {
                out.extend_from_slice(&[TYPE_STATE_RESULT, 0]);
                put_u32(out, *req);
                out.push(u8::from(*ok));
                put_u64(out, *ver);
                Ok(())
            }
            ServerFrame::Redirect { stream, host } => {
                out.extend_from_slice(&[TYPE_REDIRECT, 0]);
                put_stream(out, stream)?;
                put_str(out, host)
            }
            ServerFrame::Nack {
                handle,
                producer_seq,
                code,
                msg,
            } => {
                out.extend_from_slice(&[TYPE_NACK, 0]);
                put_u32(out, *handle);
                put_u64(out, *producer_seq);
                put_u16(out, code.to_wire());
                put_str(out, msg)
            }
            ServerFrame::Error {
                reference,
                code,
                msg,
            } => {
                out.extend_from_slice(&[TYPE_ERROR, 0]);
                put_u32(out, *reference);
                put_u16(out, code.to_wire());
                put_str(out, msg)
            }
            ServerFrame::Pong { nonce } => {
                out.extend_from_slice(&[TYPE_PONG, 0]);
                put_u32(out, *nonce);
                Ok(())
            }
        }
    }
}
