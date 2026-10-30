//! Client to server frames.

use format::{Clause, Filter, MAX_CLAUSES};

use crate::ProtoError;
use crate::wire::{
    Reader, encode_with, put_bytes, put_str, put_stream, put_u16, put_u32, put_u64, require_flags,
    split_frame,
};

pub const TYPE_HELLO: u8 = 0x01;
pub const TYPE_SUBSCRIBE: u8 = 0x02;
pub const TYPE_UNSUBSCRIBE: u8 = 0x03;
pub const TYPE_OPEN: u8 = 0x04;
pub const TYPE_PUBLISH: u8 = 0x05;
pub const TYPE_STATE_OP: u8 = 0x06;
pub const TYPE_COMMIT: u8 = 0x07;
pub const TYPE_PING: u8 = 0x08;

pub const FLAG_WITH_STATE: u8 = 0x01;
pub const FLAG_STATE_ONLY: u8 = 0x02;
pub const FLAG_NAMED_CURSOR: u8 = 0x04;
pub const FLAG_INCLUDE_SYSTEM: u8 = 0x08;
pub const FLAG_HAS_AUTHOR: u8 = 0x10;
const SUBSCRIBE_FLAGS: u8 =
    FLAG_WITH_STATE | FLAG_STATE_ONLY | FLAG_NAMED_CURSOR | FLAG_INCLUDE_SYSTEM | FLAG_HAS_AUTHOR;

pub const FLAG_HAS_STATE: u8 = 0x01;

/// Section 7.2's ack levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum AckLevel {
    None = 0,
    Memory = 1,
    WalFsync = 2,
    SegmentInS3 = 3,
}

impl AckLevel {
    pub fn from_wire(level: u8) -> Result<AckLevel, ProtoError> {
        match level {
            0 => Ok(AckLevel::None),
            1 => Ok(AckLevel::Memory),
            2 => Ok(AckLevel::WalFsync),
            3 => Ok(AckLevel::SegmentInS3),
            _ => Err(ProtoError::UnknownAckLevel),
        }
    }
}

/// Section 10's state operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StateOp {
    Set = 1,
    Merge = 2,
    Incr = 3,
}

impl StateOp {
    pub fn from_wire(op: u8) -> Result<StateOp, ProtoError> {
        match op {
            1 => Ok(StateOp::Set),
            2 => Ok(StateOp::Merge),
            3 => Ok(StateOp::Incr),
            _ => Err(ProtoError::UnknownStateOp),
        }
    }
}

/// A filter clause as it is on the wire. `any` has no use for `value`, but the
/// bytes are kept so that a frame re-encodes exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WireClause {
    pub mask: u64,
    pub value: u64,
    pub op: u8,
}

impl WireClause {
    pub fn to_clause(self) -> Result<Clause, ProtoError> {
        // `Clause::from_wire` has no other failure.
        Clause::from_wire(self.mask, self.value, self.op).map_err(|_| ProtoError::UnknownClauseOp)
    }
}

/// Up to 4 clauses, without an allocation. Unused slots stay zeroed, so equal
/// lists compare equal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WireClauses {
    slots: [WireClause; MAX_CLAUSES],
    count: u8,
}

impl WireClauses {
    /// Refuses more than 4 clauses (D2) and unknown ops (D1).
    pub fn new(clauses: &[WireClause]) -> Result<WireClauses, ProtoError> {
        if clauses.len() > MAX_CLAUSES {
            return Err(ProtoError::TooManyClauses);
        }
        let mut list = WireClauses::default();
        for (index, clause) in clauses.iter().enumerate() {
            clause.to_clause()?;
            list.slots[index] = *clause;
        }
        list.count = clauses.len() as u8;
        Ok(list)
    }

    pub fn as_slice(&self) -> &[WireClause] {
        &self.slots[..self.count as usize]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Subscribe<'a> {
    pub sub: u32,
    pub from: u64,
    pub linger_ms: u16,
    /// Present exactly when `HAS_AUTHOR` is set.
    pub author: Option<u64>,
    pub clauses: WireClauses,
    pub stream: &'a str,
    /// Present exactly when `NAMED_CURSOR` is set.
    pub name: Option<&'a str>,
    pub with_state: bool,
    pub state_only: bool,
    pub include_system: bool,
}

impl Subscribe<'_> {
    /// The record filter this subscription asks for. `from` is a cursor (section
    /// 5.1), not an id range, so the range stays unbounded.
    pub fn filter(&self) -> Result<Filter, ProtoError> {
        let mut clauses = [Clause::Any { mask: 0 }; MAX_CLAUSES];
        let wire = self.clauses.as_slice();
        for (slot, clause) in clauses.iter_mut().zip(wire) {
            *slot = clause.to_clause()?;
        }
        let mut filter =
            Filter::new(&clauses[..wire.len()]).map_err(|_| ProtoError::TooManyClauses)?;
        filter.author = self.author;
        filter.include_system = self.include_system;
        Ok(filter)
    }

    fn flags(&self) -> u8 {
        let mut flags = 0;
        if self.with_state {
            flags |= FLAG_WITH_STATE;
        }
        if self.state_only {
            flags |= FLAG_STATE_ONLY;
        }
        if self.name.is_some() {
            flags |= FLAG_NAMED_CURSOR;
        }
        if self.include_system {
            flags |= FLAG_INCLUDE_SYSTEM;
        }
        if self.author.is_some() {
            flags |= FLAG_HAS_AUTHOR;
        }
        flags
    }
}

/// The state change a publish carries with `HAS_STATE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishState<'a> {
    pub expect_ver: u64,
    pub op: StateOp,
    pub doc: &'a [u8],
}

/// `records` is always the last field, so it starts at
/// `frame.len() - records.len()`, which is how an owner holding the frame in a
/// mutable slab finds the bytes to stamp in place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Publish<'a> {
    pub handle: u32,
    pub producer_id: u64,
    pub producer_seq: u64,
    pub ack: AckLevel,
    /// Present exactly when `HAS_STATE` is set.
    pub state: Option<PublishState<'a>>,
    pub records: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientFrame<'a> {
    Hello {
        ver: u16,
        token: &'a str,
    },
    Subscribe(Subscribe<'a>),
    Unsubscribe {
        sub: u32,
    },
    Open {
        handle: u32,
        stream: &'a str,
    },
    Publish(Publish<'a>),
    StateOp {
        handle: u32,
        req: u32,
        expect_ver: u64,
        op: StateOp,
        doc: &'a [u8],
    },
    Commit {
        sub: u32,
        id: u64,
    },
    Ping {
        nonce: u32,
    },
}

/// Decodes one WebSocket message from a client. Never panics.
pub fn decode_client(frame: &[u8], max_frame_bytes: usize) -> Result<ClientFrame<'_>, ProtoError> {
    let (frame_type, flags, mut body) = split_frame(frame, max_frame_bytes)?;
    let decoded = match frame_type {
        TYPE_HELLO => {
            require_flags(flags, 0)?;
            ClientFrame::Hello {
                ver: body.u16()?,
                token: body.str()?,
            }
        }
        TYPE_SUBSCRIBE => {
            require_flags(flags, SUBSCRIBE_FLAGS)?;
            ClientFrame::Subscribe(decode_subscribe(flags, &mut body)?)
        }
        TYPE_UNSUBSCRIBE => {
            require_flags(flags, 0)?;
            ClientFrame::Unsubscribe { sub: body.u32()? }
        }
        TYPE_OPEN => {
            require_flags(flags, 0)?;
            ClientFrame::Open {
                handle: body.u32()?,
                stream: body.stream()?,
            }
        }
        TYPE_PUBLISH => {
            require_flags(flags, FLAG_HAS_STATE)?;
            ClientFrame::Publish(decode_publish(flags, &mut body)?)
        }
        TYPE_STATE_OP => {
            require_flags(flags, 0)?;
            ClientFrame::StateOp {
                handle: body.u32()?,
                req: body.u32()?,
                expect_ver: body.u64()?,
                op: StateOp::from_wire(body.u8()?)?,
                doc: body.bytes()?,
            }
        }
        TYPE_COMMIT => {
            require_flags(flags, 0)?;
            ClientFrame::Commit {
                sub: body.u32()?,
                id: body.u64()?,
            }
        }
        TYPE_PING => {
            require_flags(flags, 0)?;
            ClientFrame::Ping { nonce: body.u32()? }
        }
        _ => return Err(ProtoError::UnknownType),
    };
    body.finish()?;
    Ok(decoded)
}

fn decode_subscribe<'a>(flags: u8, body: &mut Reader<'a>) -> Result<Subscribe<'a>, ProtoError> {
    let sub = body.u32()?;
    let from = body.u64()?;
    let linger_ms = body.u16()?;
    let author = if flags & FLAG_HAS_AUTHOR != 0 {
        Some(body.u64()?)
    } else {
        None
    };
    let nfilt = body.u8()? as usize;
    if nfilt > MAX_CLAUSES {
        return Err(ProtoError::TooManyClauses);
    }
    let mut slots = [WireClause::default(); MAX_CLAUSES];
    for slot in &mut slots[..nfilt] {
        *slot = WireClause {
            mask: body.u64()?,
            value: body.u64()?,
            op: body.u8()?,
        };
    }
    let clauses = WireClauses::new(&slots[..nfilt])?;
    let stream = body.stream()?;
    let name = if flags & FLAG_NAMED_CURSOR != 0 {
        Some(body.str()?)
    } else {
        None
    };
    Ok(Subscribe {
        sub,
        from,
        linger_ms,
        author,
        clauses,
        stream,
        name,
        with_state: flags & FLAG_WITH_STATE != 0,
        state_only: flags & FLAG_STATE_ONLY != 0,
        include_system: flags & FLAG_INCLUDE_SYSTEM != 0,
    })
}

fn decode_publish<'a>(flags: u8, body: &mut Reader<'a>) -> Result<Publish<'a>, ProtoError> {
    let handle = body.u32()?;
    let producer_id = body.u64()?;
    let producer_seq = body.u64()?;
    let ack = AckLevel::from_wire(body.u8()?)?;
    let state = if flags & FLAG_HAS_STATE != 0 {
        Some(PublishState {
            expect_ver: body.u64()?,
            op: StateOp::from_wire(body.u8()?)?,
            doc: body.bytes()?,
        })
    } else {
        None
    };
    let records = body.bytes()?;
    Ok(Publish {
        handle,
        producer_id,
        producer_seq,
        ack,
        state,
        records,
    })
}

impl ClientFrame<'_> {
    /// Appends the frame to `out`, or leaves `out` unchanged on error. This doesn't
    /// know the connection's `max_frame`; the sender checks that.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), ProtoError> {
        encode_with(out, |out| self.encode_inner(out))
    }

    fn encode_inner(&self, out: &mut Vec<u8>) -> Result<(), ProtoError> {
        match self {
            ClientFrame::Hello { ver, token } => {
                out.extend_from_slice(&[TYPE_HELLO, 0]);
                put_u16(out, *ver);
                put_str(out, token)
            }
            ClientFrame::Subscribe(subscribe) => encode_subscribe(subscribe, out),
            ClientFrame::Unsubscribe { sub } => {
                out.extend_from_slice(&[TYPE_UNSUBSCRIBE, 0]);
                put_u32(out, *sub);
                Ok(())
            }
            ClientFrame::Open { handle, stream } => {
                out.extend_from_slice(&[TYPE_OPEN, 0]);
                put_u32(out, *handle);
                put_stream(out, stream)
            }
            ClientFrame::Publish(publish) => encode_publish(publish, out),
            ClientFrame::StateOp {
                handle,
                req,
                expect_ver,
                op,
                doc,
            } => {
                out.extend_from_slice(&[TYPE_STATE_OP, 0]);
                put_u32(out, *handle);
                put_u32(out, *req);
                put_u64(out, *expect_ver);
                out.push(*op as u8);
                put_bytes(out, doc)
            }
            ClientFrame::Commit { sub, id } => {
                out.extend_from_slice(&[TYPE_COMMIT, 0]);
                put_u32(out, *sub);
                put_u64(out, *id);
                Ok(())
            }
            ClientFrame::Ping { nonce } => {
                out.extend_from_slice(&[TYPE_PING, 0]);
                put_u32(out, *nonce);
                Ok(())
            }
        }
    }
}

fn encode_subscribe(subscribe: &Subscribe<'_>, out: &mut Vec<u8>) -> Result<(), ProtoError> {
    out.extend_from_slice(&[TYPE_SUBSCRIBE, subscribe.flags()]);
    put_u32(out, subscribe.sub);
    put_u64(out, subscribe.from);
    put_u16(out, subscribe.linger_ms);
    if let Some(author) = subscribe.author {
        put_u64(out, author);
    }
    let clauses = subscribe.clauses.as_slice();
    out.push(clauses.len() as u8);
    for clause in clauses {
        put_u64(out, clause.mask);
        put_u64(out, clause.value);
        out.push(clause.op);
    }
    put_stream(out, subscribe.stream)?;
    if let Some(name) = subscribe.name {
        put_str(out, name)?;
    }
    Ok(())
}

fn encode_publish(publish: &Publish<'_>, out: &mut Vec<u8>) -> Result<(), ProtoError> {
    let flags = if publish.state.is_some() {
        FLAG_HAS_STATE
    } else {
        0
    };
    out.extend_from_slice(&[TYPE_PUBLISH, flags]);
    put_u32(out, publish.handle);
    put_u64(out, publish.producer_id);
    put_u64(out, publish.producer_seq);
    out.push(publish.ack as u8);
    if let Some(state) = &publish.state {
        put_u64(out, state.expect_ver);
        out.push(state.op as u8);
        put_bytes(out, state.doc)?;
    }
    put_bytes(out, publish.records)
}
