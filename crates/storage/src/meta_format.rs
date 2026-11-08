//! A stream's metadata as bytes (SPEC section 7.6, D18): the value in the local store and the
//! blob in a checkpoint. All little-endian.
//!
//! ```text
//! meta   := crc32c u32 | version u32 | flags u32 | stream key | last_id u64 | floor_id u64 | expired_through u64 | applied_id u64 |
//!           state_ver u64 | state bytes32 | default_ack u8 | min_ack u8 | 0 u16 | 0 u32 |
//!           retention_ms u64 |
//!           n u32 | n × cursor | n u32 | n × sink | ids
//! cursor := name key | id u64
//! saved  := version u32 | sink      a sink alone, as the local store keeps it between seals
//! sink   := sink_id u64 | kind str32 | url opt | target optkey | filter opt | on_expire str32 |
//!           paused u8 | secret opt | budget_bytes u64 | pause_reason opt |
//!           last_gap (present u8 | first_id u64 | last_id u64 if present) | gap_count u64 |
//!           compression opt
//! ids    := n u32 | n × delta varint      object ids, ascending, each less the one before
//! str32  := len u32 | bytes        bytes32 likewise
//! opt    := present u8 | str32 if present
//! key    := a key hash, 16 bytes (SPEC section 1)
//! optkey := present u8 | key if present
//! ```
//!
//! `version` is `META_VERSION`, and anything else is refused with both numbers: an older build's
//! bytes would otherwise be misread. `flags` bit 0 is "deleted" (D20). The CRC covers everything
//! after it.

use std::fmt;

use bytes::Bytes;
use format::{KeyHash, crc32c};

use crate::object::ObjectId;

const FLAG_DELETED: u32 = 1;
/// The layout above. Raise it whenever the layout changes. Before it had a field of its own,
/// the word where it sits was `flags`, 0 or 1, so 2 is the first.
pub const META_VERSION: u32 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaCursor {
    /// The cursor name's key hash (section 5.1).
    pub name: KeyHash,
    pub id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaSink {
    pub sink_id: u64,
    pub kind: String,
    pub url: Option<String>,
    /// `forward`: the target stream (section 9.1).
    pub target: Option<KeyHash>,
    pub filter: Option<String>,
    pub on_expire: String,
    pub paused: bool,
    /// `webhook.*`: the HMAC key (D29).
    pub secret: Option<String>,
    /// What the sink may hold at once. 0 takes the node's.
    pub budget_bytes: u64,
    pub pause_reason: Option<String>,
    /// The latest gap a skip recorded (D30), and how many there have been.
    pub last_gap: Option<MetaSinkGap>,
    pub gap_count: u64,
    /// `webhook.deliver`: how each POST's body is compressed, if it is.
    pub compression: Option<String>,
}

/// Ids a sink skipped because they expired unread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaSinkGap {
    pub first_id: u64,
    pub last_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamMeta {
    pub stream: KeyHash,
    /// A tombstone (D20): `last_id` is what a stream made under this id must stay above.
    pub deleted: bool,
    pub last_id: u64,
    /// The last id of a deleted stream of this name. Nothing at or below it is readable.
    pub floor_id: u64,
    /// The highest expired id (section 7.4). A position below it has lost records.
    pub expired_through: u64,
    /// State includes every change up to this id, and no later one. A change above
    /// it comes from the WAL.
    pub applied_id: u64,
    pub state_ver: u64,
    pub state: Bytes,
    pub default_ack_level: u8,
    pub min_ack_level: u8,
    pub retention_ms: u64,
    pub cursors: Vec<MetaCursor>,
    pub sinks: Vec<MetaSink>,
    /// The object list (section 7.6): the ids of the objects holding its records, ascending.
    pub objects: Vec<ObjectId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaError(pub std::borrow::Cow<'static, str>);

impl MetaError {
    pub const fn new(text: &'static str) -> MetaError {
        MetaError(std::borrow::Cow::Borrowed(text))
    }

    fn version(found: u32) -> MetaError {
        MetaError(std::borrow::Cow::Owned(format!(
            "format version {found}, and this build reads only {META_VERSION}: the data is from \
             another build"
        )))
    }
}

impl fmt::Display for MetaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "stream metadata: {}", self.0)
    }
}

impl std::error::Error for MetaError {}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

fn put_opt(out: &mut Vec<u8>, text: Option<&str>) {
    match text {
        Some(text) => {
            out.push(1);
            put_bytes(out, text.as_bytes());
        }
        None => out.push(0),
    }
}

/// Ascending object ids, each as a varint less the one before (section 7.6).
pub fn encode_ids(out: &mut Vec<u8>, ids: &[ObjectId]) {
    put_u32(out, ids.len() as u32);
    let mut previous = 0;
    for id in ids {
        let mut delta = id.0 - previous;
        while delta >= 0x80 {
            out.push(delta as u8 | 0x80);
            delta >>= 7;
        }
        out.push(delta as u8);
        previous = id.0;
    }
}

pub fn decode_ids(bytes: &[u8]) -> Result<Vec<ObjectId>, MetaError> {
    let mut reader = Reader { bytes, at: 0 };
    let ids = reader.ids()?;
    if reader.at != bytes.len() {
        return Err(MetaError::new("bytes after the ids"));
    }
    Ok(ids)
}

fn put_sink(out: &mut Vec<u8>, sink: &MetaSink) {
    put_u64(out, sink.sink_id);
    put_bytes(out, sink.kind.as_bytes());
    put_opt(out, sink.url.as_deref());
    match sink.target {
        Some(target) => {
            out.push(1);
            out.extend_from_slice(&target.0);
        }
        None => out.push(0),
    }
    put_opt(out, sink.filter.as_deref());
    put_bytes(out, sink.on_expire.as_bytes());
    out.push(u8::from(sink.paused));
    put_opt(out, sink.secret.as_deref());
    put_u64(out, sink.budget_bytes);
    put_opt(out, sink.pause_reason.as_deref());
    match sink.last_gap {
        Some(gap) => {
            out.push(1);
            put_u64(out, gap.first_id);
            put_u64(out, gap.last_id);
        }
        None => out.push(0),
    }
    put_u64(out, sink.gap_count);
    put_opt(out, sink.compression.as_deref());
}

/// One sink alone, as the local store keeps it between seals (section 5.1).
pub fn encode_sink(sink: &MetaSink) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    put_u32(&mut out, META_VERSION);
    put_sink(&mut out, sink);
    out
}

pub fn decode_sink(bytes: &[u8]) -> Result<MetaSink, MetaError> {
    let mut reader = Reader { bytes, at: 0 };
    let version = reader.u32()?;
    if version != META_VERSION {
        return Err(MetaError::version(version));
    }
    let sink = reader.sink()?;
    if reader.at != bytes.len() {
        return Err(MetaError::new("bytes after the sink"));
    }
    Ok(sink)
}

pub fn encode_meta(meta: &StreamMeta) -> Vec<u8> {
    let mut out = Vec::with_capacity(128 + meta.state.len());
    put_u32(&mut out, 0);
    put_u32(&mut out, META_VERSION);
    put_u32(&mut out, if meta.deleted { FLAG_DELETED } else { 0 });
    out.extend_from_slice(&meta.stream.0);
    put_u64(&mut out, meta.last_id);
    put_u64(&mut out, meta.floor_id);
    put_u64(&mut out, meta.expired_through);
    put_u64(&mut out, meta.applied_id);
    put_u64(&mut out, meta.state_ver);
    put_bytes(&mut out, &meta.state);
    out.push(meta.default_ack_level);
    out.push(meta.min_ack_level);
    out.extend_from_slice(&[0; 6]);
    put_u64(&mut out, meta.retention_ms);
    put_u32(&mut out, meta.cursors.len() as u32);
    for cursor in &meta.cursors {
        out.extend_from_slice(&cursor.name.0);
        put_u64(&mut out, cursor.id);
    }
    put_u32(&mut out, meta.sinks.len() as u32);
    for sink in &meta.sinks {
        put_sink(&mut out, sink);
    }
    encode_ids(&mut out, &meta.objects);
    let crc = crc32c(&out[4..]);
    out[..4].copy_from_slice(&crc.to_le_bytes());
    out
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], MetaError> {
        let end = self
            .at
            .checked_add(len)
            .ok_or(MetaError::new("a length runs off the end"))?;
        let taken = self
            .bytes
            .get(self.at..end)
            .ok_or(MetaError::new("a length runs off the end"))?;
        self.at = end;
        Ok(taken)
    }

    fn u8(&mut self) -> Result<u8, MetaError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, MetaError> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self) -> Result<u64, MetaError> {
        let mut field = [0u8; 8];
        field.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(field))
    }

    fn bytes32(&mut self) -> Result<&'a [u8], MetaError> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn text(&mut self) -> Result<String, MetaError> {
        std::str::from_utf8(self.bytes32()?)
            .map(str::to_owned)
            .map_err(|_| MetaError::new("text isn't UTF-8"))
    }

    fn opt(&mut self) -> Result<Option<String>, MetaError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.text().map(Some),
            _ => Err(MetaError::new("an option flag isn't 0 or 1")),
        }
    }

    fn key(&mut self) -> Result<KeyHash, MetaError> {
        Ok(KeyHash::from_slice(self.take(KeyHash::BYTES)?).unwrap())
    }

    fn opt_key(&mut self) -> Result<Option<KeyHash>, MetaError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.key().map(Some),
            _ => Err(MetaError::new("an option flag isn't 0 or 1")),
        }
    }

    fn ids(&mut self) -> Result<Vec<ObjectId>, MetaError> {
        let count = self.count(1)?;
        let mut ids = Vec::with_capacity(count);
        let mut previous = 0u64;
        for index in 0..count {
            let mut delta = 0u64;
            let mut shift = 0;
            loop {
                let byte = self.u8()?;
                if shift > 63 {
                    return Err(MetaError::new("a varint is too long"));
                }
                delta |= u64::from(byte & 0x7F) << shift;
                if byte & 0x80 == 0 {
                    break;
                }
                shift += 7;
            }
            if index > 0 && delta == 0 {
                return Err(MetaError::new("object ids aren't ascending"));
            }
            previous = previous
                .checked_add(delta)
                .ok_or(MetaError::new("an object id is too big"))?;
            ids.push(ObjectId(previous));
        }
        Ok(ids)
    }

    fn sink(&mut self) -> Result<MetaSink, MetaError> {
        let sink_id = self.u64()?;
        let kind = self.text()?;
        let (url, target, filter) = (self.opt()?, self.opt_key()?, self.opt()?);
        let on_expire = self.text()?;
        let paused = match self.u8()? {
            0 => false,
            1 => true,
            _ => return Err(MetaError::new("a paused flag isn't 0 or 1")),
        };
        let secret = self.opt()?;
        let budget_bytes = self.u64()?;
        let pause_reason = self.opt()?;
        let last_gap = match self.u8()? {
            0 => None,
            1 => Some(MetaSinkGap {
                first_id: self.u64()?,
                last_id: self.u64()?,
            }),
            _ => return Err(MetaError::new("a last gap flag isn't 0 or 1")),
        };
        let gap_count = self.u64()?;
        let compression = self.opt()?;
        Ok(MetaSink {
            sink_id,
            kind,
            url,
            target,
            filter,
            on_expire,
            paused,
            secret,
            budget_bytes,
            pause_reason,
            last_gap,
            gap_count,
            compression,
        })
    }

    /// A count can't promise more than the bytes left could hold, so a corrupt one can't
    /// make a huge allocation.
    fn count(&mut self, min_entry_bytes: usize) -> Result<usize, MetaError> {
        let count = self.u32()? as usize;
        if count > (self.bytes.len() - self.at) / min_entry_bytes {
            return Err(MetaError::new("a count runs off the end"));
        }
        Ok(count)
    }
}

pub fn decode_meta(bytes: &[u8]) -> Result<StreamMeta, MetaError> {
    let mut reader = Reader { bytes, at: 0 };
    let crc = reader.u32()?;
    if crc != crc32c(&bytes[4..]) {
        return Err(MetaError::new("the CRC doesn't match"));
    }
    let version = reader.u32()?;
    if version != META_VERSION {
        return Err(MetaError::version(version));
    }
    let flags = reader.u32()?;
    if flags & !FLAG_DELETED != 0 {
        return Err(MetaError::new("an unknown flag"));
    }
    let stream = reader.key()?;
    let (last_id, floor_id, expired_through, applied_id, state_ver) = (
        reader.u64()?,
        reader.u64()?,
        reader.u64()?,
        reader.u64()?,
        reader.u64()?,
    );
    let state = Bytes::copy_from_slice(reader.bytes32()?);
    let (default_ack_level, min_ack_level) = (reader.u8()?, reader.u8()?);
    if reader.take(6)? != [0; 6] {
        return Err(MetaError::new("padding isn't zero"));
    }
    let retention_ms = reader.u64()?;
    let mut meta = StreamMeta {
        stream,
        deleted: flags & FLAG_DELETED != 0,
        last_id,
        floor_id,
        expired_through,
        applied_id,
        state_ver,
        state,
        default_ack_level,
        min_ack_level,
        retention_ms,
        ..StreamMeta::default()
    };
    for _ in 0..reader.count(KeyHash::BYTES + 8)? {
        meta.cursors.push(MetaCursor {
            name: reader.key()?,
            id: reader.u64()?,
        });
    }
    for _ in 0..reader.count(40)? {
        meta.sinks.push(reader.sink()?);
    }
    meta.objects = reader.ids()?;
    if reader.at != bytes.len() {
        return Err(MetaError::new("bytes after the metadata"));
    }
    Ok(meta)
}
