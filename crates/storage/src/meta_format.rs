//! A stream's metadata as bytes (SPEC section 7.6, D18): the value in the local store and the
//! blob in a checkpoint. All little-endian.
//!
//! ```text
//! meta   := crc32c u32 | flags u32 | name str32 | last_id u64 | floor_id u64 | expired_through u64 | applied_id u64 |
//!           state_ver u64 | state bytes32 | default_ack u8 | min_ack u8 | 0 u16 | 0 u32 |
//!           retention_ms u64 |
//!           n u32 | n × cursor | n u32 | n × sink | n u32 | n × extent |
//!           n u32 | n × dropped_id_first u64
//! cursor := name str32 | id u64
//! sink   := sink_id u64 | kind str32 | url opt | target opt | filter opt | on_expire str32 |
//!           concurrency u32 | paused u8 | secret opt | interval_ms u64 | budget_bytes u64 |
//!           pause_reason opt | n u32 | n × (first_id u64 | last_id u64) | gap_count u64
//! extent := path str32 | offset u64 | len_bytes u64 | id_first u64 | id_last u64 | block_count u32
//! str32  := len u32 | bytes        bytes32 likewise
//! opt    := present u8 | str32 if present
//! ```
//!
//! `flags` bit 0 is "deleted" (D20). The CRC covers everything after it.

use std::fmt;

use bytes::Bytes;
use format::crc32c;

const FLAG_DELETED: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaCursor {
    pub name: String,
    pub id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaSink {
    pub sink_id: u64,
    pub kind: String,
    pub url: Option<String>,
    pub target: Option<String>,
    pub filter: Option<String>,
    pub on_expire: String,
    pub concurrency: u32,
    pub paused: bool,
    /// `webhook.*`: the HMAC key (D29).
    pub secret: Option<String>,
    /// `webhook.notify`: least time between sends. 0 takes the node's.
    pub interval_ms: u64,
    /// What the sink may hold at once. 0 takes the node's.
    pub budget_bytes: u64,
    pub pause_reason: Option<String>,
    /// The latest gaps a skip recorded (D30), and how many there have been.
    pub gaps: Vec<MetaSinkGap>,
    pub gap_count: u64,
}

/// Ids a sink skipped because they expired unread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaSinkGap {
    pub first_id: u64,
    pub last_id: u64,
}

/// `id range → location` of section 7.6.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaExtent {
    pub path: String,
    pub offset: u64,
    pub len_bytes: u64,
    pub id_first: u64,
    pub id_last: u64,
    pub block_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamMeta {
    pub name: String,
    /// A tombstone (D20): `last_id` is what a stream made under this name must stay above.
    pub deleted: bool,
    pub last_id: u64,
    /// The last id of a deleted stream of this name. Nothing at or below it is readable.
    pub floor_id: u64,
    /// The highest id in an extent retention took away. A position below it has lost records.
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
    pub extents: Vec<MetaExtent>,
    /// `id_first` of extents this entry takes away, which compaction replaced or retention
    /// expired. It applies to what older entries hold, not to `extents` beside it.
    pub dropped_extents: Vec<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaError(pub &'static str);

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

/// One extent's bytes, which the local store keeps under a key of its own.
pub fn encode_extent(out: &mut Vec<u8>, extent: &MetaExtent) {
    put_bytes(out, extent.path.as_bytes());
    put_u64(out, extent.offset);
    put_u64(out, extent.len_bytes);
    put_u64(out, extent.id_first);
    put_u64(out, extent.id_last);
    put_u32(out, extent.block_count);
}

pub fn encode_meta(meta: &StreamMeta) -> Vec<u8> {
    let mut out = Vec::with_capacity(128 + meta.name.len() + meta.state.len());
    put_u32(&mut out, 0);
    put_u32(&mut out, if meta.deleted { FLAG_DELETED } else { 0 });
    put_bytes(&mut out, meta.name.as_bytes());
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
        put_bytes(&mut out, cursor.name.as_bytes());
        put_u64(&mut out, cursor.id);
    }
    put_u32(&mut out, meta.sinks.len() as u32);
    for sink in &meta.sinks {
        put_u64(&mut out, sink.sink_id);
        put_bytes(&mut out, sink.kind.as_bytes());
        put_opt(&mut out, sink.url.as_deref());
        put_opt(&mut out, sink.target.as_deref());
        put_opt(&mut out, sink.filter.as_deref());
        put_bytes(&mut out, sink.on_expire.as_bytes());
        put_u32(&mut out, sink.concurrency);
        out.push(u8::from(sink.paused));
        put_opt(&mut out, sink.secret.as_deref());
        put_u64(&mut out, sink.interval_ms);
        put_u64(&mut out, sink.budget_bytes);
        put_opt(&mut out, sink.pause_reason.as_deref());
        put_u32(&mut out, sink.gaps.len() as u32);
        for gap in &sink.gaps {
            put_u64(&mut out, gap.first_id);
            put_u64(&mut out, gap.last_id);
        }
        put_u64(&mut out, sink.gap_count);
    }
    put_u32(&mut out, meta.extents.len() as u32);
    for extent in &meta.extents {
        encode_extent(&mut out, extent);
    }
    put_u32(&mut out, meta.dropped_extents.len() as u32);
    for id_first in &meta.dropped_extents {
        out.extend_from_slice(&id_first.to_le_bytes());
    }
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
            .ok_or(MetaError("a length runs off the end"))?;
        let taken = self
            .bytes
            .get(self.at..end)
            .ok_or(MetaError("a length runs off the end"))?;
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
            .map_err(|_| MetaError("text isn't UTF-8"))
    }

    fn opt(&mut self) -> Result<Option<String>, MetaError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.text().map(Some),
            _ => Err(MetaError("an option flag isn't 0 or 1")),
        }
    }

    fn extent(&mut self) -> Result<MetaExtent, MetaError> {
        Ok(MetaExtent {
            path: self.text()?,
            offset: self.u64()?,
            len_bytes: self.u64()?,
            id_first: self.u64()?,
            id_last: self.u64()?,
            block_count: self.u32()?,
        })
    }

    /// A count can't promise more than the bytes left could hold, so a corrupt one can't
    /// make a huge allocation.
    fn count(&mut self, min_entry_bytes: usize) -> Result<usize, MetaError> {
        let count = self.u32()? as usize;
        if count > (self.bytes.len() - self.at) / min_entry_bytes {
            return Err(MetaError("a count runs off the end"));
        }
        Ok(count)
    }
}

pub fn decode_extent(bytes: &[u8]) -> Result<MetaExtent, MetaError> {
    let mut reader = Reader { bytes, at: 0 };
    let extent = reader.extent()?;
    if reader.at != bytes.len() {
        return Err(MetaError("bytes after the extent"));
    }
    Ok(extent)
}

pub fn decode_meta(bytes: &[u8]) -> Result<StreamMeta, MetaError> {
    let mut reader = Reader { bytes, at: 0 };
    let crc = reader.u32()?;
    if crc != crc32c(&bytes[4..]) {
        return Err(MetaError("the CRC doesn't match"));
    }
    let flags = reader.u32()?;
    if flags & !FLAG_DELETED != 0 {
        return Err(MetaError("an unknown flag"));
    }
    let name = reader.text()?;
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
        return Err(MetaError("padding isn't zero"));
    }
    let retention_ms = reader.u64()?;
    let mut meta = StreamMeta {
        name,
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
    for _ in 0..reader.count(12)? {
        meta.cursors.push(MetaCursor {
            name: reader.text()?,
            id: reader.u64()?,
        });
    }
    for _ in 0..reader.count(55)? {
        let sink_id = reader.u64()?;
        let kind = reader.text()?;
        let (url, target, filter) = (reader.opt()?, reader.opt()?, reader.opt()?);
        let on_expire = reader.text()?;
        let concurrency = reader.u32()?;
        let paused = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(MetaError("a paused flag isn't 0 or 1")),
        };
        let secret = reader.opt()?;
        let (interval_ms, budget_bytes) = (reader.u64()?, reader.u64()?);
        let pause_reason = reader.opt()?;
        let mut gaps = Vec::new();
        for _ in 0..reader.count(16)? {
            gaps.push(MetaSinkGap {
                first_id: reader.u64()?,
                last_id: reader.u64()?,
            });
        }
        let gap_count = reader.u64()?;
        meta.sinks.push(MetaSink {
            sink_id,
            kind,
            url,
            target,
            filter,
            on_expire,
            concurrency,
            paused,
            secret,
            interval_ms,
            budget_bytes,
            pause_reason,
            gaps,
            gap_count,
        });
    }
    for _ in 0..reader.count(40)? {
        meta.extents.push(reader.extent()?);
    }
    for _ in 0..reader.count(8)? {
        meta.dropped_extents.push(reader.u64()?);
    }
    if reader.at != bytes.len() {
        return Err(MetaError("bytes after the metadata"));
    }
    Ok(meta)
}
