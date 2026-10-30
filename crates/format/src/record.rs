//! The record: a 32-byte header, then data padded to 8 (SPEC section 2).

use crate::FormatError;

pub const ID_OFFSET: usize = 0;
pub const OPTIONS_OFFSET: usize = 8;
pub const AUTHOR_OFFSET: usize = 16;
pub const LEN_OFFSET: usize = 24;
pub const COUNT_OFFSET: usize = 28;
pub const CODEC_OFFSET: usize = 30;
pub const DATA_OFFSET: usize = 32;
pub const HEADER_BYTES: usize = 32;

/// `len` rounded up to a multiple of 8.
pub fn padded_data_bytes(len: u32) -> usize {
    (len as usize + 7) & !7
}

/// A decoded record header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    pub id: u64,
    pub options: u64,
    pub author: u64,
    pub len: u32,
    pub count: u16,
    pub codec: u16,
}

pub(crate) fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut field = [0u8; 8];
    field.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(field)
}

impl RecordHeader {
    /// Reads the fixed fields. Checks only that 32 bytes are present.
    pub fn read(bytes: &[u8]) -> Result<RecordHeader, FormatError> {
        if bytes.len() < HEADER_BYTES {
            return Err(FormatError::TruncatedHeader);
        }
        Ok(RecordHeader {
            id: read_u64(bytes, ID_OFFSET),
            options: read_u64(bytes, OPTIONS_OFFSET),
            author: read_u64(bytes, AUTHOR_OFFSET),
            len: u32::from_le_bytes([
                bytes[LEN_OFFSET],
                bytes[LEN_OFFSET + 1],
                bytes[LEN_OFFSET + 2],
                bytes[LEN_OFFSET + 3],
            ]),
            count: u16::from_le_bytes([bytes[COUNT_OFFSET], bytes[COUNT_OFFSET + 1]]),
            codec: u16::from_le_bytes([bytes[CODEC_OFFSET], bytes[CODEC_OFFSET + 1]]),
        })
    }

    pub fn to_bytes(&self) -> [u8; HEADER_BYTES] {
        let mut out = [0u8; HEADER_BYTES];
        out[ID_OFFSET..ID_OFFSET + 8].copy_from_slice(&self.id.to_le_bytes());
        out[OPTIONS_OFFSET..OPTIONS_OFFSET + 8].copy_from_slice(&self.options.to_le_bytes());
        out[AUTHOR_OFFSET..AUTHOR_OFFSET + 8].copy_from_slice(&self.author.to_le_bytes());
        out[LEN_OFFSET..LEN_OFFSET + 4].copy_from_slice(&self.len.to_le_bytes());
        out[COUNT_OFFSET..COUNT_OFFSET + 2].copy_from_slice(&self.count.to_le_bytes());
        out[CODEC_OFFSET..CODEC_OFFSET + 2].copy_from_slice(&self.codec.to_le_bytes());
        out
    }

    /// Header, data and padding together.
    pub fn total_bytes(&self) -> usize {
        HEADER_BYTES + padded_data_bytes(self.len)
    }

    /// The id of the record's last item. Saturates on a corrupt id near `u64::MAX`.
    pub fn last_id(&self) -> u64 {
        self.id.saturating_add(u64::from(self.count.max(1)) - 1)
    }

    /// A record with `author = 0` belongs to the server (SPEC section 2.2).
    pub fn is_system(&self) -> bool {
        self.author == 0
    }
}

/// A record inside a larger byte slice. `data` excludes the padding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordView<'a> {
    pub header: RecordHeader,
    pub data: &'a [u8],
}

/// Where the data ends and the whole record ends, relative to the record's start,
/// given `available_bytes` from there. Rejects `count = 0` and a `len` that overruns.
pub(crate) fn record_bounds(
    header: &RecordHeader,
    available_bytes: usize,
) -> Result<(usize, usize), FormatError> {
    if header.count == 0 {
        return Err(FormatError::ZeroCount);
    }
    let total_bytes = header.total_bytes();
    if total_bytes > available_bytes {
        return Err(FormatError::TruncatedData);
    }
    Ok((DATA_OFFSET + header.len as usize, total_bytes))
}

/// Decodes the record at the start of `bytes`, returning it and its total size.
///
/// Rejects `count = 0`, a `len` past the end of `bytes`, and non-zero padding.
/// It slices the data without reading it.
pub fn decode_record(bytes: &[u8]) -> Result<(RecordView<'_>, usize), FormatError> {
    let header = RecordHeader::read(bytes)?;
    let (data_end, total_bytes) = record_bounds(&header, bytes.len())?;
    if bytes[data_end..total_bytes].iter().any(|byte| *byte != 0) {
        return Err(FormatError::NonZeroPadding);
    }
    let data = &bytes[DATA_OFFSET..data_end];
    Ok((RecordView { header, data }, total_bytes))
}

/// Appends a record: header, data, then zero padding up to a multiple of 8.
pub fn encode_record(
    out: &mut Vec<u8>,
    header: &RecordHeader,
    data: &[u8],
) -> Result<(), FormatError> {
    if header.count == 0 {
        return Err(FormatError::ZeroCount);
    }
    if header.len as usize != data.len() {
        return Err(FormatError::LengthMismatch);
    }
    out.extend_from_slice(&header.to_bytes());
    out.extend_from_slice(data);
    out.resize(out.len() + padded_data_bytes(header.len) - data.len(), 0);
    Ok(())
}

/// Walks consecutive records. Stops after the first error.
#[derive(Debug, Clone)]
pub struct Records<'a> {
    remaining: &'a [u8],
}

impl<'a> Records<'a> {
    pub fn new(bytes: &'a [u8]) -> Records<'a> {
        Records { remaining: bytes }
    }
}

impl<'a> Iterator for Records<'a> {
    type Item = Result<RecordView<'a>, FormatError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining.is_empty() {
            return None;
        }
        match decode_record(self.remaining) {
            Ok((view, total_bytes)) => {
                self.remaining = &self.remaining[total_bytes..];
                Some(Ok(view))
            }
            Err(error) => {
                self.remaining = &[];
                Some(Err(error))
            }
        }
    }
}
