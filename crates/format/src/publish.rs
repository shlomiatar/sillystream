//! Publish validation and in-place stamping (SPEC section 3).
//!
//! Both walk record headers only and jump over the data, so the cost depends on
//! the number of records and not on their size. Validation reads headers and
//! padding, and never a byte of data.

use std::num::NonZeroU64;

use crate::record::{AUTHOR_OFFSET, ID_OFFSET, decode_record};
use crate::{FormatError, RecordHeader};

/// Counts of a validated publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Totals {
    record_count: u32,
    item_count: u64,
}

fn check_records(
    bytes: &[u8],
    check_header: impl Fn(&RecordHeader) -> Result<(), FormatError>,
) -> Result<Totals, FormatError> {
    let mut offset = 0;
    let mut totals = Totals {
        record_count: 0,
        item_count: 0,
    };
    while offset < bytes.len() {
        let (view, total_bytes) = decode_record(&bytes[offset..])?;
        check_header(&view.header)?;
        totals.record_count += 1;
        totals.item_count += u64::from(view.header.count);
        offset += total_bytes;
    }
    if totals.item_count > u64::from(u32::MAX) {
        return Err(FormatError::TooManyItems);
    }
    Ok(totals)
}

/// Writes ids into every header, and `author` too when given.
fn stamp_records(bytes: &mut [u8], first_id: u64, author: Option<NonZeroU64>) {
    let mut offset = 0;
    let mut next_id = first_id;
    while offset < bytes.len() {
        // The bytes were validated and stamping leaves `len` and `count` alone,
        // so a whole header is present at every offset.
        let Ok(header) = RecordHeader::read(&bytes[offset..]) else {
            return;
        };
        bytes[offset + ID_OFFSET..offset + ID_OFFSET + 8].copy_from_slice(&next_id.to_le_bytes());
        if let Some(author) = author {
            bytes[offset + AUTHOR_OFFSET..offset + AUTHOR_OFFSET + 8]
                .copy_from_slice(&author.get().to_le_bytes());
        }
        next_id += u64::from(header.count);
        offset += header.total_bytes();
    }
}

/// A producer's records, validated and ready to stamp.
#[derive(Debug)]
pub struct ValidatedPublish<'a> {
    bytes: &'a mut [u8],
    totals: Totals,
}

/// Validates a publish's records as they arrive from a producer.
///
/// Rejects if lengths don't add up exactly to `bytes`, `count = 0`, `id` or
/// `author` isn't 0, or padding isn't 0. Empty `bytes` are valid here: whether a
/// publish may have no records depends on its state change (D6a).
pub fn validate_publish(bytes: &mut [u8]) -> Result<ValidatedPublish<'_>, FormatError> {
    let totals = check_records(bytes, |header| {
        if header.id != 0 {
            return Err(FormatError::NonZeroId);
        }
        if header.author != 0 {
            return Err(FormatError::NonZeroAuthor);
        }
        Ok(())
    })?;
    Ok(ValidatedPublish { bytes, totals })
}

impl ValidatedPublish<'_> {
    pub fn record_count(&self) -> u32 {
        self.totals.record_count
    }

    /// How many ids to allocate.
    pub fn item_count(&self) -> u64 {
        self.totals.item_count
    }

    /// Writes `id` and `author` into each header, in place. Item i of the
    /// publish has id `first_id + i`. `first_id` comes from `IdAllocator`, which
    /// guarantees the range fits. The author comes from the token and is never 0.
    pub fn stamp(&mut self, first_id: u64, author: NonZeroU64) {
        stamp_records(self.bytes, first_id, Some(author));
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.bytes
    }
}

/// A forward sink's records, validated and ready to stamp.
#[derive(Debug)]
pub struct ValidatedForward<'a> {
    bytes: &'a mut [u8],
    totals: Totals,
}

/// Validates records from a forward sink, which keep their original non-zero
/// author (section 9.1). Everything else matches `validate_publish`.
pub fn validate_forward(bytes: &mut [u8]) -> Result<ValidatedForward<'_>, FormatError> {
    let totals = check_records(bytes, |header| {
        if header.id != 0 {
            return Err(FormatError::NonZeroId);
        }
        if header.author == 0 {
            return Err(FormatError::SystemRecordForwarded);
        }
        Ok(())
    })?;
    Ok(ValidatedForward { bytes, totals })
}

impl ValidatedForward<'_> {
    pub fn record_count(&self) -> u32 {
        self.totals.record_count
    }

    pub fn item_count(&self) -> u64 {
        self.totals.item_count
    }

    /// Writes `id` into each header and leaves `author` as it is.
    pub fn stamp(&mut self, first_id: u64) {
        stamp_records(self.bytes, first_id, None);
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.bytes
    }
}
