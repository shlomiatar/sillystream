//! Publish validation and in-place stamping (SPEC section 3).
//!
//! Both walk record headers only and jump over the data, so the cost depends on
//! the number of records and not on their size. Validation reads headers and
//! padding, and never a byte of data: the walk is handed a `FrameView`, which has
//! no way to get at the bytes except a header or a padding check.

use std::num::NonZeroU64;
use std::ops::Range;

use crate::record::{AUTHOR_OFFSET, ID_OFFSET, record_bounds};
use crate::{FormatError, RecordHeader};

/// Counts of a validated publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Totals {
    record_count: u32,
    item_count: u64,
}

/// All that the validation walk can see of a frame. There is deliberately no
/// accessor that returns bytes, and tests replace this with a probe that records
/// each range asked for.
trait FrameView {
    fn len_bytes(&self) -> usize;
    fn header_at(&self, offset: usize) -> Result<RecordHeader, FormatError>;
    fn padding_is_zero(&self, range: Range<usize>) -> bool;
}

struct SliceFrame<'a>(&'a [u8]);

impl FrameView for SliceFrame<'_> {
    #[inline]
    fn len_bytes(&self) -> usize {
        self.0.len()
    }

    #[inline]
    fn header_at(&self, offset: usize) -> Result<RecordHeader, FormatError> {
        RecordHeader::read(&self.0[offset..])
    }

    #[inline]
    fn padding_is_zero(&self, range: Range<usize>) -> bool {
        self.0[range].iter().all(|byte| *byte == 0)
    }
}

fn check_records<F: FrameView>(
    frame: &F,
    check_header: impl Fn(&RecordHeader) -> Result<(), FormatError>,
) -> Result<Totals, FormatError> {
    let mut offset = 0;
    let mut totals = Totals {
        record_count: 0,
        item_count: 0,
    };
    while offset < frame.len_bytes() {
        let header = frame.header_at(offset)?;
        let (data_end, total_bytes) = record_bounds(&header, frame.len_bytes() - offset)?;
        if !frame.padding_is_zero(offset + data_end..offset + total_bytes) {
            return Err(FormatError::NonZeroPadding);
        }
        check_header(&header)?;
        totals.record_count += 1;
        totals.item_count += u64::from(header.count);
        offset += total_bytes;
    }
    if totals.item_count > u64::from(u32::MAX) {
        return Err(FormatError::TooManyItems);
    }
    Ok(totals)
}

fn check_publish<F: FrameView>(frame: &F) -> Result<Totals, FormatError> {
    check_records(frame, |header| {
        if header.id != 0 {
            return Err(FormatError::NonZeroId);
        }
        if header.author != 0 {
            return Err(FormatError::NonZeroAuthor);
        }
        Ok(())
    })
}

fn check_forward<F: FrameView>(frame: &F) -> Result<Totals, FormatError> {
    check_records(frame, |header| {
        if header.id != 0 {
            return Err(FormatError::NonZeroId);
        }
        if header.author == 0 {
            return Err(FormatError::SystemRecordForwarded);
        }
        Ok(())
    })
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
    let totals = check_publish(&SliceFrame(bytes))?;
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
    let totals = check_forward(&SliceFrame(bytes))?;
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

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use proptest::prelude::*;

    use super::*;
    use crate::{HEADER_BYTES, encode_record};

    /// A frame that records every byte range the walk asks for.
    struct ProbeFrame<'a> {
        bytes: &'a [u8],
        reads: RefCell<Vec<Range<usize>>>,
    }

    impl FrameView for ProbeFrame<'_> {
        fn len_bytes(&self) -> usize {
            self.bytes.len()
        }

        fn header_at(&self, offset: usize) -> Result<RecordHeader, FormatError> {
            let end = (offset + HEADER_BYTES).min(self.bytes.len());
            self.reads.borrow_mut().push(offset..end);
            RecordHeader::read(&self.bytes[offset..])
        }

        fn padding_is_zero(&self, range: Range<usize>) -> bool {
            self.reads.borrow_mut().push(range.clone());
            self.bytes[range].iter().all(|byte| *byte == 0)
        }
    }

    /// A frame of the given data, and where the data is in it.
    fn build(records: &[Vec<u8>]) -> (Vec<u8>, Vec<Range<usize>>) {
        let mut bytes = Vec::new();
        let mut data = Vec::new();
        for record in records {
            let header = RecordHeader {
                id: 0,
                options: 3,
                author: 0,
                len: record.len() as u32,
                count: 1,
                codec: 0,
            };
            let start = bytes.len() + HEADER_BYTES;
            encode_record(&mut bytes, &header, record).unwrap();
            data.push(start..start + record.len());
        }
        (bytes, data)
    }

    fn arb_records() -> impl Strategy<Value = Vec<Vec<u8>>> {
        proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..200), 0..12)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]

        /// The walk reads every header and padding byte, and no data byte.
        #[test]
        fn the_walk_reads_exactly_what_is_not_data(records in arb_records()) {
            let (bytes, data) = build(&records);
            let probe = ProbeFrame { bytes: &bytes, reads: RefCell::new(Vec::new()) };
            let totals = check_publish(&probe).unwrap();
            prop_assert_eq!(totals.record_count as usize, records.len());

            let mut touched = vec![false; bytes.len()];
            for range in probe.reads.borrow().iter() {
                touched[range.clone()].fill(true);
            }
            let mut is_data = vec![false; bytes.len()];
            for range in &data {
                is_data[range.clone()].fill(true);
            }
            for at in 0..bytes.len() {
                prop_assert_eq!(touched[at], !is_data[at], "byte {}", at);
            }
        }

        /// The same walk over real bytes and over the probe gives the same result.
        #[test]
        fn probe_and_slice_agree(
            records in arb_records(),
            changes in proptest::collection::vec((any::<usize>(), any::<u8>()), 0..4),
        ) {
            let (mut bytes, _) = build(&records);
            if !bytes.is_empty() {
                for (at, value) in changes {
                    let at = at % bytes.len();
                    bytes[at] = value;
                }
            }
            let probe = ProbeFrame { bytes: &bytes, reads: RefCell::new(Vec::new()) };
            let on_probe = check_publish(&probe);
            let mut copy = bytes.clone();
            let on_slice = validate_publish(&mut copy).map(|publish| publish.totals);
            prop_assert_eq!(on_probe, on_slice);
        }
    }

    /// Padding is where the walk's only non-header read goes, so pin its bounds.
    #[test]
    fn the_padding_read_is_only_the_padding() {
        let (bytes, _) = build(&[vec![9; 5]]);
        let probe = ProbeFrame {
            bytes: &bytes,
            reads: RefCell::new(Vec::new()),
        };
        check_publish(&probe).unwrap();
        assert_eq!(*probe.reads.borrow(), vec![0..32, 37..40]);
    }
}
