use std::fmt;

/// Why bytes could not be decoded or a publish was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatError {
    /// Fewer than 32 bytes where a record header should start.
    TruncatedHeader,
    /// The header's `len`, padded to 8, runs past the end of the bytes.
    TruncatedData,
    /// `count = 0`.
    ZeroCount,
    /// A padding byte after the data is not 0.
    NonZeroPadding,
    /// A published record has a non-zero `id`.
    NonZeroId,
    /// A published record has a non-zero `author`.
    NonZeroAuthor,
    /// A forwarded record has `author = 0`; system records are never forwarded (D28).
    SystemRecordForwarded,
    /// A publish has more items than `ACK.count` can express (D34).
    TooManyItems,
    /// A header's `len` differs from the data given to the encoder.
    LengthMismatch,
    /// Data does not fit the `len` field.
    DataTooLong,
    /// A system record was expected and `author` is not 0.
    NotSystemRecord,
    /// A system record's data is shorter than its `actor` field.
    TruncatedSystemRecord,
    /// A filter has more than 4 clauses (D2).
    TooManyClauses,
    /// A filter clause has an `op` other than 0 or 1 (D1).
    UnknownClauseOp,
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            FormatError::TruncatedHeader => "record header is truncated",
            FormatError::TruncatedData => "record data runs past the end of the bytes",
            FormatError::ZeroCount => "record count is 0",
            FormatError::NonZeroPadding => "record padding is not 0",
            FormatError::NonZeroId => "published record has a non-zero id",
            FormatError::NonZeroAuthor => "published record has a non-zero author",
            FormatError::SystemRecordForwarded => "forwarded record is a system record",
            FormatError::TooManyItems => "publish has more items than an ack can count",
            FormatError::LengthMismatch => "header len differs from the data length",
            FormatError::DataTooLong => "data does not fit the len field",
            FormatError::NotSystemRecord => "record is not a system record",
            FormatError::TruncatedSystemRecord => "system record data has no actor",
            FormatError::TooManyClauses => "filter has more than 4 clauses",
            FormatError::UnknownClauseOp => "unknown filter clause op",
        };
        f.write_str(text)
    }
}

impl std::error::Error for FormatError {}
