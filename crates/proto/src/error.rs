use std::fmt;

/// The error codes of SPEC section 11.1, carried by `NACK` and `ERROR`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ErrorCode {
    Unauthorized = 1,
    NotFound = 2,
    VersionConflict = 3,
    RateLimited = 4,
    TooManySubscriptions = 5,
    FrameTooLarge = 6,
    RetentionExpired = 7,
    Malformed = 8,
}

impl ErrorCode {
    pub fn from_wire(code: u16) -> Option<ErrorCode> {
        Some(match code {
            1 => ErrorCode::Unauthorized,
            2 => ErrorCode::NotFound,
            3 => ErrorCode::VersionConflict,
            4 => ErrorCode::RateLimited,
            5 => ErrorCode::TooManySubscriptions,
            6 => ErrorCode::FrameTooLarge,
            7 => ErrorCode::RetentionExpired,
            8 => ErrorCode::Malformed,
            _ => return None,
        })
    }

    pub fn to_wire(self) -> u16 {
        self as u16
    }
}

/// Why a frame could not be decoded or encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtoError {
    /// The frame is longer than the `max_frame` limit.
    FrameTooLarge,
    /// A field runs past the end of the frame, or the frame has no `type | flags`.
    Truncated,
    /// Bytes remain after the last field.
    TrailingBytes,
    /// The `type` byte is not a frame of this direction.
    UnknownType,
    /// A flag bit this frame doesn't define is set.
    UnknownFlags,
    /// A `str` is not UTF-8.
    InvalidUtf8,
    /// A stream id is empty or contains `/` (SPEC section 1).
    InvalidStreamId,
    /// `nfilt` is above 4 (D2).
    TooManyClauses,
    /// A filter clause's `op` is not 0 or 1 (D1).
    UnknownClauseOp,
    /// `ack` is not a level from 0 to 3.
    UnknownAckLevel,
    /// A state `op` is not 1, 2 or 3.
    UnknownStateOp,
    /// `ok` is not 0 or 1.
    InvalidOk,
    /// `code` is not one of the codes in section 11.1.
    UnknownErrorCode,
    /// A `str` to encode is over 65,535 bytes.
    StrTooLong,
    /// A `bytes` to encode is over `u32::MAX` bytes.
    BytesTooLong,
}

impl ProtoError {
    /// The code a server reports this with.
    pub fn code(self) -> ErrorCode {
        match self {
            ProtoError::FrameTooLarge => ErrorCode::FrameTooLarge,
            _ => ErrorCode::Malformed,
        }
    }
}

impl fmt::Display for ProtoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            ProtoError::FrameTooLarge => "frame is larger than max_frame",
            ProtoError::Truncated => "frame is truncated",
            ProtoError::TrailingBytes => "frame has trailing bytes",
            ProtoError::UnknownType => "unknown frame type",
            ProtoError::UnknownFlags => "unknown flag bits",
            ProtoError::InvalidUtf8 => "string is not UTF-8",
            ProtoError::InvalidStreamId => "stream id is empty or contains '/'",
            ProtoError::TooManyClauses => "filter has more than 4 clauses",
            ProtoError::UnknownClauseOp => "unknown filter clause op",
            ProtoError::UnknownAckLevel => "unknown ack level",
            ProtoError::UnknownStateOp => "unknown state op",
            ProtoError::InvalidOk => "ok is not 0 or 1",
            ProtoError::UnknownErrorCode => "unknown error code",
            ProtoError::StrTooLong => "string is longer than 65,535 bytes",
            ProtoError::BytesTooLong => "bytes field is longer than u32::MAX",
        };
        f.write_str(text)
    }
}

impl std::error::Error for ProtoError {}
