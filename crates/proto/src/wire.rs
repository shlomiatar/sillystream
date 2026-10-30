//! Reading and writing the field types of section 11.1.

use crate::ProtoError;

/// The version this code sends in `HELLO` (D41).
pub const PROTOCOL_VERSION: u16 = 1;
/// The `max_frame` a server starts with (D6d).
pub const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Stream ids are UTF-8 names without `/` (SPEC section 1). An empty name isn't a name.
pub fn is_valid_stream_id(stream: &str) -> bool {
    !stream.is_empty() && !stream.as_bytes().contains(&b'/')
}

/// Splits a frame into its type, flags and body, after the size limit.
pub(crate) fn split_frame(
    frame: &[u8],
    max_frame_bytes: usize,
) -> Result<(u8, u8, Reader<'_>), ProtoError> {
    if frame.len() > max_frame_bytes {
        return Err(ProtoError::FrameTooLarge);
    }
    match frame {
        [frame_type, flags, body @ ..] => Ok((*frame_type, *flags, Reader { rest: body })),
        _ => Err(ProtoError::Truncated),
    }
}

pub(crate) fn require_flags(flags: u8, allowed: u8) -> Result<(), ProtoError> {
    if flags & !allowed != 0 {
        return Err(ProtoError::UnknownFlags);
    }
    Ok(())
}

pub(crate) struct Reader<'a> {
    rest: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, bytes: usize) -> Result<&'a [u8], ProtoError> {
        let rest = self.rest;
        if bytes > rest.len() {
            return Err(ProtoError::Truncated);
        }
        let (taken, rest) = rest.split_at(bytes);
        self.rest = rest;
        Ok(taken)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], ProtoError> {
        let mut array = [0u8; N];
        array.copy_from_slice(self.take(N)?);
        Ok(array)
    }

    pub fn u8(&mut self) -> Result<u8, ProtoError> {
        Ok(self.take_array::<1>()?[0])
    }

    pub fn u16(&mut self) -> Result<u16, ProtoError> {
        Ok(u16::from_le_bytes(self.take_array()?))
    }

    pub fn u32(&mut self) -> Result<u32, ProtoError> {
        Ok(u32::from_le_bytes(self.take_array()?))
    }

    pub fn u64(&mut self) -> Result<u64, ProtoError> {
        Ok(u64::from_le_bytes(self.take_array()?))
    }

    pub fn str(&mut self) -> Result<&'a str, ProtoError> {
        let len = self.u16()? as usize;
        std::str::from_utf8(self.take(len)?).map_err(|_| ProtoError::InvalidUtf8)
    }

    pub fn stream(&mut self) -> Result<&'a str, ProtoError> {
        let stream = self.str()?;
        if !is_valid_stream_id(stream) {
            return Err(ProtoError::InvalidStreamId);
        }
        Ok(stream)
    }

    pub fn bytes(&mut self) -> Result<&'a [u8], ProtoError> {
        // A u32 always fits usize on the 64-bit targets we build for; on anything
        // smaller a length that doesn't fit is truncated input.
        let len = usize::try_from(self.u32()?).map_err(|_| ProtoError::Truncated)?;
        self.take(len)
    }

    pub fn finish(self) -> Result<(), ProtoError> {
        if !self.rest.is_empty() {
            return Err(ProtoError::TrailingBytes);
        }
        Ok(())
    }
}

pub(crate) fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_str(out: &mut Vec<u8>, text: &str) -> Result<(), ProtoError> {
    let len = u16::try_from(text.len()).map_err(|_| ProtoError::StrTooLong)?;
    put_u16(out, len);
    out.extend_from_slice(text.as_bytes());
    Ok(())
}

pub(crate) fn put_stream(out: &mut Vec<u8>, stream: &str) -> Result<(), ProtoError> {
    if !is_valid_stream_id(stream) {
        return Err(ProtoError::InvalidStreamId);
    }
    put_str(out, stream)
}

pub(crate) fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ProtoError> {
    let len = u32::try_from(bytes.len()).map_err(|_| ProtoError::BytesTooLong)?;
    put_u32(out, len);
    out.extend_from_slice(bytes);
    Ok(())
}

/// Runs an encoder and leaves `out` as it was if the encoder fails.
pub(crate) fn encode_with(
    out: &mut Vec<u8>,
    encode: impl FnOnce(&mut Vec<u8>) -> Result<(), ProtoError>,
) -> Result<(), ProtoError> {
    let start = out.len();
    let result = encode(out);
    if result.is_err() {
        out.truncate(start);
    }
    result
}
