//! RFC 6455 frames, as much as the server needs (D23). This file is pure: it reads a header,
//! unmasks in place, and writes a header. Reading sockets is in `conn`.

use sha1::{Digest, Sha1};

pub const OP_CONTINUATION: u8 = 0x0;
pub const OP_TEXT: u8 = 0x1;
pub const OP_BINARY: u8 = 0x2;
pub const OP_CLOSE: u8 = 0x8;
pub const OP_PING: u8 = 0x9;
pub const OP_PONG: u8 = 0xA;

/// The longest header: 2 bytes, an 8-byte length and a mask.
pub const MAX_HEADER_BYTES: usize = 14;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub fin: bool,
    pub opcode: u8,
    /// `None` if the frame isn't masked, which a server must refuse.
    pub mask: Option<[u8; 4]>,
    pub payload_bytes: u64,
    pub header_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderParse {
    /// Not enough bytes yet.
    NeedMore,
    /// Breaks RFC 6455, and the connection has to end.
    Invalid(&'static str),
    Header(FrameHeader),
}

/// Reads a frame header. Never panics.
pub fn parse_header(bytes: &[u8]) -> HeaderParse {
    let [first, second, rest @ ..] = bytes else {
        return HeaderParse::NeedMore;
    };
    if first & 0x70 != 0 {
        return HeaderParse::Invalid("reserved bits are set");
    }
    let fin = first & 0x80 != 0;
    let opcode = first & 0x0F;
    match opcode {
        OP_CONTINUATION | OP_TEXT | OP_BINARY => {}
        OP_CLOSE | OP_PING | OP_PONG => {
            if !fin {
                return HeaderParse::Invalid("a control frame is fragmented");
            }
            if second & 0x7F > 125 {
                return HeaderParse::Invalid("a control frame is over 125 bytes");
            }
        }
        _ => return HeaderParse::Invalid("unknown opcode"),
    }
    let masked = second & 0x80 != 0;
    let (payload_bytes, length_bytes) = match second & 0x7F {
        126 => {
            let [a, b, ..] = rest else {
                return HeaderParse::NeedMore;
            };
            let length = u16::from_be_bytes([*a, *b]) as u64;
            if length < 126 {
                return HeaderParse::Invalid("length isn't in its shortest form");
            }
            (length, 2)
        }
        127 => {
            let Some(raw) = rest.first_chunk::<8>() else {
                return HeaderParse::NeedMore;
            };
            let length = u64::from_be_bytes(*raw);
            if length < 65_536 || length >> 63 != 0 {
                return HeaderParse::Invalid("length isn't in its shortest form");
            }
            (length, 8)
        }
        short => (short as u64, 0),
    };
    let after_length = &rest[length_bytes..];
    let (mask, mask_bytes) = if masked {
        let Some(mask) = after_length.first_chunk::<4>() else {
            return HeaderParse::NeedMore;
        };
        (Some(*mask), 4)
    } else {
        (None, 0)
    };
    HeaderParse::Header(FrameHeader {
        fin,
        opcode,
        mask,
        payload_bytes,
        header_bytes: 2 + length_bytes + mask_bytes,
    })
}

/// Unmasks a whole payload in place (D23).
pub fn unmask(payload: &mut [u8], mask: [u8; 4]) {
    let word = u64::from_ne_bytes([
        mask[0], mask[1], mask[2], mask[3], mask[0], mask[1], mask[2], mask[3],
    ]);
    let (words, tail) = payload.as_chunks_mut::<8>();
    for chunk in words {
        *chunk = (u64::from_ne_bytes(*chunk) ^ word).to_ne_bytes();
    }
    for (byte, mask_byte) in tail.iter_mut().zip(mask.iter().cycle()) {
        *byte ^= mask_byte;
    }
}

/// Appends an unmasked, unfragmented frame's header, as a server sends it.
pub fn encode_header(out: &mut Vec<u8>, opcode: u8, payload_bytes: u64) {
    let (head, head_bytes) = header_array(opcode, payload_bytes);
    out.extend_from_slice(&head[..head_bytes]);
}

/// `encode_header` without an allocation: the bytes, and how many are used.
pub fn header_array(opcode: u8, payload_bytes: u64) -> ([u8; 10], usize) {
    let mut head = [0u8; 10];
    head[0] = 0x80 | opcode;
    if payload_bytes < 126 {
        head[1] = payload_bytes as u8;
        (head, 2)
    } else if payload_bytes <= u16::MAX as u64 {
        head[1] = 126;
        head[2..4].copy_from_slice(&(payload_bytes as u16).to_be_bytes());
        (head, 4)
    } else {
        head[1] = 127;
        head[2..10].copy_from_slice(&payload_bytes.to_be_bytes());
        (head, 10)
    }
}

/// `Sec-WebSocket-Accept` for a `Sec-WebSocket-Key`.
pub fn accept_key(key: &str) -> String {
    let mut hash = Sha1::new();
    hash.update(key.as_bytes());
    hash.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    let digest = hash.finalize();
    const DIGITS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut text = String::with_capacity(28);
    for chunk in digest.chunks(3) {
        let group = (chunk[0] as u32) << 16
            | (*chunk.get(1).unwrap_or(&0) as u32) << 8
            | *chunk.get(2).unwrap_or(&0) as u32;
        for position in 0..4 {
            if position <= chunk.len() {
                text.push(DIGITS[(group >> (18 - position * 6)) as usize & 63] as char);
            } else {
                text.push('=');
            }
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn the_accept_key_matches_the_rfc_example() {
        assert_eq!(
            accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    /// The RFC's masked "Hello", byte for byte.
    #[test]
    fn the_rfc_example_frame_parses_and_unmasks() {
        let frame = [
            0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58,
        ];
        let HeaderParse::Header(header) = parse_header(&frame) else {
            panic!("should parse");
        };
        assert_eq!(header.opcode, OP_TEXT);
        assert!(header.fin);
        assert_eq!(header.payload_bytes, 5);
        assert_eq!(header.header_bytes, 6);
        let mut payload = frame[6..].to_vec();
        unmask(&mut payload, header.mask.unwrap());
        assert_eq!(payload, b"Hello");
    }

    #[test]
    fn invalid_headers_are_named() {
        let invalid = |bytes: &[u8]| matches!(parse_header(bytes), HeaderParse::Invalid(_));
        assert!(invalid(&[0xC2, 0x80]), "reserved bit");
        assert!(invalid(&[0x83, 0x80]), "unknown opcode");
        assert!(invalid(&[0x08, 0x80]), "fragmented close");
        assert!(invalid(&[0x89, 0xFE, 0, 200]), "long ping");
        assert!(invalid(&[0x82, 0xFE, 0, 100]), "short length as 16 bits");
        assert!(
            invalid(&[0x82, 0xFF, 0, 0, 0, 0, 0, 0, 0, 100]),
            "as 64 bits"
        );
        assert!(invalid(&[0x82, 0xFF, 0x80, 0, 0, 0, 0, 0, 0, 0]), "top bit");
    }

    proptest! {
        #[test]
        fn random_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..32)) {
            let _ = parse_header(&bytes);
        }

        /// What a client would send parses back at every cut.
        #[test]
        fn headers_round_trip_and_truncation_needs_more(
            opcode in prop::sample::select(vec![OP_BINARY, OP_TEXT, OP_CONTINUATION]),
            fin in any::<bool>(),
            payload_bytes in prop_oneof![0u64..126, 126u64..65_536, 65_536u64..(1 << 40)],
            mask in any::<[u8; 4]>(),
        ) {
            let mut bytes = vec![(if fin { 0x80 } else { 0 }) | opcode];
            if payload_bytes < 126 {
                bytes.push(0x80 | payload_bytes as u8);
            } else if payload_bytes < 65_536 {
                bytes.push(0x80 | 126);
                bytes.extend_from_slice(&(payload_bytes as u16).to_be_bytes());
            } else {
                bytes.push(0x80 | 127);
                bytes.extend_from_slice(&payload_bytes.to_be_bytes());
            }
            bytes.extend_from_slice(&mask);
            for cut in 0..bytes.len() {
                prop_assert_eq!(parse_header(&bytes[..cut]), HeaderParse::NeedMore);
            }
            prop_assert_eq!(parse_header(&bytes), HeaderParse::Header(FrameHeader {
                fin,
                opcode,
                mask: Some(mask),
                payload_bytes,
                header_bytes: bytes.len(),
            }));
        }

        /// Whatever the length and alignment, it is the byte-at-a-time result.
        #[test]
        fn unmask_matches_the_definition(
            payload in proptest::collection::vec(any::<u8>(), 0..200),
            mask in any::<[u8; 4]>(),
            skew in 0usize..8,
        ) {
            let expected: Vec<u8> = payload
                .iter()
                .enumerate()
                .map(|(i, byte)| byte ^ mask[i % 4])
                .collect();
            let mut buffer = vec![0u8; skew];
            buffer.extend_from_slice(&payload);
            unmask(&mut buffer[skew..], mask);
            prop_assert_eq!(&buffer[skew..], &expected[..]);
        }

        #[test]
        fn a_server_header_has_the_shortest_length(payload_bytes in 0u64..(1 << 40)) {
            let mut out = Vec::new();
            encode_header(&mut out, OP_BINARY, payload_bytes);
            // A server frame is unmasked, which the parser reports as no mask.
            let HeaderParse::Header(header) = parse_header(&out) else {
                panic!("should parse");
            };
            prop_assert_eq!(header.payload_bytes, payload_bytes);
            prop_assert_eq!(header.mask, None);
            prop_assert_eq!(header.header_bytes, out.len());
        }
    }
}
