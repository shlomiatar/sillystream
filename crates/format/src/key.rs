//! Stream identity (SPEC section 1): a stream is known by its key hash, never by its id.

use std::fmt;

/// The per-deployment key every key hash is made with. It never changes, since everything stored
/// refers to streams through it (section 13).
#[derive(Clone)]
pub struct IdentityKey([u8; 32]);

impl IdentityKey {
    pub const BYTES: usize = 32;

    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The key hash of a stream id, or of a cursor name (section 5.1).
    pub fn hash(&self, name: &str) -> KeyHash {
        let full = blake3::keyed_hash(&self.0, name.as_bytes());
        let mut bytes = [0u8; KeyHash::BYTES];
        bytes.copy_from_slice(&full.as_bytes()[..KeyHash::BYTES]);
        KeyHash(bytes)
    }
}

impl fmt::Debug for IdentityKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("IdentityKey(..)")
    }
}

/// BLAKE3 in keyed mode, cut to 128 bits. Printed as 32 hex digits, which is all a log or an
/// error ever shows of a stream.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct KeyHash(pub [u8; 16]);

impl KeyHash {
    pub const BYTES: usize = 16;

    /// The top 64 bits, for sharding and the segment index.
    pub fn top64(&self) -> u64 {
        u64::from_be_bytes(self.0[..8].try_into().unwrap())
    }

    pub fn from_slice(bytes: &[u8]) -> Option<Self> {
        Some(Self(bytes.try_into().ok()?))
    }

    pub fn from_hex(text: &str) -> Option<Self> {
        if text.len() != 2 * Self::BYTES {
            return None;
        }
        let mut bytes = [0u8; Self::BYTES];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(text.get(2 * index..2 * index + 2)?, 16).ok()?;
        }
        Some(Self(bytes))
    }
}

impl fmt::Display for KeyHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for KeyHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hash_depends_on_the_key_and_prints_as_hex() {
        let one = IdentityKey::new([1; 32]);
        let two = IdentityKey::new([2; 32]);
        assert_eq!(one.hash("a"), one.hash("a"));
        assert_ne!(one.hash("a"), one.hash("b"));
        assert_ne!(one.hash("a"), two.hash("a"));
        let text = one.hash("a").to_string();
        assert_eq!(text.len(), 32);
        assert_eq!(KeyHash::from_hex(&text), Some(one.hash("a")));
    }
}
