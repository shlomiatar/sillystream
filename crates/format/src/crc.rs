//! CRC32C helpers, used by the WAL and segments.

/// CRC32C of `bytes`.
pub fn crc32c(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

/// Continues `crc` over `bytes`, so `crc32c_append(crc32c(a), b)` is the CRC of `a` then `b`.
pub fn crc32c_append(crc: u32, bytes: &[u8]) -> u32 {
    crc32c::crc32c_append(crc, bytes)
}

/// CRC32C of the little-endian `seed` followed by `bytes`.
///
/// The WAL seeds entries with the file's `seq`, so a stale entry in a reused
/// file fails its CRC (BUILD_NOTES D8).
pub fn crc32c_seeded(seed: u64, bytes: &[u8]) -> u32 {
    crc32c_append(crc32c(&seed.to_le_bytes()), bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        // Standard CRC-32C check values (RFC 3720 appendix B.4 for the zeros).
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
        assert_eq!(crc32c(&[0xFFu8; 32]), 0x62A8_AB43);
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn append_equals_whole() {
        assert_eq!(
            crc32c_append(crc32c(b"1234"), b"56789"),
            crc32c(b"123456789")
        );
    }

    #[test]
    fn seed_changes_the_crc_and_is_stable() {
        assert_ne!(crc32c_seeded(1, b"entry"), crc32c_seeded(2, b"entry"));
        let mut joined = 7u64.to_le_bytes().to_vec();
        joined.extend_from_slice(b"entry");
        assert_eq!(crc32c_seeded(7, b"entry"), crc32c(&joined));
    }
}
