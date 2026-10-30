use std::num::NonZeroU64;

use auth::{
    AuthError, Grant, PERM_ADMIN, PERM_PUBLISH, PERM_READ, PERM_STATE, Token, pattern_matches,
    verify,
};
use proptest::prelude::*;

const SECRET: &[u8] = b"test-secret";
const NOW_MS: u64 = 1_700_000_000_000;
const EXPIRY_MS: u64 = 1_800_000_000_000;

fn token() -> Token {
    Token {
        expiry_ms: EXPIRY_MS,
        author: NonZeroU64::new(7).unwrap(),
        grants: vec![
            Grant {
                pattern: "logs.*".to_string(),
                perms: PERM_READ,
            },
            Grant {
                pattern: "inbox.reviewer".to_string(),
                perms: PERM_PUBLISH | PERM_STATE,
            },
        ],
    }
}

fn vector_bytes() -> Vec<u8> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/vectors/token.hex");
    let text = std::fs::read_to_string(path).unwrap();
    text.lines()
        .filter(|line| !line.starts_with('#'))
        .flat_map(|line| line.split_whitespace())
        .map(|token| u8::from_str_radix(token, 16).unwrap())
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A body signed by an independent use of HMAC, for bodies `Token::sign` won't make.
fn sign_raw(secret: &[u8], body: &[u8]) -> String {
    use hmac::{Hmac, Mac};
    let mut mac = <Hmac<sha2::Sha256> as Mac>::new_from_slice(secret).unwrap();
    mac.update(body);
    let mut bytes = body.to_vec();
    bytes.extend_from_slice(&mac.finalize().into_bytes());
    hex(&bytes)
}

#[test]
fn valid_token_verifies() {
    let text = token().sign(SECRET).unwrap();
    assert_eq!(verify(SECRET, &text, NOW_MS), Ok(token()));
}

#[test]
fn byte_exact_vector() {
    let bytes = vector_bytes();
    assert_eq!(token().sign(SECRET).unwrap(), hex(&bytes));
    assert_eq!(verify(SECRET, &hex(&bytes), NOW_MS), Ok(token()));
}

#[test]
fn wrong_signature_is_rejected() {
    let text = token().sign(b"another-secret").unwrap();
    assert_eq!(verify(SECRET, &text, NOW_MS), Err(AuthError::BadSignature));
}

#[test]
fn expired_token_is_rejected_from_the_expiry_ms_on() {
    let text = token().sign(SECRET).unwrap();
    assert!(verify(SECRET, &text, EXPIRY_MS - 1).is_ok());
    assert_eq!(verify(SECRET, &text, EXPIRY_MS), Err(AuthError::Expired));
    assert_eq!(verify(SECRET, &text, u64::MAX), Err(AuthError::Expired));
}

/// D25: a token verified at `HELLO` still expires under a live connection.
#[test]
fn check_rechecks_expiry() {
    let token = token();
    assert_eq!(token.check("logs.a", PERM_READ, EXPIRY_MS - 1), Ok(()));
    assert_eq!(
        token.check("logs.a", PERM_READ, EXPIRY_MS),
        Err(AuthError::Expired)
    );
}

#[test]
fn author_zero_is_rejected_even_when_correctly_signed() {
    let mut body = vector_bytes();
    body.truncate(body.len() - 32);
    body[8..16].fill(0);
    assert_eq!(
        verify(SECRET, &sign_raw(SECRET, &body), NOW_MS),
        Err(AuthError::ZeroAuthor)
    );
}

#[test]
fn every_tampered_byte_is_rejected() {
    let bytes = vector_bytes();
    for at in 0..bytes.len() {
        for bit in 0..8 {
            let mut tampered = bytes.clone();
            tampered[at] ^= 1 << bit;
            assert_eq!(
                verify(SECRET, &hex(&tampered), NOW_MS),
                Err(AuthError::BadSignature),
                "byte {at} bit {bit}"
            );
        }
    }
}

#[test]
fn truncated_and_extended_tokens_are_rejected() {
    let bytes = vector_bytes();
    for end in 0..bytes.len() {
        assert!(
            verify(SECRET, &hex(&bytes[..end]), NOW_MS).is_err(),
            "{end}"
        );
    }
    let mut longer = bytes.clone();
    longer.push(0);
    assert_eq!(
        verify(SECRET, &hex(&longer), NOW_MS),
        Err(AuthError::BadSignature)
    );
}

#[test]
fn only_lowercase_hex_is_a_token() {
    let text = token().sign(SECRET).unwrap();
    assert_eq!(
        verify(SECRET, &text.to_uppercase(), NOW_MS),
        Err(AuthError::Malformed)
    );
    assert_eq!(
        verify(SECRET, &text[1..], NOW_MS),
        Err(AuthError::Malformed)
    );
    assert_eq!(verify(SECRET, "", NOW_MS), Err(AuthError::Malformed));
    assert_eq!(
        verify(SECRET, &format!("{}é", &text[2..]), NOW_MS),
        Err(AuthError::Malformed)
    );
}

#[test]
fn correctly_signed_malformed_bodies_are_rejected() {
    let body = {
        let mut bytes = vector_bytes();
        bytes.truncate(bytes.len() - 32);
        bytes
    };
    for end in 0..body.len() {
        let result = verify(SECRET, &sign_raw(SECRET, &body[..end]), NOW_MS);
        assert_eq!(result, Err(AuthError::MalformedBody), "cut at {end}");
    }

    let mut extra = body.clone();
    extra.push(0);
    assert_eq!(
        verify(SECRET, &sign_raw(SECRET, &extra), NOW_MS),
        Err(AuthError::MalformedBody)
    );

    let mut bad_utf8 = body.clone();
    bad_utf8[20] = 0xFF; // first byte of "logs.*"
    assert_eq!(
        verify(SECRET, &sign_raw(SECRET, &bad_utf8), NOW_MS),
        Err(AuthError::MalformedBody)
    );

    let mut bad_perms = body.clone();
    bad_perms[26] = 0x10; // the perms byte after "logs.*"
    assert_eq!(
        verify(SECRET, &sign_raw(SECRET, &bad_perms), NOW_MS),
        Err(AuthError::UnknownPermission)
    );
}

#[test]
fn sign_refuses_what_verify_would_refuse() {
    let mut bad_perms = token();
    bad_perms.grants[0].perms = 0x80;
    assert_eq!(bad_perms.sign(SECRET), Err(AuthError::UnknownPermission));
    let mut long_pattern = token();
    long_pattern.grants[0].pattern = "x".repeat(65_536);
    assert_eq!(long_pattern.sign(SECRET), Err(AuthError::TooLong));
}

#[test]
fn exact_patterns_match_only_themselves() {
    assert!(pattern_matches("inbox.reviewer", "inbox.reviewer"));
    assert!(!pattern_matches("inbox.reviewer", "inbox.reviewer2"));
    assert!(!pattern_matches("inbox.reviewer", "inbox"));
}

#[test]
fn trailing_star_patterns_match_by_prefix() {
    assert!(pattern_matches("logs.*", "logs.payments.3"));
    assert!(pattern_matches("logs.*", "logs.a"));
    assert!(!pattern_matches("logs.*", "logsx"));
    assert!(!pattern_matches("logs.*", "logs"));
    assert!(!pattern_matches("logs.*", "xlogs.a"));
    assert!(pattern_matches("*", "anything"));
    // Only a trailing `*` is a wildcard.
    assert!(!pattern_matches("a*b", "axb"));
    assert!(pattern_matches("a*b", "a*b"));
    assert!(!pattern_matches("*.logs", "a.logs"));
}

#[test]
fn check_needs_both_the_stream_and_the_permission() {
    let token = token();
    assert_eq!(token.check("logs.a", PERM_READ, NOW_MS), Ok(()));
    assert_eq!(token.check("inbox.reviewer", PERM_PUBLISH, NOW_MS), Ok(()));
    assert_eq!(token.check("inbox.reviewer", PERM_STATE, NOW_MS), Ok(()));
    for (stream, perm) in [
        ("logsx", PERM_READ),
        ("logs.a", PERM_PUBLISH),
        ("logs.a", PERM_ADMIN),
        ("inbox.reviewer", PERM_READ),
        ("inbox.other", PERM_PUBLISH),
    ] {
        assert_eq!(
            token.check(stream, perm, NOW_MS),
            Err(AuthError::Forbidden),
            "{stream}"
        );
    }
}

#[test]
fn a_token_with_no_grants_verifies_and_allows_nothing() {
    let token = Token {
        grants: Vec::new(),
        ..token()
    };
    let verified = verify(SECRET, &token.sign(SECRET).unwrap(), NOW_MS).unwrap();
    assert_eq!(
        verified.check("logs.a", PERM_READ, NOW_MS),
        Err(AuthError::Forbidden)
    );
}

/// Verification does no I/O. The signature makes that checkable from outside: it
/// takes the clock as an argument, and the crate's source names no I/O, clock or
/// environment API, and depends only on the two hash crates.
#[test]
fn verification_source_uses_no_io() {
    let source = include_str!("../src/lib.rs");
    for banned in [
        "std::fs",
        "std::net",
        "std::io",
        "std::env",
        "std::time",
        "std::process",
        "std::thread",
        "tokio",
        "SystemTime",
        "Instant",
    ] {
        assert!(!source.contains(banned), "auth uses {banned}");
    }
    let manifest = include_str!("../Cargo.toml");
    let dependencies = manifest
        .split("[dependencies]")
        .nth(1)
        .unwrap()
        .split('[')
        .next()
        .unwrap();
    let names: Vec<&str> = dependencies
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.split(['.', ' ']).next().unwrap())
        .collect();
    assert_eq!(names, ["hmac", "sha2"]);
}

fn arb_grant() -> impl Strategy<Value = Grant> {
    (any::<String>(), 0..=0x0Fu8).prop_map(|(pattern, perms)| Grant { pattern, perms })
}

proptest! {
    #[test]
    fn roundtrip(
        expiry_ms in (NOW_MS + 1)..=u64::MAX,
        author in 1..=u64::MAX,
        grants in proptest::collection::vec(arb_grant(), 0..8),
    ) {
        let token = Token { expiry_ms, author: NonZeroU64::new(author).unwrap(), grants };
        let text = token.sign(SECRET).unwrap();
        prop_assert_eq!(verify(SECRET, &text, NOW_MS), Ok(token));
    }

    #[test]
    fn random_text_never_panics_or_verifies(text: String) {
        prop_assert!(verify(SECRET, &text, NOW_MS).is_err());
    }

    /// Signed random bodies reach the parser, which plain random text never does.
    #[test]
    fn signed_random_bodies_never_panic(body in proptest::collection::vec(any::<u8>(), 0..300)) {
        let text = sign_raw(SECRET, &body);
        let _ = verify(SECRET, &text, NOW_MS);
    }

    #[test]
    fn a_star_pattern_matches_exactly_the_streams_with_its_prefix(
        prefix in "[a-z.]{0,8}",
        stream in "[a-z.]{0,12}",
    ) {
        prop_assert_eq!(
            pattern_matches(&format!("{prefix}*"), &stream),
            stream.starts_with(&prefix)
        );
    }
}
