//! Tokens and stream patterns (SPEC section 12, D25).
//!
//! The text form of a token is the lowercase hex of `body | hmac_sha256(secret, body)`
//! (D40). The body is `expiry_ms u64 | author u64 | n u16 | n × (pattern str | perms u8)`,
//! little-endian, where `str` is a u16 length and UTF-8.
//!
//! Verification is a pure function of the secret, the token and the caller's clock
//! reading: no lookup, no I/O, and no clock of its own.
#![forbid(unsafe_code)]

use std::fmt;
use std::num::NonZeroU64;

use hmac::{Hmac, Mac};
use sha2::Sha256;

pub const PERM_READ: u8 = 0x01;
pub const PERM_PUBLISH: u8 = 0x02;
pub const PERM_STATE: u8 = 0x04;
pub const PERM_ADMIN: u8 = 0x08;
const PERM_ALL: u8 = PERM_READ | PERM_PUBLISH | PERM_STATE | PERM_ADMIN;

const SIGNATURE_BYTES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthError {
    /// Not lowercase hex, or too short to hold a signature.
    Malformed,
    /// The signature doesn't match. Nothing in the body has been looked at.
    BadSignature,
    /// A correctly signed body doesn't parse.
    MalformedBody,
    /// `author = 0` is reserved for the server (section 2.2).
    ZeroAuthor,
    /// A pattern is over 65,535 bytes, or there are over 65,535 grants.
    TooLong,
    /// A grant has permission bits section 12 doesn't define.
    UnknownPermission,
    /// `now_ms` has reached `expiry_ms`.
    Expired,
    /// The token grants nothing on this stream for this permission.
    Forbidden,
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            AuthError::Malformed => "token is malformed",
            AuthError::BadSignature => "token signature is wrong",
            AuthError::MalformedBody => "token body is malformed",
            AuthError::ZeroAuthor => "token has author 0",
            AuthError::TooLong => "token has too many or too long grants",
            AuthError::UnknownPermission => "token has an unknown permission",
            AuthError::Expired => "token has expired",
            AuthError::Forbidden => "token does not allow this on this stream",
        };
        f.write_str(text)
    }
}

impl std::error::Error for AuthError {}

/// `true` if a stream id matches a pattern: exactly, or by prefix when the pattern
/// ends in `*`. A `*` anywhere else is an ordinary character.
pub fn pattern_matches(pattern: &str, stream: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => stream.starts_with(prefix),
        None => pattern == stream,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    pub pattern: String,
    /// `PERM_*` bits.
    pub perms: u8,
}

/// The contents of a verified token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub expiry_ms: u64,
    pub author: NonZeroU64,
    pub grants: Vec<Grant>,
}

impl Token {
    /// The check every subscribe, publish and state op makes (section 12), which
    /// also rechecks expiry (D25). `perm` is one `PERM_*` bit, or several that must
    /// all be allowed by one grant.
    pub fn check(&self, stream: &str, perm: u8, now_ms: u64) -> Result<(), AuthError> {
        if now_ms >= self.expiry_ms {
            return Err(AuthError::Expired);
        }
        let allowed = self
            .grants
            .iter()
            .any(|grant| grant.perms & perm == perm && pattern_matches(&grant.pattern, stream));
        if allowed {
            Ok(())
        } else {
            Err(AuthError::Forbidden)
        }
    }

    /// Signs this token into its text form. Only the issuer does this.
    pub fn sign(&self, secret: &[u8]) -> Result<String, AuthError> {
        let mut body = Vec::new();
        body.extend_from_slice(&self.expiry_ms.to_le_bytes());
        body.extend_from_slice(&self.author.get().to_le_bytes());
        let grant_count = u16::try_from(self.grants.len()).map_err(|_| AuthError::TooLong)?;
        body.extend_from_slice(&grant_count.to_le_bytes());
        for grant in &self.grants {
            if grant.perms & !PERM_ALL != 0 {
                return Err(AuthError::UnknownPermission);
            }
            let pattern_len = u16::try_from(grant.pattern.len()).map_err(|_| AuthError::TooLong)?;
            body.extend_from_slice(&pattern_len.to_le_bytes());
            body.extend_from_slice(grant.pattern.as_bytes());
            body.push(grant.perms);
        }
        let signature = new_mac(secret).chain_update(&body).finalize().into_bytes();
        body.extend_from_slice(&signature);
        Ok(to_hex(&body))
    }
}

/// Verifies a token's text form. The signature is checked, in constant time, before
/// anything in the body is parsed.
pub fn verify(secret: &[u8], token: &str, now_ms: u64) -> Result<Token, AuthError> {
    let bytes = from_hex(token).ok_or(AuthError::Malformed)?;
    if bytes.len() < SIGNATURE_BYTES {
        return Err(AuthError::Malformed);
    }
    let (body, signature) = bytes.split_at(bytes.len() - SIGNATURE_BYTES);
    new_mac(secret)
        .chain_update(body)
        .verify_slice(signature)
        .map_err(|_| AuthError::BadSignature)?;
    let token = parse_body(body)?;
    if now_ms >= token.expiry_ms {
        return Err(AuthError::Expired);
    }
    Ok(token)
}

fn new_mac(secret: &[u8]) -> Hmac<Sha256> {
    // HMAC accepts a key of any length, so this can't fail.
    <Hmac<Sha256> as Mac>::new_from_slice(secret).expect("HMAC takes any key length")
}

fn parse_body(body: &[u8]) -> Result<Token, AuthError> {
    let mut rest = body;
    let expiry_ms = u64::from_le_bytes(take_array(&mut rest)?);
    let author = u64::from_le_bytes(take_array(&mut rest)?);
    let author = NonZeroU64::new(author).ok_or(AuthError::ZeroAuthor)?;
    let grant_count = u16::from_le_bytes(take_array(&mut rest)?);
    let mut grants = Vec::new();
    for _ in 0..grant_count {
        let pattern_len = u16::from_le_bytes(take_array(&mut rest)?) as usize;
        if pattern_len > rest.len() {
            return Err(AuthError::MalformedBody);
        }
        let (pattern, after) = rest.split_at(pattern_len);
        rest = after;
        let pattern = std::str::from_utf8(pattern).map_err(|_| AuthError::MalformedBody)?;
        let [perms] = take_array(&mut rest)?;
        if perms & !PERM_ALL != 0 {
            return Err(AuthError::UnknownPermission);
        }
        grants.push(Grant {
            pattern: pattern.to_string(),
            perms,
        });
    }
    if !rest.is_empty() {
        return Err(AuthError::MalformedBody);
    }
    Ok(Token {
        expiry_ms,
        author,
        grants,
    })
}

fn take_array<const N: usize>(rest: &mut &[u8]) -> Result<[u8; N], AuthError> {
    if rest.len() < N {
        return Err(AuthError::MalformedBody);
    }
    let (taken, after) = rest.split_at(N);
    *rest = after;
    let mut array = [0u8; N];
    array.copy_from_slice(taken);
    Ok(array)
}

fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(DIGITS[(byte >> 4) as usize] as char);
        text.push(DIGITS[(byte & 0xF) as usize] as char);
    }
    text
}

/// Lowercase only, so that a token has exactly one text form.
fn from_hex(text: &str) -> Option<Vec<u8>> {
    let digit = |byte: u8| match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    };
    let text = text.as_bytes();
    if !text.len().is_multiple_of(2) {
        return None;
    }
    text.as_chunks::<2>()
        .0
        .iter()
        .map(|pair| Some(digit(pair[0])? << 4 | digit(pair[1])?))
        .collect()
}
