//! State (SPEC section 10): CBOR, plaintext to the server, at most 64 KB. `apply_op` is
//! the only place an operation changes a document, and live application and replay both
//! go through it, so they can't disagree.

use bytes::Bytes;
use ciborium::value::{Integer, Value};
use format::{ENTRY_STATE_INCR, ENTRY_STATE_MERGE, ENTRY_STATE_SET, RecordView, SystemRecord};

pub const MAX_STATE_BYTES: usize = 64 * 1024;
/// What `incr` will walk or build. The parser refuses to nest deeper than this, so a longer
/// path could never be read back, and building it would risk the core's stack (D76).
const MAX_INCR_PATH_KEYS: usize = MAX_DEPTH;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateOp {
    Set,
    Merge,
    Incr,
}

impl StateOp {
    /// The `op` of `STATE_OP` and `PUBLISH`, which is also the system record's entry type.
    pub fn from_code(code: u64) -> Option<StateOp> {
        match code {
            ENTRY_STATE_SET => Some(StateOp::Set),
            ENTRY_STATE_MERGE => Some(StateOp::Merge),
            ENTRY_STATE_INCR => Some(StateOp::Incr),
            _ => None,
        }
    }

    pub fn entry_type(self) -> u64 {
        match self {
            StateOp::Set => ENTRY_STATE_SET,
            StateOp::Merge => ENTRY_STATE_MERGE,
            StateOp::Incr => ENTRY_STATE_INCR,
        }
    }
}

/// All of these are error 8 to a client (D6a, D6c).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateError {
    DocTooLarge,
    InvalidCbor,
    ResultTooLarge,
    /// The result is nested too deeply to be parsed again (D76).
    ResultTooDeep,
    /// `incr` found something other than a number, or a non-map on its path.
    NotNumeric,
    /// `incr`'s doc isn't `{"path": [keys], "n": int}`.
    BadIncrDoc,
    /// `incr`'s result doesn't fit a CBOR integer.
    IncrOverflow,
    /// Not a state entry: bad type, or not a system record.
    NotStateRecord,
}

impl std::fmt::Display for StateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            StateError::DocTooLarge => "state doc is over 64 KB",
            StateError::InvalidCbor => "state doc is not valid CBOR",
            StateError::ResultTooLarge => "state would be over 64 KB",
            StateError::ResultTooDeep => "state would be nested too deeply",
            StateError::NotNumeric => "incr path does not lead to a number",
            StateError::BadIncrDoc => "incr doc is not {path, n}",
            StateError::IncrOverflow => "incr result does not fit a CBOR integer",
            StateError::NotStateRecord => "record is not a state entry",
        })
    }
}

impl std::error::Error for StateError {}

/// How deep a doc can nest, which is what keeps `merge` and `encode` off the stack's edge.
const MAX_DEPTH: usize = 256;
/// `Value` has no simple values, so one is held as this tag on its number (D81). The CBOR
/// tag registry marks this tag as never valid, so no real doc can mean it.
const SIMPLE_TAG: u64 = u64::MAX;
const BIGNUM_POSITIVE: u64 = 2;
const BIGNUM_NEGATIVE: u64 = 3;
const BREAK: u8 = 0xFF;

fn simple_value(number: u8) -> Value {
    Value::Tag(SIMPLE_TAG, Box::new(Value::Integer(number.into())))
}

/// RFC 8949 decoding into a `Value`. It allocates only for bytes it has seen.
struct Reader<'a> {
    rest: &'a [u8],
}

impl Reader<'_> {
    fn take(&mut self, count: u64) -> Result<&[u8], StateError> {
        let count = usize::try_from(count).map_err(|_| StateError::InvalidCbor)?;
        if count > self.rest.len() {
            return Err(StateError::InvalidCbor);
        }
        let (taken, rest) = self.rest.split_at(count);
        self.rest = rest;
        Ok(taken)
    }

    fn at_break(&mut self) -> Result<bool, StateError> {
        let next = self.rest.first().ok_or(StateError::InvalidCbor)?;
        if *next != BREAK {
            return Ok(false);
        }
        self.rest = &self.rest[1..];
        Ok(true)
    }

    /// The bytes of an argument of 1, 2, 4 or 8 bytes.
    fn argument(&mut self, info: u8) -> Result<Option<u64>, StateError> {
        let bytes = match info {
            0..=23 => return Ok(Some(u64::from(info))),
            24 => 1,
            25 => 2,
            26 => 4,
            27 => 8,
            31 => return Ok(None),
            _ => return Err(StateError::InvalidCbor),
        };
        let taken = self.take(bytes)?;
        Ok(Some(taken.iter().fold(0, |n, b| (n << 8) | u64::from(*b))))
    }

    /// A definite string's bytes, or all an indefinite one's chunks.
    fn string(&mut self, major: u8, length: Option<u64>) -> Result<Vec<u8>, StateError> {
        let chunk = |bytes: &[u8]| {
            if major == 3 && std::str::from_utf8(bytes).is_err() {
                return Err(StateError::InvalidCbor);
            }
            Ok(bytes.to_vec())
        };
        if let Some(length) = length {
            return chunk(self.take(length)?);
        }
        let mut all = Vec::new();
        while !self.at_break()? {
            let head = self.take(1)?[0];
            let length = self.argument(head & 0x1F)?.ok_or(StateError::InvalidCbor)?;
            if head >> 5 != major {
                return Err(StateError::InvalidCbor);
            }
            all.extend(chunk(self.take(length)?)?);
        }
        Ok(all)
    }

    fn item(&mut self, depth: usize) -> Result<Value, StateError> {
        let head = self.take(1)?[0];
        let (major, info) = (head >> 5, head & 0x1F);
        if major == 7 {
            return self.simple_or_float(info);
        }
        let argument = self.argument(info)?;
        let number = argument.ok_or(StateError::InvalidCbor);
        match major {
            0 => Ok(Value::Integer(number?.into())),
            1 => int_value(-1 - i128::from(number?)).map_err(|_| StateError::InvalidCbor),
            2 => Ok(Value::Bytes(self.string(major, argument)?)),
            3 => {
                let bytes = self.string(major, argument)?;
                String::from_utf8(bytes)
                    .map(Value::Text)
                    .map_err(|_| StateError::InvalidCbor)
            }
            _ if depth == 0 => Err(StateError::InvalidCbor),
            4 => {
                let mut items = Vec::new();
                match argument {
                    Some(count) => {
                        for _ in 0..count {
                            items.push(self.item(depth - 1)?);
                        }
                    }
                    None => {
                        while !self.at_break()? {
                            items.push(self.item(depth - 1)?);
                        }
                    }
                }
                Ok(Value::Array(items))
            }
            5 => {
                let mut pairs = Vec::new();
                match argument {
                    Some(count) => {
                        for _ in 0..count {
                            let key = self.item(depth - 1)?;
                            pairs.push((key, self.item(depth - 1)?));
                        }
                    }
                    None => {
                        while !self.at_break()? {
                            let key = self.item(depth - 1)?;
                            pairs.push((key, self.item(depth - 1)?));
                        }
                    }
                }
                Ok(Value::Map(pairs))
            }
            _ => {
                let tag = number?;
                if tag == SIMPLE_TAG {
                    return Err(StateError::InvalidCbor);
                }
                Ok(Value::Tag(tag, Box::new(self.item(depth - 1)?)))
            }
        }
    }

    fn simple_or_float(&mut self, info: u8) -> Result<Value, StateError> {
        match info {
            20 => Ok(Value::Bool(false)),
            21 => Ok(Value::Bool(true)),
            22 => Ok(Value::Null),
            0..=23 => Ok(simple_value(info)),
            24 => match self.take(1)?[0] {
                // Below 32, this form isn't well formed (RFC 8949 section 3.3).
                0..=31 => Err(StateError::InvalidCbor),
                number => Ok(simple_value(number)),
            },
            25 => {
                let bits = self.argument(info)?.ok_or(StateError::InvalidCbor)?;
                Ok(Value::Float(half_to_f64(bits as u16)))
            }
            26 => {
                let bits = self.argument(info)?.ok_or(StateError::InvalidCbor)?;
                Ok(Value::Float(f64::from(f32::from_bits(bits as u32))))
            }
            27 => {
                let bits = self.argument(info)?.ok_or(StateError::InvalidCbor)?;
                Ok(Value::Float(f64::from_bits(bits)))
            }
            _ => Err(StateError::InvalidCbor),
        }
    }
}

fn half_to_f64(bits: u16) -> f64 {
    let magnitude = match (bits >> 10) & 0x1F {
        0 => f64::from(bits & 0x3FF) * 2f64.powi(-24),
        0x1F if bits & 0x3FF == 0 => f64::INFINITY,
        0x1F => f64::NAN,
        exponent => f64::from(0x400 | (bits & 0x3FF)) * 2f64.powi(i32::from(exponent) - 25),
    };
    if bits & 0x8000 != 0 {
        -magnitude
    } else {
        magnitude
    }
}

fn parse(bytes: &[u8]) -> Result<Value, StateError> {
    let mut reader = Reader { rest: bytes };
    let value = reader.item(MAX_DEPTH)?;
    if !reader.rest.is_empty() {
        return Err(StateError::InvalidCbor);
    }
    Ok(value)
}

fn write_head(out: &mut Vec<u8>, major: u8, number: u64) {
    let major = major << 5;
    match number {
        0..=23 => out.push(major | number as u8),
        24..=0xFF => out.extend_from_slice(&[major | 24, number as u8]),
        0x100..=0xFFFF => {
            out.push(major | 25);
            out.extend_from_slice(&(number as u16).to_be_bytes());
        }
        0x1_0000..=0xFFFF_FFFF => {
            out.push(major | 26);
            out.extend_from_slice(&(number as u32).to_be_bytes());
        }
        _ => {
            out.push(major | 27);
            out.extend_from_slice(&number.to_be_bytes());
        }
    }
}

fn write_value(out: &mut Vec<u8>, value: &Value) -> Result<(), StateError> {
    match value {
        Value::Integer(int) => {
            let int = i128::from(*int);
            if int >= 0 {
                write_head(out, 0, int as u64);
            } else {
                write_head(out, 1, (-1 - int) as u64);
            }
        }
        Value::Bytes(bytes) => {
            write_head(out, 2, bytes.len() as u64);
            out.extend_from_slice(bytes);
        }
        Value::Text(text) => {
            write_head(out, 3, text.len() as u64);
            out.extend_from_slice(text.as_bytes());
        }
        Value::Array(items) => {
            write_head(out, 4, items.len() as u64);
            for item in items {
                write_value(out, item)?;
            }
        }
        Value::Map(pairs) => {
            write_head(out, 5, pairs.len() as u64);
            for (key, item) in pairs {
                write_value(out, key)?;
                write_value(out, item)?;
            }
        }
        Value::Tag(SIMPLE_TAG, inner) => {
            let number = inner
                .as_integer()
                .and_then(|int| u8::try_from(int).ok())
                .ok_or(StateError::InvalidCbor)?;
            write_head(out, 7, u64::from(number));
        }
        Value::Tag(tag, inner) => {
            write_head(out, 6, *tag);
            write_value(out, inner)?;
        }
        Value::Bool(flag) => out.push(if *flag { 0xF5 } else { 0xF4 }),
        Value::Null => out.push(0xF6),
        Value::Float(float) => {
            let single = *float as f32;
            if f64::from(single) == *float {
                out.push(0xFA);
                out.extend_from_slice(&single.to_bits().to_be_bytes());
            } else {
                out.push(0xFB);
                out.extend_from_slice(&float.to_bits().to_be_bytes());
            }
        }
        _ => return Err(StateError::InvalidCbor),
    }
    Ok(())
}

fn encode(value: &Value) -> Result<Vec<u8>, StateError> {
    let mut out = Vec::new();
    write_value(&mut out, value)?;
    if out.len() > MAX_STATE_BYTES {
        return Err(StateError::ResultTooLarge);
    }
    Ok(out)
}

/// What section 3's validation asks of a doc: at most 64 KB and one valid CBOR item.
pub fn check_doc(doc: &[u8]) -> Result<(), StateError> {
    if doc.len() > MAX_STATE_BYTES {
        return Err(StateError::DocTooLarge);
    }
    parse(doc).map(|_| ())
}

fn merge(target: Option<Value>, patch: Value) -> Value {
    let Value::Map(patch) = patch else {
        return patch;
    };
    let mut map = match target {
        Some(Value::Map(map)) => map,
        _ => Vec::new(),
    };
    for (key, value) in patch {
        let at = map.iter().position(|(existing, _)| *existing == key);
        if value == Value::Null {
            map.retain(|(existing, _)| *existing != key);
            continue;
        }
        match at {
            Some(at) => {
                let old = std::mem::replace(&mut map[at].1, Value::Null);
                map[at].1 = merge(Some(old), value);
                // A map with a repeated key has no single meaning, so the patched one wins.
                let mut seen = 0;
                map.retain(|(existing, _)| {
                    if *existing != key {
                        return true;
                    }
                    seen += 1;
                    seen == 1
                });
            }
            None => {
                let merged = merge(None, value);
                map.push((key, merged));
            }
        }
    }
    Value::Map(map)
}

fn parse_incr(doc: Value) -> Result<(Vec<Value>, i128), StateError> {
    let Value::Map(fields) = doc else {
        return Err(StateError::BadIncrDoc);
    };
    if fields.len() != 2 {
        return Err(StateError::BadIncrDoc);
    }
    let mut path = None;
    let mut n = None;
    for (key, value) in fields {
        match (key.as_text(), value) {
            (Some("path"), Value::Array(keys)) => path = Some(keys),
            (Some("n"), value) => match whole_number(&value) {
                Ok(Some(number)) => n = Some(number),
                _ => return Err(StateError::BadIncrDoc),
            },
            _ => return Err(StateError::BadIncrDoc),
        }
    }
    match (path, n) {
        (Some(path), Some(n)) => Ok((path, n)),
        _ => Err(StateError::BadIncrDoc),
    }
}

/// An integer or a bignum as a number. `None` if it's neither, and an overflow if it is one
/// too large to do sums on.
fn whole_number(value: &Value) -> Result<Option<i128>, StateError> {
    let (tag, bytes) = match value {
        Value::Integer(int) => return Ok(Some(i128::from(*int))),
        Value::Tag(tag @ (BIGNUM_POSITIVE | BIGNUM_NEGATIVE), inner) => match &**inner {
            Value::Bytes(bytes) => (*tag, bytes),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    let start = bytes.iter().take_while(|byte| **byte == 0).count();
    let digits = &bytes[start..];
    if digits.len() > 16 {
        return Err(StateError::IncrOverflow);
    }
    let magnitude = digits.iter().fold(0u128, |n, b| (n << 8) | u128::from(*b));
    let magnitude = i128::try_from(magnitude).map_err(|_| StateError::IncrOverflow)?;
    Ok(Some(if tag == BIGNUM_POSITIVE {
        magnitude
    } else {
        -1 - magnitude
    }))
}

fn add(slot: &mut Value, n: i128) -> Result<(), StateError> {
    if let Value::Float(float) = slot {
        *float += n as f64;
        return Ok(());
    }
    let current = whole_number(slot)?.ok_or(StateError::NotNumeric)?;
    let sum = current.checked_add(n).ok_or(StateError::IncrOverflow)?;
    *slot = int_value(sum)?;
    Ok(())
}

fn int_value(n: i128) -> Result<Value, StateError> {
    Integer::try_from(n)
        .map(Value::Integer)
        .map_err(|_| StateError::IncrOverflow)
}

/// True if it made a new key, which is the only way an `incr` can nest a doc deeper.
fn incr_at(slot: &mut Value, path: &[Value], n: i128) -> Result<bool, StateError> {
    let Some((key, rest)) = path.split_first() else {
        add(slot, n)?;
        return Ok(false);
    };
    let Value::Map(map) = slot else {
        return Err(StateError::NotNumeric);
    };
    if let Some(at) = map.iter().position(|(existing, _)| existing == key) {
        return incr_at(&mut map[at].1, rest, n);
    }
    // Missing: created as `n`, with maps for the rest of the path.
    let mut created = int_value(n)?;
    for key in rest.iter().rev() {
        created = Value::Map(vec![(key.clone(), created)]);
    }
    map.push((key.clone(), created));
    Ok(true)
}

/// The result, and whether it needs checking for depth.
fn incr(current: Option<Value>, path: &[Value], n: i128) -> Result<(Value, bool), StateError> {
    let mut root = match current {
        Some(value) => value,
        None if path.is_empty() => return Ok((int_value(n)?, false)),
        None => Value::Map(Vec::new()),
    };
    let added_key = incr_at(&mut root, path, n)?;
    Ok((root, added_key))
}

/// The document after `op`, or an error with nothing changed. `current` is empty if the
/// stream has no state yet. A `set` keeps the doc's bytes as they are.
pub fn apply_op(current: &[u8], op: StateOp, doc: &[u8]) -> Result<Bytes, StateError> {
    if doc.len() > MAX_STATE_BYTES {
        return Err(StateError::DocTooLarge);
    }
    let patch = parse(doc)?;
    if op == StateOp::Set {
        return Ok(Bytes::copy_from_slice(doc));
    }
    let current = if current.is_empty() {
        None
    } else {
        Some(parse(current)?)
    };
    // A merge can't go deeper than the deeper of two docs that both parsed: at every level,
    // what it keeps comes from that level of one of them.
    let (result, may_be_too_deep) = match op {
        StateOp::Merge => (merge(current, patch), false),
        _ => {
            let (path, n) = parse_incr(patch)?;
            if path.len() > MAX_INCR_PATH_KEYS {
                return Err(StateError::ResultTooDeep);
            }
            incr(current, &path, n)?
        }
    };
    let bytes = encode(&result)?;
    // A doc that can't be parsed would refuse every later merge and incr, and replay.
    if may_be_too_deep {
        parse(&bytes).map_err(|_| StateError::ResultTooDeep)?;
    }
    Ok(Bytes::from(bytes))
}

/// A stream's state and its version, which is the id of the system record that made it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamState {
    doc: Bytes,
    ver: u64,
}

impl StreamState {
    pub fn new() -> StreamState {
        StreamState::default()
    }

    /// Empty if no change has been made.
    pub fn doc(&self) -> &Bytes {
        &self.doc
    }

    pub fn ver(&self) -> u64 {
        self.ver
    }

    /// Installs a result from `apply_op` with the system record's id.
    pub fn install(&mut self, doc: Bytes, ver: u64) {
        self.doc = doc;
        self.ver = ver;
    }

    /// Replays one system record. Only `author = 0` records get here (section 2.2).
    pub fn replay(&mut self, view: &RecordView<'_>) -> Result<(), StateError> {
        let record = SystemRecord::parse(view).map_err(|_| StateError::NotStateRecord)?;
        let op = StateOp::from_code(record.entry_type).ok_or(StateError::NotStateRecord)?;
        let doc = apply_op(&self.doc, op, record.doc)?;
        self.install(doc, view.header.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use format::{RecordHeader, Records, encode_system_record};
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn whatever_an_incr_or_merge_returns_can_be_applied_to_again() {
        let nested = |depth: usize| {
            let mut value = int(1);
            for _ in 0..depth {
                value = Value::Map(vec![(int(1), value)]);
            }
            value
        };
        let incr_keys =
            |keys: usize| incr_doc_values((0..keys).map(|_| int(2)).collect::<Vec<_>>(), 1);
        let next = cbor(&map(&[("a", int(1))]));
        let mut accepted_count = 0;
        // State already near the limit, with an op that would take it past.
        for depth in 200..270 {
            let current = cbor(&nested(depth));
            if parse(&current).is_err() {
                continue;
            }
            for keys in [1, 2, 60, 300] {
                match apply_op(&current, StateOp::Incr, &cbor(&incr_keys(keys))) {
                    Ok(result) => {
                        accepted_count += 1;
                        parse(&result).unwrap();
                        apply_op(&result, StateOp::Merge, &next).unwrap();
                    }
                    Err(error) => assert!(
                        matches!(error, StateError::ResultTooDeep | StateError::NotNumeric),
                        "{error:?}"
                    ),
                }
            }
            // A merge patch as deep as what it lands on, under a key of its own.
            let patch = Value::Map(vec![(int(9), nested(depth))]);
            if let Ok(result) = apply_op(&current, StateOp::Merge, &cbor(&patch)) {
                accepted_count += 1;
                parse(&result).unwrap();
            }
            // And along the same keys, with a deep key at the far end.
            let mut along = Value::Map(vec![(nested(depth / 2), int(1))]);
            for _ in 0..depth / 2 {
                along = Value::Map(vec![(int(1), along)]);
            }
            let patch = cbor(&along);
            if parse(&patch).is_ok() {
                let result = apply_op(&current, StateOp::Merge, &patch).unwrap();
                accepted_count += 1;
                parse(&result).unwrap();
            }
        }
        assert!(accepted_count > 0);
    }

    #[test]
    fn a_path_of_any_length_is_refused_before_anything_is_built() {
        let doc = incr_doc_values((0..60_000).map(|_| int(1)).collect::<Vec<_>>(), 1);
        assert_eq!(
            apply_op(&[], StateOp::Incr, &cbor(&doc)),
            Err(StateError::ResultTooDeep)
        );
    }

    fn incr_doc_values(path: Vec<Value>, n: i64) -> Value {
        map(&[("path", Value::Array(path)), ("n", int(n))])
    }

    fn cbor(value: &Value) -> Vec<u8> {
        let mut out = Vec::new();
        ciborium::ser::into_writer(value, &mut out).unwrap();
        out
    }

    fn text(value: &str) -> Value {
        Value::Text(value.to_owned())
    }

    fn int(value: i64) -> Value {
        Value::Integer(value.into())
    }

    fn map(pairs: &[(&str, Value)]) -> Value {
        Value::Map(pairs.iter().map(|(k, v)| (text(k), v.clone())).collect())
    }

    fn apply(current: &Value, op: StateOp, doc: &Value) -> Result<Value, StateError> {
        apply_op(&cbor(current), op, &cbor(doc)).map(|bytes| parse(&bytes).unwrap())
    }

    fn incr_doc(path: &[&str], n: i64) -> Value {
        map(&[
            ("path", Value::Array(path.iter().map(|k| text(k)).collect())),
            ("n", int(n)),
        ])
    }

    #[test]
    fn set_replaces_and_keeps_the_bytes() {
        // Not the encoder's shortest form: a 1-byte int written in 2 bytes.
        let doc = [0x18, 0x01];
        let result = apply_op(&cbor(&int(9)), StateOp::Set, &doc).unwrap();
        assert_eq!(&result[..], &doc);
    }

    #[test]
    fn merge_follows_rfc_7396_including_nested_deletes() {
        let current = map(&[
            ("a", int(1)),
            ("b", map(&[("x", int(1)), ("y", int(2))])),
            ("c", int(3)),
        ]);
        let patch = map(&[
            ("a", int(9)),
            ("b", map(&[("x", Value::Null), ("z", int(3))])),
            ("c", Value::Null),
            ("d", map(&[("gone", Value::Null), ("kept", int(4))])),
        ]);
        let expected = map(&[
            ("a", int(9)),
            ("b", map(&[("y", int(2)), ("z", int(3))])),
            ("d", map(&[("kept", int(4))])),
        ]);
        assert_eq!(apply(&current, StateOp::Merge, &patch).unwrap(), expected);
    }

    #[test]
    fn merge_with_a_non_map_replaces_and_onto_a_non_map_starts_empty() {
        assert_eq!(
            apply(&map(&[("a", int(1))]), StateOp::Merge, &int(5)).unwrap(),
            int(5)
        );
        assert_eq!(
            apply(&int(5), StateOp::Merge, &map(&[("a", int(1))])).unwrap(),
            map(&[("a", int(1))])
        );
        let empty = apply_op(&[], StateOp::Merge, &cbor(&map(&[("a", Value::Null)]))).unwrap();
        assert_eq!(parse(&empty).unwrap(), map(&[]));
    }

    #[test]
    fn merge_collapses_a_repeated_key() {
        let current = Value::Map(vec![(text("a"), int(1)), (text("a"), int(2))]);
        assert_eq!(
            apply(&current, StateOp::Merge, &map(&[("a", int(3))])).unwrap(),
            map(&[("a", int(3))])
        );
        assert_eq!(
            apply(&current, StateOp::Merge, &map(&[("a", Value::Null)])).unwrap(),
            map(&[])
        );
    }

    #[test]
    fn incr_adds_and_creates() {
        let current = map(&[("n", int(5)), ("f", Value::Float(1.5))]);
        assert_eq!(
            apply(&current, StateOp::Incr, &incr_doc(&["n"], -2)).unwrap(),
            map(&[("n", int(3)), ("f", Value::Float(1.5))])
        );
        assert_eq!(
            apply(&current, StateOp::Incr, &incr_doc(&["f"], 1)).unwrap(),
            map(&[("n", int(5)), ("f", Value::Float(2.5))])
        );
        assert_eq!(
            apply(&current, StateOp::Incr, &incr_doc(&["a", "b", "c"], 7)).unwrap(),
            map(&[
                ("n", int(5)),
                ("f", Value::Float(1.5)),
                ("a", map(&[("b", map(&[("c", int(7))]))])),
            ])
        );
        let created = apply_op(&[], StateOp::Incr, &cbor(&incr_doc(&["x"], 1))).unwrap();
        assert_eq!(parse(&created).unwrap(), map(&[("x", int(1))]));
        let root = apply_op(&[], StateOp::Incr, &cbor(&incr_doc(&[], 4))).unwrap();
        assert_eq!(parse(&root).unwrap(), int(4));
    }

    #[test]
    fn incr_fails_on_non_numbers_and_bad_docs() {
        let current = map(&[("s", text("no")), ("nil", Value::Null), ("m", map(&[]))]);
        for path in [&["s"][..], &["nil"], &["m"], &["s", "deeper"]] {
            assert_eq!(
                apply(&current, StateOp::Incr, &incr_doc(path, 1)),
                Err(StateError::NotNumeric),
                "{path:?}"
            );
        }
        for doc in [
            int(1),
            map(&[("n", int(1))]),
            map(&[("path", Value::Array(vec![]))]),
            map(&[("path", text("x")), ("n", int(1))]),
            map(&[("path", Value::Array(vec![])), ("n", text("1"))]),
            map(&[("path", Value::Array(vec![])), ("n", int(1)), ("x", int(1))]),
        ] {
            assert_eq!(
                apply(&map(&[]), StateOp::Incr, &doc),
                Err(StateError::BadIncrDoc),
                "{doc:?}"
            );
        }
    }

    #[test]
    fn incr_overflow_is_an_error_at_each_end_of_cbor_integers() {
        let top = Value::Integer(Integer::try_from(u64::MAX as i128).unwrap());
        let bottom = Value::Integer(Integer::try_from(-(1i128 << 64)).unwrap());
        assert_eq!(
            apply(&top, StateOp::Incr, &incr_doc(&[], 1)),
            Err(StateError::IncrOverflow)
        );
        assert_eq!(
            apply(&bottom, StateOp::Incr, &incr_doc(&[], -1)),
            Err(StateError::IncrOverflow)
        );
        assert!(apply(&top, StateOp::Incr, &incr_doc(&[], -1)).is_ok());
    }

    /// A CBOR text string of exactly `total_bytes`.
    fn text_doc(total_bytes: usize) -> Vec<u8> {
        // Header of 3 bytes: the type, then a u16 length.
        let doc = cbor(&Value::Text("x".repeat(total_bytes - 3)));
        assert_eq!(doc.len(), total_bytes);
        doc
    }

    #[test]
    fn the_64_kb_limit_is_exact_on_docs_and_results() {
        assert!(check_doc(&text_doc(MAX_STATE_BYTES)).is_ok());
        assert_eq!(
            check_doc(&text_doc(MAX_STATE_BYTES + 1)),
            Err(StateError::DocTooLarge)
        );
        assert!(apply_op(&[], StateOp::Set, &text_doc(MAX_STATE_BYTES)).is_ok());
        assert_eq!(
            apply_op(&[], StateOp::Set, &text_doc(MAX_STATE_BYTES + 1)),
            Err(StateError::DocTooLarge)
        );
        // Two legal halves make an illegal whole.
        let half = "x".repeat(MAX_STATE_BYTES / 2);
        let current = cbor(&map(&[("a", Value::Text(half.clone()))]));
        let patch = cbor(&map(&[("b", Value::Text(half))]));
        assert_eq!(
            apply_op(&current, StateOp::Merge, &patch),
            Err(StateError::ResultTooLarge)
        );
    }

    #[test]
    fn hostile_docs_are_errors_and_do_not_allocate_what_they_claim() {
        for doc in [
            &[][..],
            &[0xFF],
            &[0x01, 0x01],                                           // trailing bytes
            &[0x61, 0xFF],                                           // invalid UTF-8
            &[0x9B, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], // huge array
            &[0xBB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], // huge map
            &[0x5B, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], // huge bytes
            &[0x7B, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], // huge text
        ] {
            assert_eq!(check_doc(doc), Err(StateError::InvalidCbor), "{doc:?}");
        }
        // Deep nesting inside the size limit is refused, not a stack overflow.
        assert_eq!(check_doc(&[0x81; 60_000]), Err(StateError::InvalidCbor));
    }

    fn arb_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<i32>().prop_map(|n| int(i64::from(n))),
            "[a-c]{0,3}".prop_map(Value::Text),
        ];
        leaf.prop_recursive(3, 16, 4, |inner| {
            proptest::collection::vec(("[a-c]", inner), 0..4).prop_map(|pairs| {
                Value::Map(
                    pairs
                        .into_iter()
                        .map(|(k, v)| (Value::Text(k), v))
                        .collect(),
                )
            })
        })
    }

    /// As `arb_value`, with no repeated map keys at any level. RFC 7396 doesn't say what
    /// a repeated key in a patch means, so its properties only hold without them.
    fn arb_unique_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<i32>().prop_map(|n| int(i64::from(n))),
            "[a-c]{0,3}".prop_map(Value::Text),
        ];
        leaf.prop_recursive(3, 16, 4, |inner| {
            proptest::collection::btree_map("[a-c]", inner, 0..4).prop_map(|pairs| {
                Value::Map(
                    pairs
                        .into_iter()
                        .map(|(k, v)| (Value::Text(k), v))
                        .collect(),
                )
            })
        })
    }

    fn arb_op() -> impl Strategy<Value = (StateOp, Vec<u8>)> {
        prop_oneof![
            arb_value().prop_map(|value| (StateOp::Set, cbor(&value))),
            arb_value().prop_map(|value| (StateOp::Merge, cbor(&value))),
            (proptest::collection::vec("[a-c]", 0..3), -5i64..5).prop_map(|(path, n)| {
                let path: Vec<&str> = path.iter().map(String::as_str).collect();
                (StateOp::Incr, cbor(&incr_doc(&path, n)))
            }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]

        /// Replaying the system records gives the state live application gave: the same
        /// bytes and version, with failed ops leaving no record and no change.
        #[test]
        fn replay_equals_live_application(ops in proptest::collection::vec(arb_op(), 0..40)) {
            let mut live = StreamState::new();
            let mut log = Vec::new();
            let mut id = 1_000u64;
            for (op, doc) in ops {
                id += 7;
                if let Ok(result) = apply_op(live.doc(), op, &doc) {
                    live.install(result, id);
                    encode_system_record(&mut log, id, op.entry_type(), 42, &doc).unwrap();
                }
            }
            let mut replayed = StreamState::new();
            for view in Records::new(&log) {
                replayed.replay(&view.unwrap()).unwrap();
            }
            prop_assert_eq!(replayed, live);
        }

        /// Merging twice changes nothing more (RFC 7396 patches are idempotent), which
        /// also shows null-stripping reaches every level.
        #[test]
        fn merge_is_idempotent(current in arb_unique_value(), patch in arb_unique_value()) {
            let once = apply_op(&cbor(&current), StateOp::Merge, &cbor(&patch)).unwrap();
            let twice = apply_op(&once, StateOp::Merge, &cbor(&patch)).unwrap();
            prop_assert_eq!(once, twice);
        }
    }

    #[test]
    fn every_well_formed_simple_value_is_valid_and_survives_a_merge() {
        for doc in [&[0xE0][..], &[0xF0], &[0xF7], &[0xF8, 0x20], &[0xF8, 0xFF]] {
            assert_eq!(check_doc(doc), Ok(()), "{doc:?}");
            let mut current = vec![0xA1, 0x61, b'a'];
            current.extend_from_slice(doc);
            let merged = apply_op(&current, StateOp::Merge, &[0xA1, 0x61, b'b', 0x01]).unwrap();
            let mut expected = current[1..].to_vec();
            expected.insert(0, 0xA2);
            expected.extend_from_slice(&[0x61, b'b', 0x01]);
            assert_eq!(&merged[..], &expected[..], "{doc:?}");
        }
        // These two forms are not well formed (RFC 8949 section 3.3).
        for doc in [&[0xF8, 0x00][..], &[0xF8, 0x1F]] {
            assert_eq!(check_doc(doc), Err(StateError::InvalidCbor), "{doc:?}");
        }
    }

    #[test]
    fn only_null_deletes_in_a_merge() {
        assert_eq!(
            &apply_op(&[0xA0], StateOp::Merge, &[0xA1, 0x61, b'a', 0xF7]).unwrap()[..],
            &[0xA1, 0x61, b'a', 0xF7]
        );
        assert_eq!(
            &apply_op(
                &[0xA1, 0x61, b'a', 0x01],
                StateOp::Merge,
                &[0xA1, 0x61, b'a', 0xF6]
            )
            .unwrap()[..],
            &[0xA0]
        );
    }

    /// `{"path": [], "n": n}` with `n` already encoded.
    fn incr_root(n: &[u8]) -> Vec<u8> {
        let mut doc = vec![0xA2, 0x64, b'p', b'a', b't', b'h', 0x80, 0x61, b'n'];
        doc.extend_from_slice(n);
        doc
    }

    #[test]
    fn incr_treats_a_bignum_as_the_number_it_is() {
        // 2^64 - 1 = u64::MAX, which fits.
        let two_to_64 = [0xC2, 0x49, 1, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(
            &apply_op(&two_to_64, StateOp::Incr, &incr_root(&[0x20])).unwrap()[..],
            &[0x1B, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
        );
        // Still too large, or far too large, is an overflow and not "not a number".
        assert_eq!(
            apply_op(&two_to_64, StateOp::Incr, &incr_root(&[0x01])),
            Err(StateError::IncrOverflow)
        );
        let mut huge = vec![0xC2, 0x58, 20];
        huge.extend_from_slice(&[0xFF; 20]);
        assert_eq!(
            apply_op(&huge, StateOp::Incr, &incr_root(&[0x20])),
            Err(StateError::IncrOverflow)
        );
        // Tag 3 is -1 - n: -1 - 2^64, plus 1.
        let below = [0xC3, 0x49, 1, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(
            &apply_op(&below, StateOp::Incr, &incr_root(&[0x01])).unwrap()[..],
            &[0x3B, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
        );
        // Leading zeros and an empty string are legal bignums. `n` can be one too.
        assert_eq!(
            &apply_op(
                &[0xC2, 0x42, 0, 5],
                StateOp::Incr,
                &incr_root(&[0xC2, 0x40])
            )
            .unwrap()[..],
            &[0x05]
        );
        // A bignum tag on something else is well formed, and isn't a number.
        assert_eq!(check_doc(&[0xC2, 0x01]), Ok(()));
        assert_eq!(
            apply_op(&[0xC2, 0x01], StateOp::Incr, &incr_root(&[0x01])),
            Err(StateError::NotNumeric)
        );
    }

    #[test]
    fn the_parser_reads_every_form_of_cbor_and_refuses_what_is_not() {
        let floats: [(&[u8], f64); 4] = [
            (&[0xF9, 0x3E, 0x00], 1.5),
            (&[0xF9, 0x00, 0x01], 2f64.powi(-24)),
            (&[0xFA, 0x3F, 0xC0, 0x00, 0x00], 1.5),
            (&[0xFB, 0x3F, 0xF8, 0, 0, 0, 0, 0, 0], 1.5),
        ];
        for (doc, expected) in floats {
            assert_eq!(parse(doc).unwrap(), Value::Float(expected), "{doc:?}");
        }
        assert_eq!(
            parse(&[0x9F, 0x01, 0xFF]).unwrap(),
            Value::Array(vec![int(1)])
        );
        assert_eq!(
            parse(&[0xBF, 0x61, b'a', 0x01, 0xFF]).unwrap(),
            map(&[("a", int(1))])
        );
        assert_eq!(
            parse(&[0x5F, 0x41, 1, 0x41, 2, 0xFF]).unwrap(),
            Value::Bytes(vec![1, 2])
        );
        assert_eq!(
            parse(&[0x7F, 0x61, b'a', 0x61, b'b', 0xFF]).unwrap(),
            text("ab")
        );
        for doc in [
            &[0x1C][..],                                                   // reserved
            &[0x5F, 0x61, b'a', 0xFF],                                     // text chunk in bytes
            &[0x5F, 0x5F, 0xFF, 0xFF], // nested indefinite chunk
            &[0x7F, 0x61, 0xFF, 0xFF], // chunk isn't UTF-8
            &[0xBF, 0x01, 0xFF],       // key with no value
            &[0x9F, 0x01],             // no break
            &[0xDB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01], // never-valid tag
        ] {
            assert_eq!(parse(doc), Err(StateError::InvalidCbor), "{doc:?}");
        }
    }

    #[test]
    fn nesting_is_limited_where_ciborium_limits_it() {
        for depth in 250..262 {
            let mut doc = vec![0x81; depth];
            doc.push(0x01);
            let theirs: Result<Value, _> = ciborium::de::from_reader(&doc[..]);
            assert_eq!(parse(&doc).is_ok(), theirs.is_ok(), "{depth}");
        }
    }

    /// `{"a": text, "b": 1}`, where the text makes it exactly `total_bytes`.
    fn merge_inputs(total_bytes: usize) -> (Vec<u8>, Value) {
        let text_bytes = total_bytes - (1 + 2 + 3 + 2 + 1);
        let current = cbor(&map(&[("a", Value::Text("x".repeat(text_bytes)))]));
        (current, map(&[("b", int(1))]))
    }

    #[test]
    fn the_64_kb_limit_is_exact_on_merge_and_incr_results() {
        let (current, patch) = merge_inputs(MAX_STATE_BYTES);
        let merged = apply_op(&current, StateOp::Merge, &cbor(&patch)).unwrap();
        assert_eq!(merged.len(), MAX_STATE_BYTES);
        let added = apply_op(&current, StateOp::Incr, &cbor(&incr_doc(&["b"], 1))).unwrap();
        assert_eq!(added.len(), MAX_STATE_BYTES);

        let (current, patch) = merge_inputs(MAX_STATE_BYTES + 1);
        assert_eq!(
            apply_op(&current, StateOp::Merge, &cbor(&patch)),
            Err(StateError::ResultTooLarge)
        );
        assert_eq!(
            apply_op(&current, StateOp::Incr, &cbor(&incr_doc(&["b"], 1))),
            Err(StateError::ResultTooLarge)
        );
    }

    /// A path within the key limit whose last key is itself deep: only parsing the result
    /// can tell that it went too far (D76).
    #[test]
    fn an_incr_with_a_deep_map_as_a_path_key_is_refused() {
        let mut deep_key = int(1);
        for _ in 0..250 {
            deep_key = Value::Map(vec![(int(1), deep_key)]);
        }
        let mut path: Vec<Value> = (0..100).map(|_| int(2)).collect();
        path.push(deep_key);
        let doc = cbor(&incr_doc_values(path, 1));
        check_doc(&doc).unwrap();
        assert_eq!(
            apply_op(&cbor(&map(&[])), StateOp::Incr, &doc),
            Err(StateError::ResultTooDeep)
        );
    }

    proptest! {
        /// What the encoder writes for anything the old parser understood is unchanged.
        #[test]
        fn parser_and_encoder_agree_with_ciborium(value in arb_value()) {
            let bytes = cbor(&value);
            prop_assert_eq!(parse(&bytes).unwrap(), value.clone());
            prop_assert_eq!(encode(&value).unwrap(), bytes);
        }

        #[test]
        fn hostile_bytes_are_an_error_or_round_trip(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            if let Ok(value) = parse(&bytes) {
                // By bytes, since a NaN doesn't equal itself.
                let encoded = encode(&value).unwrap();
                prop_assert_eq!(encode(&parse(&encoded).unwrap()).unwrap(), encoded);
            }
        }
    }

    #[test]
    fn replay_refuses_what_is_not_a_state_record() {
        let header = RecordHeader {
            id: 5,
            options: 1,
            author: 9,
            len: 8,
            count: 1,
            codec: 0,
        };
        let view = RecordView {
            header,
            data: &[0; 8],
        };
        let mut state = StreamState::new();
        assert_eq!(state.replay(&view), Err(StateError::NotStateRecord));
        let mut bytes = Vec::new();
        encode_system_record(&mut bytes, 5, 99, 1, &cbor(&int(1))).unwrap();
        let view = Records::new(&bytes).next().unwrap().unwrap();
        assert_eq!(state.replay(&view), Err(StateError::NotStateRecord));
        assert_eq!(state.ver(), 0);
    }
}
