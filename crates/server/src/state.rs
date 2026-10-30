//! State (SPEC section 10): CBOR, plaintext to the server, at most 64 KB. `apply_op` is
//! the only place an operation changes a document, and live application and replay both
//! go through it, so they can't disagree.

use bytes::Bytes;
use ciborium::value::{Integer, Value};
use format::{ENTRY_STATE_INCR, ENTRY_STATE_MERGE, ENTRY_STATE_SET, RecordView, SystemRecord};

pub const MAX_STATE_BYTES: usize = 64 * 1024;
/// What `incr` will walk or build. The parser refuses to nest deeper than this, so a longer
/// path could never be read back, and building it would risk the core's stack (D76).
const MAX_INCR_PATH_KEYS: usize = 256;

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

fn parse(bytes: &[u8]) -> Result<Value, StateError> {
    let mut rest = bytes;
    let value: Value = ciborium::de::from_reader(&mut rest).map_err(|_| StateError::InvalidCbor)?;
    if !rest.is_empty() {
        return Err(StateError::InvalidCbor);
    }
    Ok(value)
}

fn encode(value: &Value) -> Result<Vec<u8>, StateError> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(value, &mut out).map_err(|_| StateError::InvalidCbor)?;
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
            (Some("n"), Value::Integer(int)) => n = Some(i128::from(int)),
            _ => return Err(StateError::BadIncrDoc),
        }
    }
    match (path, n) {
        (Some(path), Some(n)) => Ok((path, n)),
        _ => Err(StateError::BadIncrDoc),
    }
}

fn add(slot: &mut Value, n: i128) -> Result<(), StateError> {
    match slot {
        Value::Integer(int) => {
            let sum = i128::from(*int)
                .checked_add(n)
                .ok_or(StateError::IncrOverflow)?;
            *int = Integer::try_from(sum).map_err(|_| StateError::IncrOverflow)?;
            Ok(())
        }
        Value::Float(float) => {
            *float += n as f64;
            Ok(())
        }
        _ => Err(StateError::NotNumeric),
    }
}

fn int_value(n: i128) -> Result<Value, StateError> {
    Integer::try_from(n)
        .map(Value::Integer)
        .map_err(|_| StateError::IncrOverflow)
}

fn incr_at(slot: &mut Value, path: &[Value], n: i128) -> Result<(), StateError> {
    let Some((key, rest)) = path.split_first() else {
        return add(slot, n);
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
    Ok(())
}

fn incr(current: Option<Value>, path: &[Value], n: i128) -> Result<Value, StateError> {
    let mut root = match current {
        Some(value) => value,
        None if path.is_empty() => return int_value(n),
        None => Value::Map(Vec::new()),
    };
    incr_at(&mut root, path, n)?;
    Ok(root)
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
    let result = match op {
        StateOp::Merge => merge(current, patch),
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
    parse(&bytes).map_err(|_| StateError::ResultTooDeep)?;
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
