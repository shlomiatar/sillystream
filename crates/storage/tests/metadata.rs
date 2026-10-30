//! The local metadata store.

use storage::{MAX_METADATA_KEY_BYTES, MetadataError, MetadataStore};

#[test]
fn values_survive_a_reopen_and_removal_sticks() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = MetadataStore::open(dir.path()).unwrap();
        assert_eq!(store.get(b"stream/a").unwrap(), None);
        store
            .apply([
                (&b"stream/a"[..], Some(&b"one"[..])),
                (&b"stream/b"[..], Some(&b"two"[..])),
                (&b"other/c"[..], Some(&b"three"[..])),
            ])
            .unwrap();
        store
            .apply([
                (&b"stream/a"[..], Some(&b"uno"[..])),
                (&b"stream/b"[..], None),
            ])
            .unwrap();
    }
    let store = MetadataStore::open(dir.path()).unwrap();
    assert_eq!(store.get(b"stream/a").unwrap().unwrap(), b"uno");
    assert_eq!(store.get(b"stream/b").unwrap(), None);
    let streams = store.scan_prefix(b"stream/").unwrap();
    assert_eq!(streams, [(b"stream/a".to_vec(), b"uno".to_vec())]);
}

#[test]
fn a_state_sized_value_round_trips() {
    // 64 KB of incompressible bytes, the largest state (section 10).
    let dir = tempfile::tempdir().unwrap();
    let mut value = vec![0u8; 64 * 1024];
    let mut state = 88172645463325252u64;
    for byte in &mut value {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
    {
        let store = MetadataStore::open(dir.path()).unwrap();
        store.apply([(&b"big"[..], Some(&value[..]))]).unwrap();
    }
    let store = MetadataStore::open(dir.path()).unwrap();
    assert_eq!(store.get(b"big").unwrap().unwrap(), value);
}

#[test]
fn many_small_entries_scan_in_key_order() {
    let dir = tempfile::tempdir().unwrap();
    let store = MetadataStore::open(dir.path()).unwrap();
    let keys: Vec<Vec<u8>> = (0..10_000u32)
        .map(|index| format!("s/{index:06}").into_bytes())
        .collect();
    store
        .apply(keys.iter().map(|key| (&key[..], Some(&b"v"[..]))))
        .unwrap();
    let scanned = store.scan_prefix(b"s/").unwrap();
    assert_eq!(scanned.len(), 10_000);
    assert!(scanned.windows(2).all(|pair| pair[0].0 < pair[1].0));
}

#[test]
fn a_key_the_store_cannot_hold_is_an_error_and_not_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    let store = MetadataStore::open(dir.path()).unwrap();
    for len_bytes in [0, MAX_METADATA_KEY_BYTES + 1, 70_000] {
        let key = vec![b'k'; len_bytes];
        let error = store.apply([(&key[..], Some(&b"v"[..]))]).unwrap_err();
        assert!(
            matches!(error, MetadataError::KeyLength { len_bytes: seen } if seen == len_bytes),
            "{error}"
        );
        assert!(store.apply([(&key[..], None)]).is_err());
        assert!(store.get(&key).is_err());
    }
    assert!(store.apply(Vec::<(&[u8], Option<&[u8]>)>::new()).is_ok());
}

#[test]
fn the_longest_key_works() {
    let dir = tempfile::tempdir().unwrap();
    let store = MetadataStore::open(dir.path()).unwrap();
    let key = vec![b'k'; MAX_METADATA_KEY_BYTES];
    store.apply([(&key[..], Some(&b"v"[..]))]).unwrap();
    assert_eq!(store.get(&key).unwrap().unwrap(), b"v");
    assert_eq!(store.scan_prefix(&key).unwrap().len(), 1);
    // A longer prefix can't match any key, which is an answer and not a failure.
    let longer = vec![b'k'; MAX_METADATA_KEY_BYTES + 1];
    assert!(store.scan_prefix(&longer).unwrap().is_empty());
    // The last possible key, whose prefix has no next value to be an upper bound.
    let top = vec![0xff; MAX_METADATA_KEY_BYTES];
    store.apply([(&top[..], Some(&b"t"[..]))]).unwrap();
    assert_eq!(store.scan_prefix(&top).unwrap().len(), 1);
}

#[test]
fn one_bad_key_applies_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let store = MetadataStore::open(dir.path()).unwrap();
    assert!(
        store
            .apply([(&b"good"[..], Some(&b"v"[..])), (&b""[..], Some(&b"v"[..]))])
            .is_err()
    );
    assert_eq!(store.get(b"good").unwrap(), None);
}

#[test]
fn a_value_the_store_cannot_hold_is_an_error_and_not_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    let store = MetadataStore::open(dir.path()).unwrap();
    // Zeroed pages are only mapped: refused before anything reads them.
    let value = vec![0u8; u32::MAX as usize + 1];
    let error = store.apply([(&b"k"[..], Some(&value[..]))]).unwrap_err();
    assert!(
        matches!(error, MetadataError::ValueLength { .. }),
        "{error}"
    );
}
