//! The local metadata store.

use storage::MetadataStore;

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
