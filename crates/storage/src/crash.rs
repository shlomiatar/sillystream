//! Named crash points (D32). With the `test-hooks` feature, `SILLYSTREAM_CRASH_AT=point` or
//! `point:nth` ends the process at that point on its nth pass, as abruptly as `kill -9`: no
//! destructors, no flushing. Without the feature the call is empty, so a release build holds
//! no such thing.

#[cfg(feature = "test-hooks")]
pub fn crash_point(name: &str) {
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TARGET: OnceLock<Option<(String, u64)>> = OnceLock::new();
    static PASSES: AtomicU64 = AtomicU64::new(0);
    let target = TARGET.get_or_init(|| {
        let text = std::env::var("SILLYSTREAM_CRASH_AT").ok()?;
        Some(match text.split_once(':') {
            Some((point, nth)) => (point.to_owned(), nth.parse().unwrap_or(1)),
            None => (text, 1),
        })
    });
    if let Some((point, nth)) = target
        && point == name
        && PASSES.fetch_add(1, Ordering::SeqCst) + 1 >= *nth
    {
        std::process::abort();
    }
}

#[cfg(not(feature = "test-hooks"))]
#[inline(always)]
pub fn crash_point(_name: &str) {}

/// While set, the WAL writer waits before each write, like a disk that has stalled. Tests in a
/// binary of their own use it, since it holds every node in the process.
#[cfg(feature = "test-hooks")]
static WAL_WRITES_HELD: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[cfg(feature = "test-hooks")]
pub fn hold_wal_writes(held: bool) {
    WAL_WRITES_HELD.store(held, std::sync::atomic::Ordering::SeqCst);
}

#[cfg(feature = "test-hooks")]
pub(crate) fn wal_write_gate() {
    while WAL_WRITES_HELD.load(std::sync::atomic::Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

#[cfg(not(feature = "test-hooks"))]
#[inline(always)]
pub(crate) fn wal_write_gate() {}
