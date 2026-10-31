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
