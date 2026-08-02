//! Shared helpers for `migraloop-app` integration tests.
//!
//! Each `tests/*.rs` binary that needs these does `mod common;`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static UNIQUE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique suffix for ephemeral Postgres / Mongo names under parallel `cargo test`.
///
/// Wall-clock nanos alone collide when multiple tests (or binaries) call
/// `SystemTime::now()` in the same tick — CI saw
/// `duplicate key value violates unique constraint "pg_database_datname_index"`.
/// Pid separates cargo test binaries; the atomic seq separates threads inside one binary.
pub fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let seq = UNIQUE_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{nanos}_{}_{seq}", std::process::id())
}
